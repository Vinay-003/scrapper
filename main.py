from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pathlib import Path
import json
import zipfile
import re
import asyncio
import uuid
from typing import Optional
from datetime import datetime

# Import multi-site adapter system
from sites import registry

app = FastAPI(title="Manga Reader")

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

TRACKING_FILE = DATA_DIR / "tracking.json"
COMMENTS_FILE = DATA_DIR / "comments.json"


def load_json(path, default=None):
    try:
        if path.exists():
            with open(path) as f:
                return json.load(f)
    except Exception:
        pass
    return default if default is not None else {}


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


tracking = load_json(TRACKING_FILE, {})
comments = load_json(COMMENTS_FILE, {})

IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".gif")
MAX_IMAGE_HEIGHT = 2000


def find_cbz_files(manga_dir):
    """Find all chapter_N.cbz files in a manga directory."""
    chapters = []
    if not manga_dir.is_dir():
        return chapters
    for f in sorted(manga_dir.iterdir()):
        m = re.match(r"^chapter_(\d+(?:\.\d+)?)\.cbz$", f.name, re.IGNORECASE)
        if m:
            chapters.append({
                "number": float(m.group(1)),
                "file": f.name,
            })
    return chapters


def get_image_list_from_cbz(cbz_path):
    """Get image list with auto-segmentation for tall images.

    Returns (images, sizes) where images is a list of display entries.
    Tall images are split into segments like "0002.jpg:s0", "0002.jpg:s1".
    Sizes dict maps each display name to its display {w, h}.
    """
    images = []
    sizes = {}
    try:
        from PIL import Image
        import io
        with zipfile.ZipFile(cbz_path, "r") as zf:
            for name in sorted(zf.namelist()):
                lower = name.lower()
                if not (lower.endswith(IMAGE_EXTS) and not lower.startswith("__")):
                    continue
                try:
                    data = zf.read(name)
                    img = Image.open(io.BytesIO(data))
                    w, h = img.size
                except Exception:
                    images.append(name)
                    continue

                if h <= MAX_IMAGE_HEIGHT:
                    images.append(name)
                    sizes[name] = {"w": w, "h": h}
                else:
                    num_segments = (h + MAX_IMAGE_HEIGHT - 1) // MAX_IMAGE_HEIGHT
                    for seg in range(num_segments):
                        seg_name = f"{name}:s{seg}"
                        seg_top = seg * MAX_IMAGE_HEIGHT
                        seg_bottom = min((seg + 1) * MAX_IMAGE_HEIGHT, h)
                        seg_h = seg_bottom - seg_top
                        images.append(seg_name)
                        sizes[seg_name] = {"w": w, "h": seg_h}
    except ImportError:
        with zipfile.ZipFile(cbz_path, "r") as zf:
            for name in sorted(zf.namelist()):
                lower = name.lower()
                if lower.endswith(IMAGE_EXTS) and not lower.startswith("__"):
                    images.append(name)
    except Exception as e:
        print(f"Error reading {cbz_path}: {e}")
    return images, sizes


def read_image_segment_from_cbz(cbz_path, image_name):
    """Read an image or a segment of an image from a CBZ archive.

    image_name can be:
      - "0001.jpg"         -> full image
      - "0002.jpg:s0"      -> first 4000px of a tall image
      - "0002.jpg:s3"      -> fourth segment, etc.
    """
    from PIL import Image
    import io

    seg_index = None
    raw_name = image_name
    if ":s" in image_name:
        parts = image_name.rsplit(":s", 1)
        raw_name = parts[0]
        try:
            seg_index = int(parts[1])
        except ValueError:
            pass

    try:
        with zipfile.ZipFile(cbz_path, "r") as zf:
            data = zf.read(raw_name)
    except Exception:
        return None, "image/jpeg"

    if seg_index is None:
        ct = _detect_content_type(data)
        return data, ct

    try:
        img = Image.open(io.BytesIO(data))
        w, h = img.size
        top = seg_index * MAX_IMAGE_HEIGHT
        bottom = min((seg_index + 1) * MAX_IMAGE_HEIGHT, h)
        segment = img.crop((0, top, w, bottom))
        buf = io.BytesIO()
        segment.save(buf, format="PNG")
        return buf.getvalue(), "image/png"
    except Exception:
        return data, _detect_content_type(data)


def _detect_content_type(data):
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    elif data[:4] == b"GIF8":
        return "image/gif"
    elif data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    elif data[:2] == b"\xff\xd8":
        return "image/jpeg"
    return "image/jpeg"


def read_image_from_cbz(cbz_path, image_name):
    """Read image bytes from a CBZ archive."""
    try:
        with zipfile.ZipFile(cbz_path, "r") as zf:
            return zf.read(image_name)
    except Exception:
        return None


# Serve static files
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")


@app.get("/")
async def root():
    return FileResponse(str(BASE_DIR / "static" / "index.html"))


@app.get("/scraper.html")
async def scraper_page():
    return FileResponse(str(BASE_DIR / "static" / "scraper.html"))


@app.get("/api/manga")
async def list_manga(path: Optional[str] = None):
    base = Path(path) if path else BASE_DIR
    if not base.is_dir():
        return {"manga": []}

    manga_list = []
    for item in sorted(base.iterdir()):
        if not item.is_dir():
            continue
        cbz_files = find_cbz_files(item)
        if not cbz_files:
            continue
        slug = item.name
        manga_list.append({
            "slug": slug,
            "title": slug.replace("_", " ").replace("-", " ").title(),
            "chapters": len(cbz_files),
            "last_chapter": tracking.get(slug, {}).get("last_chapter", None),
        })
    return {"manga": manga_list}


@app.get("/api/manga/{slug}")
async def get_manga(slug: str, path: Optional[str] = None):
    base = Path(path) if path else BASE_DIR
    manga_dir = base / slug
    if not manga_dir.is_dir():
        raise HTTPException(404, "Manga not found")

    cbz_files = find_cbz_files(manga_dir)
    return {
        "slug": slug,
        "title": slug.replace("_", " ").replace("-", " ").title(),
        "chapters": cbz_files,
        "progress": tracking.get(slug, {}),
        "comments": comments.get(slug, []),
    }


@app.get("/api/manga/{slug}/chapter/{chapter_num}")
async def get_chapter(slug: str, chapter_num: float, path: Optional[str] = None):
    base = Path(path) if path else BASE_DIR
    manga_dir = base / slug
    cbz_name = f"chapter_{chapter_num:g}.cbz"
    cbz_path = manga_dir / cbz_name

    if not cbz_path.is_file():
        raise HTTPException(404, f"Chapter file not found: {cbz_name}")

    images, sizes = get_image_list_from_cbz(cbz_path)
    return {
        "chapter": chapter_num,
        "slug": slug,
        "images": images,
        "sizes": sizes,
        "total": len(images),
    }


@app.get("/api/manga/{slug}/chapter/{chapter_num}/image/{image_name:path}")
async def get_image(slug: str, chapter_num: float, image_name: str, path: Optional[str] = None):
    base = Path(path) if path else BASE_DIR
    cbz_path = base / slug / f"chapter_{chapter_num:g}.cbz"

    if not cbz_path.is_file():
        raise HTTPException(404, "Chapter not found")

    try:
        data, ct = read_image_segment_from_cbz(cbz_path, image_name)
    except Exception:
        raise HTTPException(404, "Image not found")

    if data is None:
        raise HTTPException(404, "Image not found")

    return Response(content=data, media_type=ct, headers={"Cache-Control": "no-store"})


@app.get("/api/manga/{slug}/progress")
async def get_progress(slug: str):
    return tracking.get(slug, {})


@app.post("/api/manga/{slug}/progress")
async def update_progress(slug: str, data: dict):
    tracking[slug] = data
    save_json(TRACKING_FILE, tracking)
    return {"ok": True}


@app.get("/api/manga/{slug}/comments")
async def get_comments(slug: str):
    return comments.get(slug, [])


@app.post("/api/manga/{slug}/comments")
async def add_comment(slug: str, data: dict):
    if slug not in comments:
        comments[slug] = []
    comments[slug].append(data)
    save_json(COMMENTS_FILE, comments)
    return {"ok": True}


@app.delete("/api/manga/{slug}/delete")
async def delete_manga(slug: str, path: Optional[str] = None):
    """Delete all chapters for a manga"""
    base = Path(path) if path else BASE_DIR
    manga_dir = base / slug
    if not manga_dir.is_dir():
        raise HTTPException(404, "Manga not found")
    
    import shutil
    shutil.rmtree(manga_dir)
    
    # Clean up tracking and comments
    if slug in tracking:
        del tracking[slug]
        save_json(TRACKING_FILE, tracking)
    if slug in comments:
        del comments[slug]
        save_json(COMMENTS_FILE, comments)
    
    return {"ok": True}


@app.delete("/api/manga/{slug}/chapter/{chapter_num}/delete")
async def delete_chapter(slug: str, chapter_num: float, path: Optional[str] = None):
    """Delete a single chapter"""
    base = Path(path) if path else BASE_DIR
    manga_dir = base / slug
    cbz_name = f"chapter_{chapter_num:g}.cbz"
    cbz_path = manga_dir / cbz_name
    
    if not cbz_path.is_file():
        raise HTTPException(404, f"Chapter not found: {cbz_name}")
    
    cbz_path.unlink()
    return {"ok": True}


# ──────────────────────────────────────────────
# MULTI-SITE SUPPORT
# ──────────────────────────────────────────────
@app.get("/api/sites")
async def list_supported_sites():
    """List all supported manga sites"""
    return {"sites": registry.get_all_sites()}


@app.post("/api/scraper/detect")
async def detect_site_from_url(data: dict):
    """Detect which site a URL is from"""
    url = data.get("url", "")
    domain = registry.detect_site(url)
    if domain:
        return {"detected": True, "domain": domain, "name": registry.SITE_NAMES.get(domain, domain)}
    return {"detected": False}


# ──────────────────────────────────────────────
# SCRAPER JOBS
# ──────────────────────────────────────────────
scraper_jobs = {}  # job_id -> job info


@app.post("/api/scraper/start")
async def start_scraper_job(config: dict):
    url = config.get("url", "").strip()
    if not url:
        raise HTTPException(400, "URL is required")

    # Detect site from URL
    site_domain = config.get("site") or registry.detect_site(url)
    if not site_domain:
        raise HTTPException(400, "Unsupported site. Use /api/sites to list supported sites.")

    job_id = str(uuid.uuid4())[:8]
    job = {
        "id": job_id,
        "url": url,
        "site": site_domain,
        "site_name": registry.SITE_NAMES.get(site_domain, site_domain),
        "start": config.get("start"),
        "end": config.get("end"),
        "workers": config.get("workers", 4),
        "chapter_workers": config.get("chapter_workers", 2),
        "status": "starting",
        "progress": 0,
        "current_chapter": None,
        "total_chapters": 0,
        "completed_chapters": 0,
        "failed_chapters": [],
        "log": [],
        "created_at": datetime.now().isoformat(),
        "error": None,
    }
    scraper_jobs[job_id] = job

    asyncio.create_task(run_scraper_job(job_id, job))

    return {"job_id": job_id, "status": "starting", "site": job["site_name"]}


async def run_scraper_job(job_id: str, job: dict):
    try:
        import aiohttp
        import asyncio
        import io
        import time
        from sites.base import ScrapeResult

        site_domain = job["site"]
        adapter = registry.get_adapter(site_domain)
        
        if not adapter:
            job["status"] = "failed"
            job["error"] = f"No adapter found for site: {site_domain}"
            job["log"].append(job["error"])
            return

        job["status"] = "fetching"
        job["log"].append(f"Using adapter: {adapter.name}")
        job["log"].append(f"Fetching manga info from {site_domain}...")

        connector = aiohttp.TCPConnector(limit=job["workers"] * job["chapter_workers"] + 4)
        async with aiohttp.ClientSession(connector=connector, headers=adapter.headers) as session:
            # Resolve correct manga slug (some sites have different slugs for chapter vs manga URLs)
            manga_slug = await adapter.resolve_manga_slug(session, job["url"])
            manga_url = adapter.get_manga_url(manga_slug or "")
            if not manga_url:
                # Try using the original URL directly
                manga_url = job["url"]
            
            try:
                async with session.get(manga_url) as resp:
                    if resp.status != 200:
                        job["status"] = "failed"
                        job["error"] = f"Failed to fetch manga page: HTTP {resp.status}"
                        job["log"].append(job["error"])
                        return
                    manga_html = await resp.text()
            except Exception as e:
                job["status"] = "failed"
                job["error"] = f"Failed to fetch manga page: {e}"
                job["log"].append(job["error"])
                return

            # Get available chapters
            chapters = adapter.get_available_chapters(manga_html)
            if not chapters:
                job["status"] = "failed"
                job["error"] = "No chapters found"
                job["log"].append("No chapters found. Check the URL.")
                return

            job["log"].append(f"Found {len(chapters)} chapters on main page")

            # Some sites have a separate all-chapters page (e.g., mgeko.cc)
            # If the user requested a range that might not be in the initial list, try the full list
            start = job["start"] if job["start"] is not None else None
            end = job["end"] if job["end"] is not None else None
            if start is not None and end is not None:
                min_ch = min(ch.number for ch in chapters)
                max_ch = max(ch.number for ch in chapters)
                if float(start) < min_ch or float(end) > max_ch:
                    all_chapters_url = adapter.get_all_chapters_url(manga_slug)
                    if all_chapters_url:
                        try:
                            async with session.get(all_chapters_url) as resp:
                                if resp.status == 200:
                                    all_html = await resp.text()
                                    all_chapters = adapter.get_available_chapters(all_html)
                                    if len(all_chapters) > len(chapters):
                                        chapters = all_chapters
                                        job["log"].append(f"Loaded all-chapters page: {len(chapters)} chapters")
                        except Exception:
                            pass

            # Filter by chapter range
            start = job["start"] if job["start"] is not None else chapters[0].number
            end = job["end"] if job["end"] is not None else chapters[-1].number

            if start is None:
                start = chapters[0].number
            if end is None:
                end = chapters[-1].number

            start = float(start)
            end = float(end)

            if start > end:
                start, end = end, start

            selected = [ch for ch in chapters if start <= ch.number <= end]
            if not selected:
                job["status"] = "failed"
                job["error"] = f"No chapters in range {start}–{end}"
                job["log"].append(job["error"])
                return

            job["total_chapters"] = len(selected)
            job["status"] = "downloading"
            job["log"].append(f"Downloading {len(selected)} chapters...")

            # Determine manga title from slug for directory name
            manga_slug = adapter.get_manga_slug(job["url"])
            if not manga_slug:
                # Try to extract from URL
                from urllib.parse import urlparse
                parsed = urlparse(job["url"])
                manga_slug = parsed.path.strip('/').split('/')[-1]
            
            manga_title = manga_slug.replace('-', ' ').replace('_', ' ').title() if manga_slug else "Unknown Manga"
            manga_dir = BASE_DIR / manga_title
            manga_dir.mkdir(exist_ok=True)

            async def download_image(session, url, idx, referer=None):
                """Download a single image, return (idx, bytes, ext) or (idx, None, None)"""
                try:
                    headers = {}
                    if referer:
                        headers["Referer"] = referer
                    async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status != 200:
                            return (idx, None, None)
                        data = await resp.read()
                        # Determine extension from URL or content type
                        ext = "jpg"
                        if '.png' in url.lower():
                            ext = "png"
                        elif '.webp' in url.lower():
                            ext = "webp"
                        elif '.gif' in url.lower():
                            ext = "gif"
                        return (idx, data, ext)
                except Exception:
                    return (idx, None, None)

            def create_cbz(images_data, output_path):
                """Create a CBZ file from a list of (bytes, ext) tuples"""
                with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_STORED) as zf:
                    for i, (data, ext) in enumerate(images_data):
                        if data is not None:
                            filename = f"{i+1:04d}.{ext}"
                            zf.writestr(filename, data)

            # Download each chapter
            for ch in selected:
                job["current_chapter"] = ch.number
                job["log"].append(f"Chapter {ch.number}...")

                try:
                    # Fetch chapter page
                    async with session.get(ch.url) as resp:
                        if resp.status != 200:
                            job["failed_chapters"].append(ch.number)
                            job["log"].append(f"Chapter {ch.number} failed: HTTP {resp.status}")
                            continue
                        ch_html = await resp.text()

                    # Get image URLs
                    image_urls = adapter.get_image_urls_from_page(ch_html)
                    if not image_urls:
                        job["failed_chapters"].append(ch.number)
                        job["log"].append(f"Chapter {ch.number}: No images found")
                        continue

                    job["log"].append(f"Chapter {ch.number}: Found {len(image_urls)} images, downloading...")

                    # Download images concurrently with rate limiting
                    sem = asyncio.Semaphore(job["workers"])
                    
                    async def limited_download(session, url, idx, referer):
                        async with sem:
                            # Small delay to avoid hammering the server
                            await asyncio.sleep(0.1)
                            return await download_image(session, url, idx, referer)

                    tasks = [
                        limited_download(session, url, i, f"https://{site_domain}/")
                        for i, url in enumerate(image_urls)
                    ]
                    results = await asyncio.gather(*tasks)
                    
                    # Sort by index and pair data with extensions
                    results.sort(key=lambda x: x[0])
                    images_data = [(data, ext) for _, data, ext in results]
                    
                    # Filter out failed downloads
                    successful = sum(1 for d, _ in images_data if d is not None)
                    if successful == 0:
                        job["failed_chapters"].append(ch.number)
                        job["log"].append(f"Chapter {ch.number}: All image downloads failed")
                        continue

                    # Create CBZ file
                    cbz_name = f"chapter_{ch.number:g}.cbz"
                    cbz_path = manga_dir / cbz_name
                    create_cbz(images_data, cbz_path)
                    
                    job["completed_chapters"] += 1
                    job["log"].append(f"Chapter {ch.number}: Saved {cbz_name} ({successful}/{len(image_urls)} images)")

                except Exception as e:
                    job["failed_chapters"].append(ch.number)
                    job["log"].append(f"Chapter {ch.number} error: {e}")

                job["progress"] = round(
                    (job["completed_chapters"] + len(job["failed_chapters"])) / job["total_chapters"] * 100
                )

            job["status"] = "completed"
            job["progress"] = 100
            job["log"].append(f"Done! Saved {job['completed_chapters']} chapters to {manga_dir}/")
            if job["failed_chapters"]:
                job["log"].append(f"Failed chapters: {job['failed_chapters']}")

    except Exception as e:
        job["status"] = "failed"
        job["error"] = str(e)
        job["log"].append(f"Fatal error: {e}")


@app.get("/api/scraper/jobs")
async def list_scraper_jobs():
    return {"jobs": list(scraper_jobs.values())}


@app.get("/api/scraper/jobs/{job_id}")
async def get_scraper_job(job_id: str):
    if job_id not in scraper_jobs:
        raise HTTPException(404, "Job not found")
    return scraper_jobs[job_id]


@app.delete("/api/scraper/jobs/{job_id}")
async def delete_scraper_job(job_id: str):
    if job_id not in scraper_jobs:
        raise HTTPException(404, "Job not found")
    del scraper_jobs[job_id]
    return {"ok": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
