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


def get_images_from_cbz(cbz_path):
    """Extract image filenames from a CBZ archive."""
    images = []
    try:
        with zipfile.ZipFile(cbz_path, "r") as zf:
            for name in sorted(zf.namelist()):
                lower = name.lower()
                if lower.endswith(IMAGE_EXTS) and not lower.startswith("__"):
                    images.append(name)
    except Exception as e:
        print(f"Error reading {cbz_path}: {e}")
    return images


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

    images = get_images_from_cbz(cbz_path)
    return {
        "chapter": chapter_num,
        "slug": slug,
        "images": images,
        "total": len(images),
    }


@app.get("/api/manga/{slug}/chapter/{chapter_num}/image/{image_name:path}")
async def get_image(slug: str, chapter_num: float, image_name: str, path: Optional[str] = None):
    base = Path(path) if path else BASE_DIR
    cbz_path = base / slug / f"chapter_{chapter_num:g}.cbz"

    if not cbz_path.is_file():
        raise HTTPException(404, "Chapter not found")

    data = read_image_from_cbz(cbz_path, image_name)
    if data is None:
        raise HTTPException(404, "Image not found")

    ext = image_name.lower().rsplit(".", 1)[-1] if "." in image_name else "jpeg"
    content_types = {
        "jpg": "image/jpeg", "jpeg": "image/jpeg",
        "png": "image/png", "webp": "image/webp", "gif": "image/gif",
    }
    ct = content_types.get(ext, "image/jpeg")
    return Response(content=data, media_type=ct)


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


# ──────────────────────────────────────────────
# SCRAPER JOBS
# ──────────────────────────────────────────────
scraper_jobs = {}  # job_id -> job info


@app.post("/api/scraper/start")
async def start_scraper_job(config: dict):
    url = config.get("url", "").strip()
    if not url:
        raise HTTPException(400, "URL is required")

    job_id = str(uuid.uuid4())[:8]
    job = {
        "id": job_id,
        "url": url,
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

    return {"job_id": job_id, "status": "starting"}


async def run_scraper_job(job_id: str, job: dict):
    try:
        from arenascans import (
            get_manga_url, get_manga_title, get_chapters, download_chapter,
            BanDetector, HEADERS, REQUEST_TIMEOUT, DEFAULT_WORKERS,
            extract_slug
        )
        import aiohttp

        job["status"] = "fetching"
        job["log"].append("Fetching manga info...")

        try:
            manga_url = get_manga_url(job["url"])
        except ValueError as e:
            job["status"] = "failed"
            job["error"] = str(e)
            job["log"].append(f"Error: {e}")
            return

        connector = aiohttp.TCPConnector(limit=job["workers"] * job["chapter_workers"] + 4)
        async with aiohttp.ClientSession(connector=connector) as session:
            manga_title = await get_manga_title(session, manga_url)
            job["log"].append(f"Manga: {manga_title}")

            chapters = await get_chapters(session, manga_url)
            if not chapters:
                job["status"] = "failed"
                job["error"] = "No chapters found"
                job["log"].append("No chapters found. Check the URL.")
                return

            nums = list(chapters.keys())
            lo, hi = nums[0], nums[-1]

            start = job["start"] if job["start"] is not None else lo
            end = job["end"] if job["end"] is not None else hi

            if start is None:
                start = lo
            if end is None:
                end = hi

            start = float(start)
            end = float(end)

            if start > end:
                start, end = end, start

            selected = [(ch, url) for ch, url in chapters.items() if start <= ch <= end]
            if not selected:
                job["status"] = "failed"
                job["error"] = f"No chapters in range {start}–{end}"
                job["log"].append(job["error"])
                return

            job["total_chapters"] = len(selected)
            job["status"] = "downloading"
            job["log"].append(f"Downloading {len(selected)} chapters...")

            img_semaphore = asyncio.Semaphore(job["workers"] * job["chapter_workers"])
            ch_semaphore = asyncio.Semaphore(job["chapter_workers"])

            ban_detector = BanDetector()

            for ch_num, ch_url in selected:
                job["current_chapter"] = ch_num
                job["log"].append(f"Chapter {ch_num}...")

                try:
                    ok = await download_chapter(
                        session, ch_num, ch_url, manga_title,
                        img_semaphore, ch_semaphore
                    )
                    if ok:
                        job["completed_chapters"] += 1
                        job["log"].append(f"Chapter {ch_num} done")
                    else:
                        job["failed_chapters"].append(ch_num)
                        job["log"].append(f"Chapter {ch_num} partial")
                except Exception as e:
                    job["failed_chapters"].append(ch_num)
                    job["log"].append(f"Chapter {ch_num} error: {e}")

                job["progress"] = round(
                    (job["completed_chapters"] + len(job["failed_chapters"])) / job["total_chapters"] * 100
                )

            job["status"] = "completed"
            job["progress"] = 100
            job["log"].append("Done!")

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
    uvicorn.run(app, host="0.0.0.0", port=8000)
