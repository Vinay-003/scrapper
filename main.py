from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response
from pathlib import Path
import json
import zipfile
import re
from typing import Optional

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
