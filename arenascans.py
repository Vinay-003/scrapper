"""
arenascan_dl.py  —  A fault-tolerant manga downloader for arenascan.com

Usage:
  python arenascan_dl.py --url <manga-or-chapter-url> [--start N] [--end N] [--workers N]

Examples:
  python3 arenascans.py --url https://arenascan.com/manga/some-manga/ --chapter-workers 10 --worker 5
  python3 arenascans.py --url https://arenascan.com/manga/some-manga/ --start 10 --end 20
  python3 arenascans.py --url https://arenascan.com/manga/some-manga/ --workers 3
  python3 arenascans.py --url https://arenascan.com/reaper-of-the-drifting-moon/ --chapter-workers 10 --workers 6
"""

import asyncio
import aiohttp
import os
import re
import json
import zipfile
import argparse
import sys
import logging
from pathlib import Path
from urllib.parse import urlparse, urljoin

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("arenascan")

# ──────────────────────────────────────────────
# CONSTANTS  (tweak these for rate-limit tuning)
# ──────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://arenascan.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

BASE_URL        = "https://arenascan.com"
CDN_HOST        = "cdn.arenascan.com"
REQUEST_TIMEOUT   = aiohttp.ClientTimeout(total=30)  # seconds per request
RETRY_ATTEMPTS    = 4                                # total tries per image
RETRY_BACKOFF     = [1, 2, 4, 8]                     # seconds between retries
MIN_IMAGE_BYTES   = 2_048                            # below this = corrupt/error page
DEFAULT_WORKERS   = 4                                # concurrent image downloads
                                                     # raise to 6–8 if no bans,
                                                     # lower to 2 if you get 429s

# IP-ban detection: how many consecutive failures before we suspect a ban
BAN_CONSECUTIVE_THRESHOLD = 5
# How long to wait between connectivity probes while waiting for VPN
BAN_PROBE_INTERVAL        = 10   # seconds
# URL to hit when probing whether the site is reachable again
BAN_PROBE_URL             = "https://arenascan.com/"

# ──────────────────────────────────────────────
# IP-BAN DETECTION + VPN RECOVERY
# ──────────────────────────────────────────────

# Signals that carry their status codes as data, not exceptions
BAN_STATUS_CODES  = {403, 503}   # sites often return these on bans
RATE_STATUS_CODES = {429}        # classic rate-limit response

# Phrases found in HTML that indicate a block/ban page
BAN_HTML_MARKERS = [
    "access denied",
    "you have been blocked",
    "cloudflare",
    "403 forbidden",
    "your ip",
    "banned",
]


class BanDetector:
    """
    Tracks consecutive request failures across all workers.
    When the failure count exceeds BAN_CONSECUTIVE_THRESHOLD it assumes
    an IP ban, pauses ALL downloads, and waits until the site is reachable
    again (i.e. you've cycled your VPN).
    """

    def __init__(self) -> None:
        self._consecutive_failures = 0
        self._banned               = False
        self._ban_event            = asyncio.Event()
        self._ban_event.set()       # not banned = event is SET = workers proceed
        self._lock                 = asyncio.Lock()

    def _is_ban_response(self, status: int, body: bytes) -> bool:
        if status in BAN_STATUS_CODES:
            return True
        if len(body) < 50_000:   # only scan small responses (error pages)
            try:
                text = body.decode("utf-8", errors="ignore").lower()
                if any(m in text for m in BAN_HTML_MARKERS):
                    return True
            except Exception:
                pass
        return False

    async def record_failure(self, status: int, body: bytes, session: aiohttp.ClientSession) -> None:
        async with self._lock:
            self._consecutive_failures += 1
            is_ban = self._is_ban_response(status, body)

            if is_ban or self._consecutive_failures >= BAN_CONSECUTIVE_THRESHOLD:
                if not self._banned:
                    self._banned = True
                    self._ban_event.clear()   # PAUSE all workers
                    asyncio.ensure_future(self._recovery_loop(session))

    async def record_success(self) -> None:
        async with self._lock:
            self._consecutive_failures = 0

    async def wait_if_banned(self) -> None:
        """Call this before every request. Blocks until ban is lifted."""
        await self._ban_event.wait()

    async def _recovery_loop(self, session: aiohttp.ClientSession) -> None:
        """
        Polls the site until it responds with 200.
        Prints clear instructions for the user to cycle their VPN.
        """
        print("\n")
        print("╔══════════════════════════════════════════════════════╗")
        print("║           ⛔  IP BAN / BLOCK DETECTED  ⛔            ║")
        print("╠══════════════════════════════════════════════════════╣")
        print("║  All downloads are PAUSED.                           ║")
        print("║                                                      ║")
        print("║  What to do right now:                               ║")
        print("║    1. Disconnect your VPN                            ║")
        print("║    2. Reconnect your VPN (different server/country)  ║")
        print("║    3. Just wait — script will auto-resume            ║")
        print("║       when the site is reachable again.              ║")
        print("╚══════════════════════════════════════════════════════╝")

        attempt = 0
        while True:
            attempt += 1
            await asyncio.sleep(BAN_PROBE_INTERVAL)
            try:
                async with session.get(
                    BAN_PROBE_URL,
                    headers=HEADERS,
                    timeout=aiohttp.ClientTimeout(total=10),
                    allow_redirects=True,
                ) as res:
                    body = await res.read()
                    if res.status == 200 and not self._is_ban_response(res.status, body):
                        async with self._lock:
                            self._banned = False
                            self._consecutive_failures = 0
                        print(f"\n✅  Site reachable again (probe #{attempt}) — resuming downloads …\n")
                        self._ban_event.set()   # RESUME all workers
                        return
                    else:
                        print(f"  [probe #{attempt}] Still blocked (HTTP {res.status}) — "
                              f"retrying in {BAN_PROBE_INTERVAL}s …")
            except Exception as e:
                print(f"  [probe #{attempt}] No connection ({type(e).__name__}) — "
                      f"retrying in {BAN_PROBE_INTERVAL}s …")


# Module-level singleton — shared across all download coroutines
ban_detector = BanDetector()


# ──────────────────────────────────────────────
# URL HELPERS
# ──────────────────────────────────────────────

def normalise_url(url: str) -> str:
    """Ensure URL is absolute and has no trailing junk."""
    url = url.strip()
    if not url.startswith("http"):
        url = BASE_URL + "/" + url.lstrip("/")
    return url.split("?")[0].split("#")[0].rstrip("/")


def extract_slug(url: str) -> str | None:
    """
    Pull the manga slug out of any arenascan URL.
    Handles:
      /manga/{slug}/
      /{slug}-{chapter_number}/
      /{slug}-chapter-{chapter_number}/
    """
    url = normalise_url(url)
    path = urlparse(url).path.strip("/")

    # Direct manga page: /manga/{slug}
    m = re.match(r"^manga/([^/]+)$", path)
    if m:
        return m.group(1)

    # Direct manga page: /{slug}/ (no /manga/ prefix)
    m = re.match(r"^([^/]+)/?$", path)
    if m:
        return m.group(1)

    # Chapter URL: /{slug}-chapter-N  or  /{slug}-N
    m = re.match(r"^(.+?)(?:-chapter-\d+|-\d+(?:\.\d+)?)$", path)
    if m:
        return m.group(1)

    log.warning("Could not extract slug from: %s", url)
    return None


def get_manga_url(url: str) -> str:
    slug = extract_slug(url)
    if not slug:
        raise ValueError(f"Cannot determine manga slug from URL: {url}")
    return f"{BASE_URL}/manga/{slug}/"


def extract_chapter_number(href: str) -> float | None:
    """
    Returns a float so decimal chapters (e.g. 12.5) are preserved.
    Uses the LAST number group to avoid matching numbers inside the slug.
    """
    nums = re.findall(r"\d+(?:\.\d+)?", href)
    try:
        return float(nums[-1]) if nums else None
    except (ValueError, IndexError):
        return None


# ──────────────────────────────────────────────
# HTTP  (single fetch with timeout + error handling)
# ──────────────────────────────────────────────

async def fetch_html(session: aiohttp.ClientSession, url: str) -> str:
    """Fetch a page and return its HTML. Raises on HTTP errors."""
    async with session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT) as res:
        if res.status != 200:
            raise aiohttp.ClientResponseError(
                res.request_info, res.history, status=res.status,
                message=f"HTTP {res.status} for {url}"
            )
        return await res.text()


# ──────────────────────────────────────────────
# MANGA METADATA
# ──────────────────────────────────────────────

async def get_manga_title(session: aiohttp.ClientSession, manga_url: str) -> str:
    """Try to get a clean manga title from the page <title> tag."""
    try:
        from bs4 import BeautifulSoup
        html = await fetch_html(session, manga_url)
        soup = BeautifulSoup(html, "html.parser")
        title_tag = soup.find("title")
        if title_tag:
            # Typical format: "Manga Title – ArenaScan"
            raw = title_tag.get_text(strip=True)
            clean = re.split(r"\s*[–\-|]\s*", raw)[0].strip()
            if clean:
                # Make it safe for use as a folder name
                return re.sub(r'[<>:"/\\|?*]', "", clean).strip()
    except Exception as e:
        log.debug("Could not get manga title: %s", e)

    # Fallback to slug
    slug = extract_slug(manga_url)
    return slug or "unknown_manga"


# ──────────────────────────────────────────────
# CHAPTER MAP
# ──────────────────────────────────────────────

async def get_chapters(session: aiohttp.ClientSession, manga_url: str) -> dict[float, str]:
    """
    Returns {chapter_number: absolute_chapter_url} sorted ascending.
    Only collects links that look like chapter links from this manga.
    """
    from bs4 import BeautifulSoup

    html = await fetch_html(session, manga_url)
    soup = BeautifulSoup(html, "html.parser")
    slug = extract_slug(manga_url)
    chapter_map: dict[float, str] = {}

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full = urljoin(BASE_URL, href)          # handles both relative and absolute
        path = urlparse(full).path.strip("/")

        # Must look like {slug}-chapter-N or {slug}-N
        pattern = rf"^{re.escape(slug)}(?:-chapter-|-)\d+(?:\.\d+)?$"
        if not re.match(pattern, path):
            continue

        ch = extract_chapter_number(path)
        if ch is None:
            continue

        # Prefer the longer form (more canonical)
        if ch not in chapter_map or "-chapter-" in full:
            chapter_map[ch] = full

    if not chapter_map:
        log.warning("No chapters found at %s. The page structure may have changed.", manga_url)

    return dict(sorted(chapter_map.items()))


# ──────────────────────────────────────────────
# IMAGE EXTRACTION
# ──────────────────────────────────────────────

async def get_images(session: aiohttp.ClientSession, chapter_url: str) -> list[str]:
    """Return deduplicated list of CDN image URLs for a chapter."""
    from bs4 import BeautifulSoup

    html = await fetch_html(session, chapter_url)
    soup = BeautifulSoup(html, "html.parser")

    seen: set[str] = set()
    imgs: list[str] = []

    for img in soup.find_all("img"):
        src = img.get("data-src") or img.get("src") or ""
        src = src.strip()
        if CDN_HOST in src and src not in seen:
            seen.add(src)
            imgs.append(src)

    if not imgs:
        log.warning("No CDN images found on chapter page: %s", chapter_url)

    return imgs


# ──────────────────────────────────────────────
# RESUME / PROGRESS
# ──────────────────────────────────────────────

def load_progress(path: Path) -> dict:
    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except json.JSONDecodeError:
            log.warning("Corrupt progress file, starting fresh: %s", path)
    return {}


def save_progress(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


# ──────────────────────────────────────────────
# DOWNLOAD ONE IMAGE  (retry + content-type check)
# ──────────────────────────────────────────────

# Sentinel returned when an image is consistently too small across all retries.
# Treated as a skippable placeholder, not a real failure.
JUNK_IMAGE = "JUNK"

async def download_image(
    session: aiohttp.ClientSession,
    img_url: str,
    dest: Path,
    semaphore: asyncio.Semaphore,
) -> bool | str:
    """
    Download a single image with retries, ban detection, and integrity checks.
    Returns:
      True        — success
      JUNK_IMAGE  — image was consistently too small on every attempt (placeholder/spacer)
      False       — real failure (network error, HTTP error, etc.)
    """
    if dest.exists() and dest.stat().st_size >= MIN_IMAGE_BYTES:
        return True  # already downloaded and looks valid

    junk_strikes = 0  # how many attempts returned a tiny-but-200 response

    for attempt, wait in enumerate(RETRY_BACKOFF[:RETRY_ATTEMPTS], start=1):

        # Block here if a ban has been detected — resumes automatically
        await ban_detector.wait_if_banned()

        try:
            async with semaphore:
                async with session.get(img_url, headers=HEADERS, timeout=REQUEST_TIMEOUT) as res:
                    data = await res.read()

                    if res.status in RATE_STATUS_CODES:
                        retry_after = int(res.headers.get("Retry-After", wait * 2))
                        log.warning("Rate limited (429). Waiting %ds before retry %d/%d …",
                                    retry_after, attempt, RETRY_ATTEMPTS)
                        await ban_detector.record_failure(res.status, data, session)
                        await asyncio.sleep(retry_after)
                        continue

                    if res.status != 200:
                        log.warning("HTTP %d for %s (attempt %d/%d)",
                                    res.status, img_url, attempt, RETRY_ATTEMPTS)
                        await ban_detector.record_failure(res.status, data, session)
                        await asyncio.sleep(wait)
                        continue

                    # Content-type guard: reject HTML error pages served as images
                    ct = res.headers.get("Content-Type", "")
                    if "text/html" in ct:
                        log.warning("Server returned HTML instead of image: %s", img_url)
                        await ban_detector.record_failure(res.status, data, session)
                        await asyncio.sleep(wait)
                        continue

                    if len(data) < MIN_IMAGE_BYTES:
                        junk_strikes += 1
                        log.warning(
                            "Image too small (%d bytes), likely placeholder: %s "
                            "(strike %d/%d)",
                            len(data), img_url, junk_strikes, RETRY_ATTEMPTS,
                        )
                        # If it's tiny on EVERY attempt, it's not a transient error —
                        # it's a real placeholder. Stop retrying and mark as junk.
                        if junk_strikes >= RETRY_ATTEMPTS:
                            log.info("Skipping placeholder image: %s", img_url)
                            return JUNK_IMAGE
                        await asyncio.sleep(wait)
                        continue

                    dest.write_bytes(data)
                    await ban_detector.record_success()
                    return True

        except asyncio.TimeoutError:
            log.warning("Timeout on %s (attempt %d/%d)", img_url, attempt, RETRY_ATTEMPTS)
            await asyncio.sleep(wait)
        except aiohttp.ClientError as e:
            log.warning("Network error on %s: %s (attempt %d/%d)", img_url, e, attempt, RETRY_ATTEMPTS)
            await asyncio.sleep(wait)

    log.error("FAILED after %d attempts: %s", RETRY_ATTEMPTS, img_url)
    return False


# ──────────────────────────────────────────────
# CBZ CREATION
# ──────────────────────────────────────────────

def create_cbz(folder: Path, chapter_num: float, manga_title: str) -> Path | None:
    """
    Pack all .jpg files in `folder` into a CBZ archive.
    Returns the CBZ path, or None if no valid images were found.
    Skips progress.json and any zero-byte files.
    """
    # Sort numerically by the page number embedded in filename (page_1, page_10, etc.)
    def page_sort_key(p: Path) -> int:
        m = re.search(r"\d+", p.stem)
        return int(m.group()) if m else 0

    images = sorted(
        [f for f in folder.iterdir() if f.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp")
         and f.stat().st_size >= MIN_IMAGE_BYTES],
        key=page_sort_key,
    )

    if not images:
        log.error("No valid images in %s — skipping CBZ creation.", folder)
        return None

    cbz_path = folder.parent / f"chapter_{chapter_num:g}.cbz"

    with zipfile.ZipFile(cbz_path, "w", compression=zipfile.ZIP_STORED) as cbz:
        for img in images:
            cbz.write(img, img.name)   # flat archive, no subdirectory

        cbz.writestr(
            "ComicInfo.xml",
            f"""<?xml version="1.0"?>
<ComicInfo>
  <Series>{manga_title}</Series>
  <Number>{chapter_num:g}</Number>
  <PageCount>{len(images)}</PageCount>
</ComicInfo>""",
        )

    log.info("✓ Chapter %g → %s  (%d pages)", chapter_num, cbz_path.name, len(images))
    return cbz_path


# ──────────────────────────────────────────────
# DOWNLOAD ONE CHAPTER
# ──────────────────────────────────────────────

async def download_chapter(
    session: aiohttp.ClientSession,
    ch_num: float,
    ch_url: str,
    manga_name: str,
    img_semaphore: asyncio.Semaphore,
    ch_semaphore: asyncio.Semaphore,
) -> bool:
    """
    Download all images for a chapter and create its CBZ.
    - Runs under ch_semaphore  → limits how many chapters run at once.
    - Each image runs under img_semaphore → limits total concurrent HTTP requests.
    - If any pages fail permanently, saves failed.json and still builds a
      partial CBZ from whatever succeeded, so you don't lose the good pages.
    Returns True only when every page succeeded.
    """
    async with ch_semaphore:
        cbz_path = Path(manga_name) / f"chapter_{ch_num:g}.cbz"
        if cbz_path.exists():
            log.info("Skipping Chapter %g (CBZ already exists)", ch_num)
            return True

        folder = Path(manga_name) / f"chapter_{ch_num:g}"
        folder.mkdir(parents=True, exist_ok=True)
        progress_file = folder / "progress.json"
        failed_file   = folder.parent / f"chapter_{ch_num:g}_failed.json"
        progress      = load_progress(progress_file)

        log.info("→ Chapter %g  (%s)", ch_num, ch_url)

        try:
            images = await get_images(session, ch_url)
        except Exception as e:
            log.error("Could not fetch chapter page for %g: %s", ch_num, e)
            return False

        if not images:
            return False

        # Build only the tasks that aren't already done or confirmed junk
        pending: list[tuple[int, str]] = [
            (i, img_url)
            for i, img_url in enumerate(images)
            if not progress.get(f"page_{i+1:04d}")   # skips True and "junk"
        ]

        tasks = [
            download_image(
                session,
                img_url,
                folder / f"page_{i+1:04d}.jpg",
                img_semaphore,
            )
            for i, img_url in pending
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Reconcile results back against the pending list
        failed_pages: dict[str, str] = {}   # page_key → original URL (real failures only)
        junk_count = 0
        for (i, img_url), ok in zip(pending, results):
            key = f"page_{i+1:04d}"
            if ok is True:
                progress[key] = True
            elif ok is JUNK_IMAGE:
                # Placeholder/spacer — silently skip, don't count as failure
                junk_count += 1
                progress[key] = "junk"   # mark so resume doesn't retry it
            else:
                failed_pages[key] = img_url

        if junk_count:
            log.info(
                "Chapter %g: skipped %d placeholder image(s) (tiny/spacer, not real pages).",
                ch_num, junk_count,
            )

        save_progress(progress_file, progress)

        if failed_pages:
            # Save a dedicated failed.json so user can inspect / retry later
            with open(failed_file, "w") as f:
                json.dump({"chapter": ch_num, "url": ch_url, "pages": failed_pages}, f, indent=2)

            log.warning(
                "Chapter %g: %d/%d page(s) permanently failed. "
                "Details saved to: %s",
                ch_num, len(failed_pages), len(images), failed_file.name,
            )
            log.warning(
                "Failed pages for chapter %g: %s",
                ch_num,
                ", ".join(sorted(failed_pages.keys())),
            )

            # Build CBZ with whatever succeeded — partial is better than nothing
            log.info("Building partial CBZ for chapter %g (%d/%d pages) …",
                     ch_num, len(images) - len(failed_pages), len(images))
            cbz = create_cbz(folder, ch_num, manga_name)
            if cbz:
                # Clean up raw images; leave failed.json next to the cbz
                for f in folder.iterdir():
                    f.unlink()
                folder.rmdir()
            return False   # still report as failed so summary is accurate

        # All pages succeeded
        cbz = create_cbz(folder, ch_num, manga_name)
        if cbz:
            for f in folder.iterdir():
                f.unlink()
            folder.rmdir()
            # Remove stale failed.json if this is a retry that fully succeeded
            if failed_file.exists():
                failed_file.unlink()

        return cbz is not None


# ──────────────────────────────────────────────
# INTERACTIVE CHAPTER SELECTION
# ──────────────────────────────────────────────

def ask_chapter_range(chapters: dict) -> tuple[float, float]:
    nums = list(chapters.keys())
    lo, hi = nums[0], nums[-1]
    print(f"\nAvailable chapters: {lo:g} – {hi:g}  ({len(nums)} total)")

    def prompt(msg, default):
        try:
            raw = input(msg).strip()
            return float(raw) if raw else default
        except ValueError:
            print("  Invalid input, using default.")
            return default

    start = prompt(f"  Start chapter [{lo:g}]: ", lo)
    end   = prompt(f"  End chapter   [{hi:g}]: ", hi)

    if start > end:
        log.warning("Start (%g) > End (%g) — swapping.", start, end)
        start, end = end, start

    return start, end


# ──────────────────────────────────────────────
# RATE-LIMIT ADVISORY (printed once at startup)
# ──────────────────────────────────────────────

STARTUP_ADVICE = """
──────────────────────────────────────────────
  arenascan-dl  |  startup info
──────────────────────────────────────────────
  Chapter workers (parallel chapters):  {ch_workers}
  Image workers  (parallel images):     {img_workers}
  Total max concurrent requests:        {total}
  • Lower --workers / --chapter-workers if you see 429s.
  • Ban detection is ON — pauses automatically on IP block,
    resumes once you cycle your VPN.
──────────────────────────────────────────────
"""


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download manga chapters from arenascan.com as CBZ files."
    )
    parser.add_argument("--url",             required=True, help="Manga or chapter URL")
    parser.add_argument("--start",           type=float,    help="First chapter to download")
    parser.add_argument("--end",             type=float,    help="Last chapter to download")
    parser.add_argument("--workers",         type=int, default=DEFAULT_WORKERS,
                        help=f"Concurrent image downloads per chapter (default: {DEFAULT_WORKERS})")
    parser.add_argument("--chapter-workers", type=int, default=2,
                        help="How many chapters to download in parallel (default: 2)")
    args = parser.parse_args()

    if args.workers < 1 or args.workers > 20:
        parser.error("--workers must be between 1 and 20")
    if args.chapter_workers < 1 or args.chapter_workers > 10:
        parser.error("--chapter-workers must be between 1 and 10")

    print(STARTUP_ADVICE.format(
        ch_workers=args.chapter_workers,
        img_workers=args.workers,
        total=args.chapter_workers * args.workers,
    ))

    try:
        manga_url = get_manga_url(args.url)
    except ValueError as e:
        log.error(str(e))
        sys.exit(1)

    # Semaphore for total concurrent image requests (shared across all chapters)
    img_semaphore = asyncio.Semaphore(args.workers * args.chapter_workers)
    # Semaphore for how many chapters run at the same time
    ch_semaphore  = asyncio.Semaphore(args.chapter_workers)

    # Re-create ban_detector so its asyncio.Event is tied to the running loop
    global ban_detector
    ban_detector = BanDetector()

    connector = aiohttp.TCPConnector(limit=args.workers * args.chapter_workers + 4)
    async with aiohttp.ClientSession(connector=connector) as session:

        log.info("Fetching manga info from: %s", manga_url)
        manga_title = await get_manga_title(session, manga_url)
        log.info("Manga title: %s", manga_title)

        chapters = await get_chapters(session, manga_url)
        if not chapters:
            log.error("No chapters found. Check the URL or the site may have changed.")
            sys.exit(1)

        # Determine chapter range
        if args.start is not None and args.end is not None:
            start, end = args.start, args.end
            if start > end:
                log.warning("--start > --end, swapping.")
                start, end = end, start
        elif args.start is None and args.end is None:
            start, end = ask_chapter_range(chapters)
        else:
            parser.error("Provide both --start and --end, or neither.")
            return

        selected = [(ch, url) for ch, url in chapters.items() if start <= ch <= end]
        if not selected:
            log.error("No chapters in range %g–%g", start, end)
            sys.exit(1)

        log.info(
            "Downloading %d chapter(s) — %d chapter(s) at a time, %d image workers each …",
            len(selected), args.chapter_workers, args.workers,
        )

        # Launch all chapters concurrently — ch_semaphore throttles actual parallelism
        tasks = [
            download_chapter(session, ch_num, ch_url, manga_title, img_semaphore, ch_semaphore)
            for ch_num, ch_url in selected
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed: list[float] = []
        for (ch_num, _), ok in zip(selected, results):
            if ok is not True:
                failed.append(ch_num)

        # Summary
        print("\n" + "─" * 54)
        print(f"Done.  {len(selected) - len(failed)}/{len(selected)} chapters fully succeeded.")
        if failed:
            print(f"Incomplete chapters : {', '.join(f'{c:g}' for c in failed)}")
            print("These have a partial CBZ + a  chapter_N_failed.json")
            print("listing exactly which pages failed and their URLs.")
            print("Re-run the same command to retry — good pages are skipped.")
        print("─" * 54)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted. Partial progress has been saved — re-run to resume.")
        sys.exit(0)