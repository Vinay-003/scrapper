# Manga Reader Project

Local manga reader app with FastAPI backend + React Native (Expo) mobile app. Reads `.cbz` chapter archives with vertical scroll reading, a multi-site scraper supporting 9 manga sites, progress tracking, and a dark editorial UI.

## Quick Start

**Backend (Python):**
```bash
cd ~/Projects/scrapper
source venv/bin/activate
python3 main.py
# Runs on http://localhost:8000
```

**Mobile App (Expo):**
```bash
cd ~/Projects/scrapper/manga-reader-rn
npx expo start --host lan
# Scan QR with Expo Go on Android
```

Phone and PC must be on the same WiFi network. The RN app connects to `http://10.47.169.128:8000` (PC's local IP).

## Architecture

```
scrapper/
├── main.py                  # FastAPI backend — manga serving, scraper API, CBZ creation
├── static/
│   ├── index.html           # Web reader SPA shell
│   ├── scraper.html         # Web scraper page
│   ├── css/style.css        # Web UI styles
│   └── js/app.js            # Web reader JS (routing, API, zoom, keyboard nav)
├── sites/                   # Multi-site scraper adapters
│   ├── base.py              # BaseSiteAdapter ABC + utility functions
│   ├── registry.py          # SITE_REGISTRY, detect_site(), get_adapter()
│   ├── madara.py            # MadaraAdapter + 4 subclasses (ManhuaPlus, ManhwaTop, etc.)
│   ├── asura.py             # AsuraAdapter for asurascanz.com
│   ├── arena.py             # ArenaAdapter for arenascans.com
│   ├── mgeko.py             # MgekoAdapter — slug resolution, all-chapters page
│   └── thunderscans.py      # ThunderscansAdapter + RoliascanAdapter
├── manga-reader-rn/         # React Native Expo app (branch: react-native)
│   ├── app/
│   │   ├── _layout.tsx      # Root layout with theme loading
│   │   ├── index.tsx        # Home screen — library, recently read, quick actions
│   │   ├── manga.tsx        # Manga detail — chapter list, delete
│   │   ├── reader.tsx       # Vertical scroll reader — pinch zoom, lazy load
│   │   └── scraper.tsx      # Scraper — URL detect, workers, job monitoring, wishlist
│   ├── src/lib/
│   │   ├── api.ts           # API service, encodeSlug(), base URL config
│   │   ├── storage.ts       # AsyncStorage — recently read, wishlist
│   │   └── theme.ts         # Dark/light theme — teal accent, deep blacks
│   ├── app.json             # Expo config, plugins
│   ├── eas.json             # EAS build config (preview = APK)
│   └── .npmrc               # legacy-peer-deps=true (needed for EAS build)
├── data/                    # tracking.json, comments.json (gitignored)
└── venv/                    # Python venv (gitignored)
```

## Git Branches

- `main` — Web reader + backend only
- `react-native` — Full project with RN mobile app (active development)

## How It Works

### Backend (main.py)
- `python3 main.py` starts uvicorn on port 8000
- Manga stored as directories in project root: `{Manga Name}/chapter_{num}.cbz`
- CBZ files are standard ZIP archives containing images (named `0001.jpg`, `0002.jpg`, etc.)
- Images served on-the-fly from CBZ via `zipfile` — no extraction to disk
- Tracking in `data/tracking.json`, comments in `data/comments.json`

### Key API Endpoints
| Endpoint | Method | Description |
|---|---|---|
| `/api/manga` | GET | List all manga with chapter counts |
| `/api/manga/{slug}` | GET | Get manga detail + chapters |
| `/api/manga/{slug}/chapter/{num}` | GET | Get chapter image list |
| `/api/manga/{slug}/chapter/{num}/image/{name}` | GET | Serve image from CBZ |
| `/api/manga/{slug}/delete` | DELETE | Delete manga directory |
| `/api/manga/{slug}/chapter/{num}/delete` | DELETE | Delete single chapter |
| `/api/manga/{slug}/progress` | GET/POST | Read/write last-read progress |
| `/api/manga/{slug}/comments` | GET/POST | Read/write comments |
| `/api/scraper/detect` | POST | Detect site from URL |
| `/api/scraper/start` | POST | Start scraper job |
| `/api/scraper/jobs` | GET | List all jobs |
| `/api/scraper/jobs/{id}` | GET | Get job status + logs |
| `/api/scraper/jobs/{id}` | DELETE | Delete job |

### Scraper Jobs
Request body for `/api/scraper/start`:
```json
{
  "url": "https://mgeko.cc/manga/sword-sheath-s-child-mg1/",
  "start_chapter": 1,
  "end_chapter": 10,
  "site": "mgeko.cc",
  "workers": 4,
  "chapter_workers": 2
}
```

### Site Adapter Pattern
Each site has a class inheriting `BaseSiteAdapter` in `sites/`:
- `get_manga_title(url)` — Extract title from page
- `get_chapters(url)` — Return `[{number, url}]` list
- `get_image_urls(chapter_url)` — Return list of image URLs for a chapter
- `get_manga_url(chapter_url)` — Resolve chapter URL back to manga page
- `resolve_manga_slug(url)` — Get correct slug for directory naming
- `get_all_chapters_url(manga_url)` — URL for full chapter list (some sites paginate)

## Registered Sites (9 total)

| Domain | Adapter | Status |
|---|---|---|
| arenascans.com | ArenaAdapter | Works |
| asurascanz.com | AsuraAdapter | Works |
| manhuaplus.com | ManhuaPlusAdapter | Works |
| manhuascan.us | ManhuascanAdapter | Works |
| mgeko.cc | MgekoAdapter | Works |
| en-thunderscans.com | ThunderscansAdapter | Works |
| roliascan.com | RoliascanAdapter | Works |
| manhwatop.com | ManhwaTopAdapter | Cloudflare blocked |
| manhuaplus.top | ManhuaPlusTopAdapter | Cloudflare blocked |

**Known blocked sites:** manhwatop.com, manhuaus.com, utoon.net (Cloudflare), vortexscans.org (404), mangakakalot.com (522), manganato.com (parked), reaperscans.com (502), mangadex.org (requires API)

## React Native App

### Theme (src/lib/theme.ts)
Dark editorial manga aesthetic:
- Background: `#0a0a0c` (deep black)
- Accent: `#00e5c3` (electric teal)
- Cards: `#121216`
- Borders: `#1e1e24`
- Text: `#e8e8ec` (fg), `#9898a0` (fg2), `#5a5a66` (fg3)

### Reader (app/reader.tsx)
- Vertical scroll (webtoon style)
- Images loaded from CBZ via backend API
- Pinch-to-zoom via raw touch events (`onTouchStart/Move/End`)
- +/- buttons for button-based zoom
- Tap to show/hide UI bars
- `Image.getSize()` intentionally NOT used — it causes low-res cached images
- Uses fixed 2:3 aspect ratio for image containers
- Recently read saved to AsyncStorage

### Slug Encoding (src/lib/api.ts)
- `encodeSlug()` normalizes slugs before API calls
- Prevents double-encoding: `Sword Sheath S Child` → `Sword%20Sheath%20S%20Child` (not `%2520`)
- expo-router may already encode params, so `encodeSlug` strips `%20` back to spaces first

### API Base URL
Default: `http://10.47.169.128:8000` (PC's local IP on user's network)
Stored in AsyncStorage, configurable from Settings panel in app.

### EAS Build
```bash
cd manga-reader-rn
npx eas build --profile preview --platform android
```
Builds APK via EAS cloud. `.npmrc` has `legacy-peer-deps=true` for peer dep conflicts.

## Conventions

### Python
- FastAPI, uvicorn, aiohttp for scraping
- CBZ uses `ZIP_STORED` (no compression)
- Images named `0001.jpg`, `0002.jpg`, etc. in CBZ
- Async scraping with `asyncio.Semaphore(workers)` + 100ms rate limiting
- `if __name__ == "__main__": uvicorn.run(...)` at bottom of main.py

### TypeScript/React Native
- Expo SDK 54, expo-router file-based routing
- No external gesture libraries (reanimated/worklets cause babel crashes)
- Raw React Native APIs for gestures
- AsyncStorage for local persistence (recently read, wishlist, API base)
- `api()` function handles all backend calls with error logging
- Theme via `t()` hook returning color object, `isDark()` for dark mode check

### Adding a New Site
1. Create `sites/newsite.py` with class inheriting `BaseSiteAdapter`
2. Implement required methods: `get_manga_title`, `get_chapters`, `get_image_urls`
3. Register in `sites/registry.py` — add to `SITE_REGISTRY` and `SITE_NAMES`
4. Add display name in RN scraper screen's `SUPPORTED_SITES` array (`app/scraper.tsx`)

## Known Issues / TODO

- [ ] Some sites blocked by Cloudflare (manhwatop, manhuaus, utoon)
- [ ] Reader pinch-to-zoom conflicts with ScrollView scroll on some devices
- [ ] No offline/caching for images
- [ ] No search functionality in library
- [ ] Chapter ordering could be smarter (numeric vs string sort)
- [ ] Need to run entirely on Android (currently PC runs backend, phone runs app)
- [ ] Web reader zoom uses CSS width change (different from RN approach)

## PC Local IP

Check with: `hostname -I`
Current: `10.47.169.128`
Update in `manga-reader-rn/src/lib/api.ts` if network changes.

## Expo Account

- Email: sinister.03.07@gmail.com
- Username: vinaysaini003
- EAS Project: `@vinaysaini003/manga-reader`
- Build logs: https://expo.dev/accounts/vinaysaini003/projects/manga-reader/builds
