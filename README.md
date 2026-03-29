# arenascan-dl

A fault-tolerant manga downloader for [arenascan.com](https://arenascan.com).

## Features

- Download entire manga or specific chapter ranges
- Concurrent downloads with configurable workers
- Automatic retry with exponential backoff
- IP ban detection with automatic pause/resume
- Creates ZIP archives of downloaded chapters

## Installation

```bash
pip install aiohttp beautifulsoup4
```

## Usage

```bash
# Download entire manga
python3 arenascans.py --url https://arenascan.com/manga/some-manga/

# Download specific chapter range
python3 arenascans.py --url https://arenascan.com/manga/some-manga/ --start 10 --end 20

# Custom concurrency settings
python3 arenascans.py --url https://arenascan.com/manga/some-manga/ --chapter-workers 10 --workers 6
```

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `--url` | Manga or chapter URL (required) | - |
| `--start` | First chapter to download | 1 |
| `--end` | Last chapter to download | ∞ |
| `--workers` | Parallel image downloads | 4 |
| `--chapter-workers` | Parallel chapter fetches | 2 |

## Rate Limiting

- Default workers are conservative (2 chapter, 4 image workers)
- Increase to 6-8 if you don't get 429 errors
- Decrease to 2 if you encounter rate limiting
- Ban detection will automatically pause if IP is blocked

## Output

Downloaded images are saved to `./{manga-slug}/chapters/{chapter_number}/`

Use Ctrl+C to gracefully stop downloading. Completed chapters are preserved.
