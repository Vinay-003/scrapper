# Manga Sites Research - URL Patterns, Slugs, CDN, and Structure

## Summary
- Many sites use **WordPress + Madara theme** - shared scraper pattern possible
- Some sites are custom (Asura Scans has unique CDN handling)
- Several sites are Cloudflare-protected and may need browser automation

---

## ✅ Working / Accessible Sites

### 1. Arenascans (arenascans.com) - ALREADY IMPLEMENTED
- **Status**: ✅ Working in `arenascans.py`
- **Type**: Custom
- **CDN**: `cdn.arenascan.com`
- **Manga URL**: `https://arenascans.com/manga/{slug}/`
- **Chapter URL**: `https://arenascans.com/manga/{slug}/{chapter-slug}/`
- **Image Container**: `<img>` in chapter content
- **Headers Required**: `Referer: https://arenascans.com/`

### 2. Asura Scans (asurascanz.com)
- **Status**: ✅ Accessible
- **Type**: Custom (NOT Madara)
- **CDN**: `asurascans.imagemanga.online` (base64 encoded paths)
- **Manga URL**: `https://asurascanz.com/manga/{slug}/`
- **Chapter URL**: `https://asurascanz.com/{manga-slug}-chapter-{num}/`
- **Image Container**: `#readerarea .reading-content img`
- **Image Attributes**: `src` or `data-src`, base64 encoded path
- **Headers Required**: `Referer: https://asurascanz.com/`
- **Notes**: Images require base64 decoding for full path

### 3. ManhuaPlus (manhuaplus.com)
- **Status**: ✅ Accessible
- **Type**: WordPress + Madara Theme
- **CDN**: Self-hosted (manhuaplus.com)
- **Manga URL**: `https://manhuaplus.com/manga/{slug}/`
- **Chapter URL**: `https://manhuaplus.com/manga/{slug}/chapter-{num}/`
- **Image Container**: `.read-container img` or `.reading-content .page-break img`
- **Image Attributes**: `data-src` (lazy load), fallback to `src`
- **Notes**: Standard Madara pattern

### 4. ManhwaTop (manhwatop.com)
- **Status**: ✅ Accessible
- **Type**: WordPress + Madara Theme
- **CDN**: Self-hosted (manhwatop.com)
- **Manga URL**: `https://manhwatop.com/manga/{slug}/`
- **Chapter URL**: `https://manhwatop.com/manga/{slug}/chapter-{num}/`
- **Image Container**: `.reading-content .page-break img` or `.read-container img`
- **Image Attributes**: `data-src` (lazy load), fallback to `src`
- **Notes**: Standard Madara pattern, uses same selectors as manhuaplus

### 5. Flamescans (flamescans.org)
- **Status**: ⚠️ Heavy Ads/Popups (may need browser)
- **Type**: WordPress + Madara (possibly)
- **CDN**: Self-hosted
- **Notes**: Has consent manager popup, may need special handling

---

## ❌ Blocked / Not Accessible

### 6. MangaKakalot (mangakakalot.com)
- **Status**: ❌ 522 Error
- **Type**: Custom
- **CDN**: Self-hosted
- **Known Structure**: `/{manga-slug}/chapter-{num}` pattern
- **Notes**: Often blocks scrapers, needs cloudscraper

### 7. Manganato (manganato.com)
- **Status**: ❌ 522 Error
- **Type**: Custom (sister site of MangaKakalot)
- **CDN**: Self-hosted
- **Known Structure**: Similar to MangaKakalot

### 8. ReaperScans (reaperscans.com)
- **Status**: ❌ 502 Error
- **Type**: Custom
- **CDN**: Unknown
- **Notes**: May be temporarily down

### 9. KunManga (kunmanga.com)
- **Status**: ❌ 403 Forbidden
- **Type**: Unknown
- **Notes**: Cloudflare protection

### 10. Harimanga (harimanga.com)
- **Status**: ❌ Transport Error
- **Type**: Unknown
- **Notes**: May be down

### 11. 1stKissManga (1stkissmanga.me)
- **Status**: ❌ Transport Error
- **Type**: Unknown
- **Notes**: May be down or blocked

### 12. MangaDex (mangadex.org)
- **Status**: ❌ 400 (requires API)
- **Type**: API-based
- **API**: `api.mangadex.org`
- **CDN**: `uploads.mangadex.org`
- **Notes**: Requires OAuth/API key for full access

---

## 🔍 Discovered Patterns

### WordPress + Madara Theme Sites (Shared Pattern)
These sites use identical selectors and can share a single scraper:

| Selector | Purpose |
|----------|---------|
| `.read-container img` | Main image container |
| `.reading-content .page-break img` | Alternative image container |
| `data-src` attribute | Lazy-loaded image URL |
| `src` attribute | Fallback image URL |
| `/manga/{slug}/` | Manga page URL pattern |
| `/chapter-{num}/` | Chapter URL pattern |

**Known Madara Sites**: manhuaplus.com, manhwatop.com, asurascanz.com (partially)

### Custom Sites
| Site | CDN Pattern | Notes |
|------|-------------|-------|
| Arenascans | `cdn.arenascan.com` | Direct image links |
| Asura Scans | `*.imagemanga.online` | Base64 encoded paths |

---

## 📋 Recommended Scraper Architecture

### 1. MadaraAdapter (shared for WP+Madara sites)
```python
# Works for: manhuaplus, manhwatop, and other Madara sites
class MadaraAdapter:
    domain = ""  # Set per site
    manga_pattern = "/manga/{slug}/"
    chapter_pattern = "/chapter-{num}/"
    image_selector = ".read-container img, .reading-content .page-break img"
    image_attr = "data-src"  # fallback to src
```

### 2. AsuraAdapter (asurascanz.com specific)
```python
class AsuraAdapter:
    domain = "asurascanz.com"
    cdn = "asurascans.imagemanga.online"
    manga_pattern = "/manga/{slug}/"
    chapter_pattern = "/{slug}-chapter-{num}/"
    image_selector = "#readerarea img"
    # Requires base64 path decoding
```

### 3. ArenaAdapter (already implemented)
```python
# Arenascans already has dedicated scraper
```

### 4. GenericAdapter (for other sites)
```python
# Custom patterns per site
```

---

## 🔧 Implementation Plan

1. Create `sites/` module with adapter pattern
2. Implement `MadaraAdapter` (covers 3+ sites)
3. Implement `AsuraAdapter` (unique CDN handling)
4. Keep `ArenaAdapter` (existing arenascans.py)
5. Add site selector to scraper UI
6. Add site-specific headers and CDN handling
7. Test with accessible sites first
