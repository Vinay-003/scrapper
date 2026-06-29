import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { selectInner, findImages, selectLinks } from "../html";

export class AsuraAdapter extends BaseSiteAdapter {
  readonly name = "AsuraScans";
  readonly domain = "asurascanz.com";

  constructor() {
    super();
    this.headers.Referer = "https://asurascanz.com/";
  }

  getMangaSlug(url: string): string | null {
    const m = url.match(/\/manga\/([^/]+)/);
    if (m) return m[1];
    const m2 = url.match(/\/([^/]+)-chapter-\d+/);
    return m2 ? m2[1] : null;
  }

  getMangaUrl(slug: string): string {
    return `https://asurascanz.com/manga/${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://asurascanz.com/${mangaSlug}-chapter-${chapterNumber}/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    // Python: reader_area = soup.select_one('#readerarea'); for img in reader_area.find_all('img')
    const readerArea = selectInner(html, "#readerarea");
    if (!readerArea) return [];

    // Python: url = img.get('data-src') or img.get('src') — preferDataSrc=true
    const imgs = findImages(readerArea, true);
    return imgs.map((u) => this._decodeAsuraUrl(u));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Python: chapter_list = soup.select('.eplister ul li a')
    const links = selectLinks(html, ".eplister ul li a");
    for (const link of links) {
      // Python: chapter_num = extract_chapter_number(text); if None: extract_chapter_number(href)
      const num = extractChapterNumber(link.text) || extractChapterNumber(link.href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({
          number: num,
          url: normalizeUrl(link.href, `https://asurascanz.com/`),
          title: link.text,
        });
      }
    }

    return chapters;
  }

  normalizeImageUrl(url: string): string {
    return this._decodeAsuraUrl(url);
  }

  private _decodeAsuraUrl(url: string): string {
    // Python: decode asura's base64 encoded image URLs
    try {
      const u = new URL(url);
      const parts = u.pathname.split("/");
      for (let i = 1; i < parts.length; i++) {
        try {
          const decoded = atob(parts[i]);
          if (decoded.includes("imagemanga.online")) {
            const newUrl = decoded.startsWith("http") ? decoded : `https://${decoded}`;
            return newUrl;
          }
        } catch {
          // Not base64, continue
        }
      }
    } catch {
      // Invalid URL
    }
    return url;
  }
}
