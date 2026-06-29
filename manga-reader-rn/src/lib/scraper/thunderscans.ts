import * as cheerio from "cheerio";
import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";

export class ThunderscansAdapter extends BaseSiteAdapter {
  readonly name = "Thunderscans";
  readonly domain = "en-thunderscans.com";
  private mangaPath = "/comics/";

  constructor() {
    super();
    this.headers.Referer = "https://en-thunderscans.com/";
    this.headers.Accept =
      "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8";
    this.headers["Accept-Language"] = "en-US,en;q=0.5";
  }

  getMangaSlug(url: string): string | null {
    const m1 = url.match(/\/comics\/([^/]+)/);
    if (m1) return m1[1];
    const m2 = url.match(/\/([^/]+)-chapter-\d+/);
    return m2 ? m2[1] : null;
  }

  getMangaUrl(slug: string): string {
    return `https://en-thunderscans.com${this.mangaPath}${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://en-thunderscans.com/${mangaSlug}-chapter-${chapterNumber}/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    // Try to extract from ts_reader.run() JavaScript call
    const tsMatch = html.match(/ts_reader\.run\(({.*?})\)/s);
    if (tsMatch) {
      try {
        // Fix trailing commas for JSON parsing
        const fixed = tsMatch[1].replace(/,\s*}/g, "}").replace(/,\s*]/g, "]");
        const data = JSON.parse(fixed);
        if (data.sources) {
          const urls: string[] = [];
          for (const source of data.sources) {
            if (source.images) {
              urls.push(...source.images);
            }
          }
          if (urls.length > 0) return urls;
        }
      } catch {
        // Fall through to HTML parsing
      }
    }

    // Fallback: parse HTML
    const $ = cheerio.load(html);
    const urls: string[] = [];

    $(".reading-content img, #readerarea img").each((_, el) => {
      const src = $(el).attr("data-src") || $(el).attr("src") || "";
      if (src && !src.startsWith("data:")) {
        urls.push(normalizeUrl(src, "https://en-thunderscans.com/"));
      }
    });

    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const $ = cheerio.load(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    const selectors = [
      '.chapter-list a[href*="chapter"]',
      'a[href*="-chapter-"]',
      ".version-chap a",
    ];

    for (const sel of selectors) {
      $(sel).each((_, el) => {
        const href = $(el).attr("href") || "";
        const num = extractChapterNumber(href);
        if (num !== null && !seen.has(num)) {
          seen.add(num);
          chapters.push({ number: num, url: href, title: $(el).text().trim() });
        }
      });
      if (chapters.length > 0) break;
    }

    return chapters;
  }

  getSearchUrl(query: string): string {
    return `https://en-thunderscans.com/?s=${encodeURIComponent(query)}`;
  }
}

export class RoliascanAdapter extends BaseSiteAdapter {
  readonly name = "Roliascan";
  readonly domain = "roliascan.com";

  constructor() {
    super();
    this.headers.Referer = "https://roliascan.com/";
  }

  getMangaSlug(url: string): string | null {
    const m1 = url.match(/\/read\/([^/]+)/);
    if (m1) return m1[1];
    const m2 = url.match(/\/manga\/([^/]+)/);
    return m2 ? m2[1] : null;
  }

  getMangaUrl(slug: string): string {
    return `https://roliascan.com/manga/${slug}/`;
  }

  getChapterUrl(_mangaSlug: string, _chapterNumber: number): string {
    // Chapter URLs require an ID that can't be predicted.
    // Return manga page as fallback; actual chapter URLs come from getAvailableChapters.
    return this.getMangaUrl(_mangaSlug);
  }

  getImageUrlsFromPage(html: string): string[] {
    // Regex-based extraction for mangataro.yachts CDN URLs
    const urls: string[] = [];
    const regex = /https?:\/\/mangataro\.yachts\/storage\/chapters\/[^\s"'<>]+/gi;
    let match;
    while ((match = regex.exec(html)) !== null) {
      if (!urls.includes(match[0])) {
        urls.push(match[0]);
      }
    }
    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const $ = cheerio.load(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    $('a[href*="/read/"]').each((_, el) => {
      const href = $(el).attr("href") || "";
      const num = extractChapterNumber(href) || extractChapterNumber($(el).text());
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({ number: num, url: href, title: $(el).text().trim() });
      }
    });

    return chapters;
  }

  getSearchUrl(query: string): string {
    return `https://roliascan.com/?s=${encodeURIComponent(query)}`;
  }
}
