import * as cheerio from "cheerio";
import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";

export class MgekoAdapter extends BaseSiteAdapter {
  readonly name = "Mgeko";
  readonly domain = "mgeko.cc";
  requiresReferer = true;

  constructor() {
    super();
    this.headers.Referer = "https://mgeko.cc/";
  }

  getMangaSlug(url: string): string | null {
    const m1 = url.match(/\/manga\/([^/]+)/);
    if (m1) return m1[1];
    const m2 = url.match(/\/reader\/en\/([^/]+)-chapter-/);
    return m2 ? m2[1] : null;
  }

  async resolveMangaSlug(url: string): Promise<string | null> {
    const slug = this.getMangaSlug(url);
    if (!slug) return null;

    // If it's a chapter URL, fetch the page to get the correct manga slug
    if (url.includes("/reader/")) {
      try {
        const resp = await fetch(url, { headers: this.headers });
        const html = await resp.text();
        const $ = cheerio.load(html);
        const link = $('h1 a[href*="/manga/"]').attr("href") || "";
        const m = link.match(/\/manga\/([^/]+)/);
        if (m) return m[1];
      } catch {
        // Fall through to return the original slug
      }
    }

    return slug;
  }

  getMangaUrl(slug: string): string {
    return `https://mgeko.cc/manga/${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://mgeko.cc/reader/en/${mangaSlug}-chapter-${chapterNumber}-eng-li/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    const $ = cheerio.load(html);
    const urls: string[] = [];

    $("#chapter-reader img").each((_, el) => {
      const src = $(el).attr("src") || "";
      if (src && src.includes("imgsrv4.com") && !this._isCreditsImage(src)) {
        urls.push(src);
      }
    });

    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const $ = cheerio.load(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Try chapter list first
    $('ul.chapter-list li a, a[href*="chapter"]').each((_, el) => {
      const href = $(el).attr("href") || "";
      const num = extractChapterNumber(href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({ number: num, url: href, title: $(el).text().trim() });
      }
    });

    // Fallback to dropdown
    if (chapters.length === 0) {
      $('#cars option, select option').each((_, el) => {
        const val = $(el).attr("value") || "";
        const num = extractChapterNumber(val) || extractChapterNumber($(el).text());
        if (num !== null && !seen.has(num)) {
          seen.add(num);
          chapters.push({ number: num, url: val, title: $(el).text().trim() });
        }
      });
    }

    return chapters;
  }

  getAllChaptersUrl(slug: string): string {
    return `https://mgeko.cc/manga/${slug}/all-chapters/`;
  }

  getSearchUrl(query: string): string {
    return `https://mgeko.cc/search/?inputContent=${encodeURIComponent(query)}`;
  }

  private _isCreditsImage(url: string): boolean {
    const lower = url.toLowerCase();
    return lower.includes("credits") || lower.includes("watermark") || lower.includes("logo");
  }
}
