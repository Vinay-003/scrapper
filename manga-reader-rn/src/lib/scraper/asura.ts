import * as cheerio from "cheerio";
import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";

export class AsuraAdapter extends BaseSiteAdapter {
  readonly name = "AsuraScans";
  readonly domain = "asurascanz.com";
  private cdnDomain = "asurascans.imagemanga.online";

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
    const $ = cheerio.load(html);
    const urls: string[] = [];

    $("#readerarea img").each((_, el) => {
      let src = $(el).attr("data-src") || $(el).attr("src") || "";
      if (src && !src.startsWith("data:")) {
        src = this.normalizeImageUrl(normalizeUrl(src, "https://asurascanz.com/"));
        urls.push(src);
      }
    });

    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const $ = cheerio.load(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    $(".eplister ul li a").each((_, el) => {
      const href = $(el).attr("href") || "";
      const num = extractChapterNumber(href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({ number: num, url: href, title: $(el).text().trim() });
      }
    });

    return chapters;
  }

  normalizeImageUrl(url: string): string {
    return this._decodeAsuraUrl(url);
  }

  private _decodeAsuraUrl(url: string): string {
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
