import * as cheerio from "cheerio";
import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";

export class ArenaAdapter extends BaseSiteAdapter {
  readonly name = "Arenascans";
  readonly domain = "arenascans.com";
  private cdnDomain = "cdn.arenascan.com";

  constructor() {
    super();
    this.headers.Referer = "https://arenascans.com/";
  }

  getMangaSlug(url: string): string | null {
    const m = url.match(/\/manga\/([^/]+)/);
    return m ? m[1] : null;
  }

  getMangaUrl(slug: string): string {
    return `https://arenascans.com/manga/${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://arenascans.com/manga/${mangaSlug}/chapter-${chapterNumber}/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    const $ = cheerio.load(html);
    const urls: string[] = [];

    $(".reading-content img").each((_, el) => {
      let src = $(el).attr("data-src") || $(el).attr("src") || "";
      if (src && !src.startsWith("data:")) {
        src = normalizeUrl(src, "https://arenascans.com/");
        urls.push(src);
      }
    });

    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const $ = cheerio.load(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    $(".eplister ul li a, .listing-chapters_wrap li a").each((_, el) => {
      const href = $(el).attr("href") || "";
      const num = extractChapterNumber(href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({ number: num, url: href, title: $(el).text().trim() });
      }
    });

    return chapters;
  }
}
