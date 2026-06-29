import * as cheerio from "cheerio";
import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";

export class MadaraAdapter extends BaseSiteAdapter {
  readonly name = "Madara";
  readonly domain = "";
  protected mangaPath = "/manga/";
  protected chapterPattern = "chapter-{num}";

  readSelectors = [
    ".read-container img",
    ".reading-content .page-break img",
    "#readerarea img",
  ];
  chapterSelectors = [
    ".listing-chapters_wrap li.wp-manga-chapter a",
    ".eph-num a",
  ];

  constructor(domain: string, name?: string) {
    super();
    (this as any).domain = domain;
    if (name) (this as any).name = name;
    this.headers.Referer = `https://${domain}/`;
  }

  getMangaSlug(url: string): string | null {
    const m = url.match(new RegExp(`${this.mangaPath.replace(/\//g, "\\/")}([^/]+)`));
    return m ? m[1] : null;
  }

  getMangaUrl(slug: string): string {
    return `https://${this.domain}${this.mangaPath}${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    const ch = this.chapterPattern.replace("{num}", String(chapterNumber));
    return `https://${this.domain}${this.mangaPath}${mangaSlug}/${ch}/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    const $ = cheerio.load(html);
    const urls: string[] = [];

    for (const sel of this.readSelectors) {
      $(sel).each((_, el) => {
        let src = $(el).attr("data-src") || $(el).attr("src") || "";
        if (src && !src.startsWith("data:")) {
          src = normalizeUrl(src, `https://${this.domain}/`);
          urls.push(src);
        }
      });
      if (urls.length > 0) break;
    }

    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const $ = cheerio.load(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    for (const sel of this.chapterSelectors) {
      $(sel).each((_, el) => {
        const href = $(el).attr("href") || "";
        const num = extractChapterNumber(href) || extractChapterNumber($(el).text());
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
    return `https://${this.domain}/?s=${encodeURIComponent(query)}&post_type=wp-manga`;
  }
}

export class ManhuaPlusAdapter extends MadaraAdapter {
  constructor() {
    super("manhuaplus.com", "ManhuaPlus");
  }
}

export class ManhwaTopAdapter extends MadaraAdapter {
  constructor() {
    super("manhwatop.com", "ManhwaTop");
  }
}

export class ManhuascanAdapter extends MadaraAdapter {
  constructor() {
    super("manhuascan.us", "Manhuascan");
    this.readSelectors = ["#readerarea img"];
  }
}

export class ManhuaPlusTopAdapter extends MadaraAdapter {
  constructor() {
    super("manhuaplus.top", "ManhuaPlusTop");
  }
}
