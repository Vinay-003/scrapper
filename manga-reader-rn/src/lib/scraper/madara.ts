import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { selectInner, findImages, selectLinks, getAttr } from "../html";

export class MadaraAdapter extends BaseSiteAdapter {
  readonly name = "Madara";
  readonly domain = "";
  protected mangaPath = "/manga/";
  protected chapterPattern = "chapter-{num}";

  // Python: READ_CONTAINER_SELECTOR = ".read-container img"
  //         READING_CONTENT_SELECTOR = ".reading-content .page-break img"
  //         READER_AREA_SELECTOR = "#readerarea img"
  //         CHAPTER_LIST_SELECTORS = [".listing-chapters_wrap li.wp-manga-chapter a", ".eph-num a"]
  protected readSelectors = [
    ".read-container",
    ".reading-content .page-break",
    "#readerarea",
  ];
  protected chapterSelectors = [
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
    // Python: for selector in selectors: img_tags = soup.select(selector)
    for (const sel of this.readSelectors) {
      const container = selectInner(html, sel);
      if (container) {
        // Python: url = img.get('data-src') or img.get('src') — preferDataSrc=true
        const imgs = findImages(container, true);
        if (imgs.length > 0) {
          return imgs.map((u) => normalizeUrl(u, `https://${this.domain}/`));
        }
      }
    }
    return [];
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Python: for selector in selectors: chapter_links = soup.select(selector)
    for (const sel of this.chapterSelectors) {
      const links = selectLinks(html, sel);
      for (const link of links) {
        // Python: chapter_num = extract_chapter_number(href); if None: extract_chapter_number(text)
        const num = extractChapterNumber(link.href) || extractChapterNumber(link.text);
        if (num !== null && !seen.has(num)) {
          seen.add(num);
          chapters.push({
            number: num,
            url: normalizeUrl(link.href, `https://${this.domain}/`),
            title: link.text,
          });
        }
      }
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
    this.readSelectors = ["#readerarea"];
  }
}

export class ManhuaPlusTopAdapter extends MadaraAdapter {
  constructor() {
    super("manhuaplus.top", "ManhuaPlusTop");
  }
}
