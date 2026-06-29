import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { parseHtml, extractLinks } from "../html";

export class ThunderscansAdapter extends BaseSiteAdapter {
  readonly name = "Thunderscans";
  readonly domain = "en-thunderscans.com";

  constructor() {
    super();
    this.headers.Referer = "https://en-thunderscans.com/";
  }

  getMangaSlug(url: string): string | null {
    const m1 = url.match(/\/comics\/([^/]+)/);
    if (m1) return m1[1];
    const m2 = url.match(/\/([^/]+)-chapter-\d+/);
    return m2 ? m2[1] : null;
  }

  getMangaUrl(slug: string): string {
    return `https://en-thunderscans.com/comics/${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://en-thunderscans.com/${mangaSlug}-chapter-${chapterNumber}/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    // ts_reader.run() JavaScript call
    const match = html.match(/ts_reader\.run\((\{[\s\S]*?\})\)/);
    if (!match) return [];

    try {
      let dataStr = match[1];
      dataStr = dataStr.replace(/,\s*}/g, "}").replace(/,\s*]/g, "]");
      const data = JSON.parse(dataStr);

      const urls: string[] = [];
      for (const source of data.sources || []) {
        urls.push(...(source.images || []));
      }
      return urls;
    } catch {
      return [];
    }
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const root = parseHtml(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    const selectors = [".chapter-list a[href*='chapter']", "a[href*='-chapter-']", ".version-chap a"];
    for (const sel of selectors) {
      const links = extractLinks(root, sel);
      for (const link of links) {
        const num = extractChapterNumber(link.text) || extractChapterNumber(link.href);
        if (num !== null && !seen.has(num)) {
          seen.add(num);
          chapters.push({
            number: num,
            url: normalizeUrl(link.href, "https://en-thunderscans.com/"),
            title: link.text,
          });
        }
      }
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
    return this.getMangaUrl(_mangaSlug);
  }

  getImageUrlsFromPage(html: string): string[] {
    const urls: string[] = [];
    const regex = /https?:\/\/mangataro\.yachts\/storage\/chapters\/[^\s"'<>]+/gi;
    let match;
    while ((match = regex.exec(html)) !== null) {
      const url = match[0].replace(/['"]+$/, "");
      if (!urls.includes(url)) {
        urls.push(url);
      }
    }
    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const root = parseHtml(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    const links = extractLinks(root, 'a[href*="/read/"]');
    for (const link of links) {
      const num = extractChapterNumber(link.text) || extractChapterNumber(link.href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({
          number: num,
          url: normalizeUrl(link.href, "https://roliascan.com/"),
          title: link.text,
        });
      }
    }

    return chapters;
  }

  getSearchUrl(query: string): string {
    return `https://roliascan.com/?s=${encodeURIComponent(query)}`;
  }
}
