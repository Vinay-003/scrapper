import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { findImages, findLinks, selectInner } from "../html";

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
    const container = selectInner(html, ".reading-content");
    if (!container) return [];
    const imgs = findImages(container, true);
    return imgs.map((u) => normalizeUrl(u, "https://arenascans.com/"));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    const links = findLinks(html, (href) => href.includes("chapter"));
    for (const link of links) {
      const num = extractChapterNumber(link.href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({ number: num, url: link.href, title: link.text });
      }
    }

    return chapters;
  }
}
