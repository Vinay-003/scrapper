import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { parseHtml, extractImages, extractLinks } from "../html";

export class ArenaAdapter extends BaseSiteAdapter {
  readonly name = "Arenascans";
  readonly domain = "arenascans.com";

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
    const root = parseHtml(html);
    const container = root.querySelector(".reading-content");
    if (!container) return [];
    const imgs = extractImages(container, true);
    return imgs.map((u) => normalizeUrl(u, "https://arenascans.com/"));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const root = parseHtml(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    let links = extractLinks(root, ".eplister ul li a");
    if (links.length === 0) {
      links = extractLinks(root, ".listing-chapters_wrap li a");
    }
    for (const link of links) {
      const num = extractChapterNumber(link.text) || extractChapterNumber(link.href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({
          number: num,
          url: normalizeUrl(link.href, "https://arenascans.com/"),
          title: link.text,
        });
      }
    }

    return chapters;
  }
}
