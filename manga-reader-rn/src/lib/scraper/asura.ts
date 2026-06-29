import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { parseHtml, extractImages, extractLinks } from "../html";

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
    const root = parseHtml(html);
    const readerArea = root.querySelector("#readerarea");
    if (!readerArea) return [];
    const imgs = extractImages(readerArea, true);
    return imgs.map((u) => this._decodeAsuraUrl(u));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const root = parseHtml(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    const links = extractLinks(root, ".eplister ul li a");
    for (const link of links) {
      const num = extractChapterNumber(link.text) || extractChapterNumber(link.href);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({
          number: num,
          url: normalizeUrl(link.href, "https://asurascanz.com/"),
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
    try {
      const u = new URL(url);
      const parts = u.pathname.split("/");
      for (let i = 1; i < parts.length; i++) {
        try {
          const decoded = atob(parts[i]);
          if (decoded.includes("imagemanga.online")) {
            return decoded.startsWith("http") ? decoded : `https://${decoded}`;
          }
        } catch {}
      }
    } catch {}
    return url;
  }
}
