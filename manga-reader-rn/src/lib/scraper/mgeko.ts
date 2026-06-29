import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
} from "./base";
import { findImages, findLinks, selectInner, attr, text } from "../html";

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
        // Find <h1><a href="/manga/SLUG/"> inside the page
        const h1Match = html.match(/<h1[^>]*>\s*<a[^>]*href="\/manga\/([^"\/]+)\/"[^>]*>/i);
        if (h1Match) return h1Match[1];
      } catch {
        // Fall through
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
    const container = selectInner(html, "#chapter-reader");
    if (!container) return [];
    const imgs = findImages(container, false);
    return imgs.filter((u) => u.includes("imgsrv4.com") && !this._isCreditsImage(u));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Try chapter list
    const links = findLinks(html, (href) => href.includes("chapter"));
    for (const link of links) {
      const num = extractChapterNumber(link.href) || extractChapterNumber(link.text);
      if (num !== null && !seen.has(num)) {
        seen.add(num);
        chapters.push({ number: num, url: link.href, title: link.text });
      }
    }

    // Fallback: find <select> dropdown with chapter options
    if (chapters.length === 0) {
      const selectMatch = html.match(/<select[^>]*id="cars"[^>]*>([\s\S]*?)<\/select>/i);
      if (selectMatch) {
        const optionRegex = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
        let m;
        while ((m = optionRegex.exec(selectMatch[1])) !== null) {
          const val = m[1];
          const optText = m[2];
          const num = extractChapterNumber(val) || extractChapterNumber(optText);
          if (num !== null && !seen.has(num)) {
            seen.add(num);
            chapters.push({ number: num, url: val, title: optText });
          }
        }
      }
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
