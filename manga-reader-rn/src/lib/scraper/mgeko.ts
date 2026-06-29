import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { parseHtml, findById, extractImages, extractLinks } from "../html";

export class MgekoAdapter extends BaseSiteAdapter {
  readonly name = "Mgeko";
  readonly domain = "www.mgeko.cc";
  requiresReferer = true;

  constructor() {
    super();
    this.headers.Referer = "https://www.mgeko.cc/";
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

    if (url.includes("/reader/")) {
      try {
        const resp = await fetch(url, { headers: this.headers });
        const html = await resp.text();
        const root = parseHtml(html);
        const h1 = root.querySelector("h1");
        if (h1) {
          const a = h1.querySelector("a");
          if (a) {
            const href = a.getAttribute("href") || "";
            if (href.includes("/manga/")) {
              return href.split("/manga/").pop()?.replace(/\/$/, "") || slug;
            }
          }
        }
      } catch {}
    }

    return slug;
  }

  getMangaUrl(slug: string): string {
    return `https://${this.domain}/manga/${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://${this.domain}/reader/en/${mangaSlug}-chapter-${chapterNumber}-eng-li/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    const root = parseHtml(html);
    const reader = findById(root, "chapter-reader");
    if (!reader) return [];

    const imgs = extractImages(reader, false);
    return imgs.filter((u) => u.includes("imgsrv4.com") && !this._isCreditsImage(u));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const root = parseHtml(html);
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Find <ul class="chapter-list"> then <li> > <a>
    const chapterList = root.querySelector("ul.chapter-list");
    if (chapterList) {
      const links = extractLinks(chapterList, "li a");
      for (const link of links) {
        const match = link.href.match(/-chapter-(\d+(?:\.\d+)?)/);
        if (match) {
          const num = parseFloat(match[1]);
          if (!seen.has(num)) {
            seen.add(num);
            chapters.push({
              number: num,
              url: normalizeUrl(link.href, `https://${this.domain}/`),
              title: `Chapter ${num}`,
            });
          }
        }
      }
    }

    // Fallback: <select id="cars">
    if (chapters.length === 0) {
      const select = root.querySelector("select#cars");
      if (select) {
        for (const option of select.querySelectorAll("option")) {
          const val = option.getAttribute("value") || "";
          const optText = option.textContent.trim();
          if (!val || !optText) continue;
          const numMatch = val.match(/-chapter-(\d+(?:\.\d+)?)/);
          if (numMatch) {
            const num = parseFloat(numMatch[1]);
            if (!seen.has(num)) {
              seen.add(num);
              chapters.push({
                number: num,
                url: normalizeUrl(val, `https://${this.domain}/`),
                title: optText,
              });
            }
          }
        }
      }
    }

    return chapters;
  }

  getAllChaptersUrl(slug: string): string {
    return `https://${this.domain}/manga/${slug}/all-chapters/`;
  }

  getSearchUrl(query: string): string {
    return `https://${this.domain}/search/?inputContent=${encodeURIComponent(query)}`;
  }

  private _isCreditsImage(url: string): boolean {
    const lower = url.toLowerCase();
    return lower.includes("credits") || lower.includes("watermark") || lower.includes("logo");
  }
}
