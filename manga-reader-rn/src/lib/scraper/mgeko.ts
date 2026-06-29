import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { findById, findImages, selectInner, selectLinks, getAttr } from "../html";

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

    // If it's a chapter URL, fetch the page to get the correct manga slug
    if (url.includes("/reader/")) {
      try {
        const resp = await fetch(url, { headers: this.headers });
        const html = await resp.text();
        // Python: h1.find('a') where h1 = soup.find('h1')
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
    return `https://${this.domain}/manga/${slug}/`;
  }

  getChapterUrl(mangaSlug: string, chapterNumber: number): string {
    return `https://${this.domain}/reader/en/${mangaSlug}-chapter-${chapterNumber}-eng-li/`;
  }

  getImageUrlsFromPage(html: string): string[] {
    // Python: reader = soup.find('div', id='chapter-reader'); for img in reader.find_all('img')
    const reader = findById(html, "chapter-reader");
    if (!reader) return [];

    // Python: src = img.get('src', ''); only include 'imgsrv4.com' in src
    const imgs = findImages(reader, false);
    return imgs.filter((u) => u.includes("imgsrv4.com") && !this._isCreditsImage(u));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Python: chapter_list = soup.find('ul', class_='chapter-list')
    // for li in chapter_list.find_all('li'): a = li.find('a')
    const chapterListHtml = selectInner(html, "ul.chapter-list");
    if (chapterListHtml) {
      const links = selectLinks(chapterListHtml, "li a");
      for (const link of links) {
        // Python: match = re.search(r'-chapter-(\d+(?:\.\d+)?)', href)
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

    // Python: if not chapters: select = soup.find('select', id='cars')
    if (chapters.length === 0) {
      const selectHtml = selectInner(html, "select#cars");
      if (selectHtml) {
        const optionRegex = /<option[^>]*value="([^"]*)"[^>]*>([^<]*)<\/option>/gi;
        let m;
        while ((m = optionRegex.exec(selectHtml)) !== null) {
          const val = m[1];
          const optText = m[2];
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
