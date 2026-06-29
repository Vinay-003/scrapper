import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { selectInner, findImages, selectLinks } from "../html";

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
    // Python: reading_content = soup.select_one('.reading-content')
    const container = selectInner(html, ".reading-content");
    if (!container) return [];

    // Python: url = img.get('data-src') or img.get('src') — preferDataSrc=true
    const imgs = findImages(container, true);
    return imgs.map((u) => normalizeUrl(u, "https://arenascans.com/"));
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Python: chapter_links = soup.select('.eplister ul li a, .listing-chapters_wrap li a')
    // Try both selectors
    let links = selectLinks(html, ".eplister ul li a");
    if (links.length === 0) {
      links = selectLinks(html, ".listing-chapters_wrap li a");
    }
    for (const link of links) {
      // Python: chapter_num = extract_chapter_number(text); if None: extract_chapter_number(href)
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
