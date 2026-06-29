import {
  BaseSiteAdapter,
  ChapterInfo,
  extractChapterNumber,
  normalizeUrl,
} from "./base";
import { selectLinks } from "../html";

export class ThunderscansAdapter extends BaseSiteAdapter {
  readonly name = "Thunderscans";
  readonly domain = "en-thunderscans.com";

  constructor() {
    super();
    this.headers.Referer = "https://en-thunderscans.com/";
  }

  getMangaSlug(url: string): string | null {
    // Python: /comics/{slug}/ or /{slug}-chapter-{num}/
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
    // Python: match = re.search(r'ts_reader\.run\(({.*?})\);', html, re.DOTALL)
    const match = html.match(/ts_reader\.run\((\{[\s\S]*?\})\)/);
    if (!match) return [];

    try {
      // Python: data_str = re.sub(r',\s*}', '}', data_str); re.sub(r',\s*]', ']', data_str)
      let dataStr = match[1];
      dataStr = dataStr.replace(/,\s*}/g, "}").replace(/,\s*]/g, "]");
      const data = JSON.parse(dataStr);

      // Python: for source in data.get('sources', []): images.extend(source.get('images', []))
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
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Python: for selector in selectors: chapter_links = soup.select(selector)
    const selectors = [".chapter-list a[href*='chapter']", "a[href*='-chapter-']", ".version-chap a"];
    for (const sel of selectors) {
      const links = selectLinks(html, sel);
      for (const link of links) {
        // Python: chapter_num = extract_chapter_number(text); if None: extract_chapter_number(href)
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
    // Python: img_urls = re.findall(r'https://mangataro\.yachts/storage/chapters/[^\s"<>]+', html)
    const urls: string[] = [];
    const regex = /https?:\/\/mangataro\.yachts\/storage\/chapters\/[^\s"'<>]+/gi;
    let match;
    while ((match = regex.exec(html)) !== null) {
      // Python: url = url.rstrip('\'"')
      const url = match[0].replace(/['"]+$/, "");
      if (!urls.includes(url)) {
        urls.push(url);
      }
    }
    return urls;
  }

  getAvailableChapters(html: string): ChapterInfo[] {
    const chapters: ChapterInfo[] = [];
    const seen = new Set<number>();

    // Python: chapter_links = soup.select('a[href*="/read/"]')
    const links = selectLinks(html, 'a[href*="/read/"]');
    for (const link of links) {
      // Python: chapter_num = extract_chapter_number(text); if None: extract_chapter_number(href)
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
