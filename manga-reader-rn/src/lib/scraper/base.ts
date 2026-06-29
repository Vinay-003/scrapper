export interface ChapterInfo {
  number: number;
  url: string;
  title?: string;
}

export interface ScrapeResult {
  success: boolean;
  images: string[];
  error?: string;
  chapterNumber?: number;
}

export abstract class BaseSiteAdapter {
  abstract readonly name: string;
  abstract readonly domain: string;
  requiresReferer = true;

  protected headers: Record<string, string>;

  constructor() {
    this.headers = {
      "User-Agent":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      Referer: "",
    };
  }

  abstract getMangaSlug(url: string): string | null;
  abstract getMangaUrl(slug: string): string;
  abstract getChapterUrl(mangaSlug: string, chapterNumber: number): string;
  abstract getImageUrlsFromPage(html: string): string[];
  abstract getAvailableChapters(html: string): ChapterInfo[];

  async resolveMangaSlug(url: string): Promise<string | null> {
    return this.getMangaSlug(url);
  }

  getAllChaptersUrl(slug: string): string | null {
    return null;
  }

  normalizeImageUrl(url: string): string {
    return url;
  }

  getSearchUrl(query: string): string {
    return "";
  }
}

/**
 * Extract chapter number from text/URL using common patterns.
 */
export function extractChapterNumber(text: string): number | null {
  const patterns = [
    /chapter[\s-]*(\d+(?:\.\d+)?)/i,
    /ch[\s.]*(\d+(?:\.\d+)?)/i,
    /(\d+(?:\.\d+)?)/,
  ];
  for (const pat of patterns) {
    const m = text.match(pat);
    if (m) return parseFloat(m[1]);
  }
  return null;
}

/**
 * Normalize a URL relative to a base URL.
 */
export function normalizeUrl(url: string, baseUrl: string): string {
  if (!url) return "";
  if (url.startsWith("http://") || url.startsWith("https://")) return url;
  if (url.startsWith("//")) {
    const proto = baseUrl.startsWith("https") ? "https:" : "http:";
    return proto + url;
  }
  if (url.startsWith("/")) {
    const u = new URL(baseUrl);
    return `${u.protocol}//${u.host}${url}`;
  }
  const base = baseUrl.endsWith("/") ? baseUrl : baseUrl.substring(0, baseUrl.lastIndexOf("/") + 1);
  return base + url;
}
