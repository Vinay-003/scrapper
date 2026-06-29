import { File, Directory, Paths } from "expo-file-system/next";
import { getImageListFromCbz, readImageFromCbz, ImageDimensions } from "./cbz";
import { loadJson, saveJson } from "./storage";

const TRACKING_FILE = "manga_tracking.json";
const COMMENTS_FILE = "manga_comments.json";

export interface ChapterInfo {
  number: number;
  file: string;
}

export interface MangaDetail {
  slug: string;
  title: string;
  chapters: ChapterInfo[];
  progress: Record<string, unknown>;
  comments: unknown[];
}

function getMangaDir(): Directory {
  return new Directory(Paths.document, "manga");
}

function slugToTitle(slug: string): string {
  return slug.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * List all manga in the local storage.
 */
export async function listManga(): Promise<
  { slug: string; title: string; chapters: number; last_chapter: number | null }[]
> {
  const baseDir = getMangaDir();
  if (!baseDir.exists) return [];

  const entries = baseDir.list();
  const tracking = (await loadJson(TRACKING_FILE, {})) as Record<string, Record<string, unknown>>;
  const result: {
    slug: string;
    title: string;
    chapters: number;
    last_chapter: number | null;
  }[] = [];

  for (const entry of entries) {
    if (!(entry instanceof File)) continue;
    const name = entry.name;
    if (!name.endsWith(".cbz")) continue;

    // Extract manga slug from filename (e.g., "sword-sheath-s-child_chapter_1.cbz")
    const match = name.match(/^(.+?)_chapter_\d+(?:\.\d+)?\.cbz$/);
    if (!match) continue;

    const slug = match[1];
    const existing = result.find((r) => r.slug === slug);
    if (existing) {
      existing.chapters++;
    } else {
      result.push({
        slug,
        title: slugToTitle(slug),
        chapters: 1,
        last_chapter: (tracking[slug]?.last_chapter as number) ?? null,
      });
    }
  }

  return result;
}

/**
 * Get detail for a single manga.
 */
export async function getMangaDetail(slug: string): Promise<MangaDetail | null> {
  const baseDir = getMangaDir();
  if (!baseDir.exists) return null;

  const entries = baseDir.list();
  const chapters: ChapterInfo[] = [];

  for (const entry of entries) {
    if (!(entry instanceof File)) continue;
    const name = entry.name;
    if (!name.startsWith(slug + "_chapter_") || !name.endsWith(".cbz")) continue;
    const numStr = name.replace(`${slug}_chapter_`, "").replace(".cbz", "");
    const num = parseFloat(numStr);
    if (!isNaN(num)) {
      chapters.push({ number: num, file: name });
    }
  }

  chapters.sort((a, b) => a.number - b.number);

  const tracking = (await loadJson(TRACKING_FILE, {})) as Record<string, Record<string, unknown>>;
  const allComments = (await loadJson(COMMENTS_FILE, {})) as Record<string, unknown[]>;

  return {
    slug,
    title: slugToTitle(slug),
    chapters,
    progress: tracking[slug] || {},
    comments: allComments[slug] || [],
  };
}

/**
 * Get image list for a chapter.
 */
export async function getChapterImages(
  slug: string,
  chapterNumber: number
): Promise<{ images: string[]; sizes: Record<string, ImageDimensions> } | null> {
  const cbzPath = getCbzPath(slug, chapterNumber);
  const file = new File(cbzPath);
  if (!file.exists) return null;

  return getImageListFromCbz(cbzPath);
}

/**
 * Get image as a file URI that React Native Image can display directly.
 * Extracts the image from CBZ to a temp file.
 */
export async function getImageFileUri(
  slug: string,
  chapterNumber: number,
  imageName: string
): Promise<string | null> {
  const cbzPath = getCbzPath(slug, chapterNumber);
  const file = new File(cbzPath);
  if (!file.exists) return null;

  const cleanName = imageName.replace(/:s\d+$/, "").replace(/[^a-zA-Z0-9._-]/g, "_");
  const cacheDir = new Directory(Paths.cache, "images");
  if (!cacheDir.exists) {
    cacheDir.create();
  }

  // Check cache
  const ext = ".jpg";
  const cachedFile = new File(cacheDir, `img_${slug}_${chapterNumber}_${cleanName}${ext}`);
  if (cachedFile.exists) return cachedFile.uri;

  try {
    const { base64, contentType } = await readImageFromCbz(cbzPath, imageName);
    const fileExt = contentType.includes("png") ? ".png" : contentType.includes("webp") ? ".webp" : ".jpg";
    const filePath = new File(cacheDir, `img_${slug}_${chapterNumber}_${cleanName}${fileExt}`);
    await filePath.write(base64);
    return filePath.uri;
  } catch {
    return null;
  }
}

/**
 * Delete an entire manga and its CBZ files.
 */
export async function deleteManga(slug: string): Promise<boolean> {
  const baseDir = getMangaDir();
  if (!baseDir.exists) return false;

  const entries = baseDir.list();
  for (const entry of entries) {
    if (entry instanceof File && entry.name.startsWith(slug)) {
      entry.delete();
    }
  }

  const tracking = (await loadJson(TRACKING_FILE, {})) as Record<string, Record<string, unknown>>;
  delete tracking[slug];
  await saveJson(TRACKING_FILE, tracking);
  return true;
}

/**
 * Delete a single chapter CBZ.
 */
export async function deleteChapter(slug: string, chapterNumber: number): Promise<boolean> {
  const cbzPath = getCbzPath(slug, chapterNumber);
  const file = new File(cbzPath);
  if (!file.exists) return false;
  file.delete();
  return true;
}

/**
 * Get reading progress for a manga.
 */
export async function getProgress(slug: string): Promise<Record<string, unknown>> {
  const tracking = (await loadJson(TRACKING_FILE, {})) as Record<string, Record<string, unknown>>;
  return tracking[slug] || {};
}

/**
 * Save reading progress for a manga.
 */
export async function saveProgress(
  slug: string,
  data: Record<string, unknown>
): Promise<void> {
  const tracking = (await loadJson(TRACKING_FILE, {})) as Record<string, Record<string, unknown>>;
  tracking[slug] = data;
  await saveJson(TRACKING_FILE, tracking);
}

/**
 * Get comments for a manga.
 */
export async function getComments(slug: string): Promise<unknown[]> {
  const allComments = (await loadJson(COMMENTS_FILE, {})) as Record<string, unknown[]>;
  return allComments[slug] || [];
}

/**
 * Add a comment to a manga.
 */
export async function addComment(slug: string, comment: unknown): Promise<void> {
  const allComments = (await loadJson(COMMENTS_FILE, {})) as Record<string, unknown[]>;
  if (!allComments[slug]) allComments[slug] = [];
  allComments[slug].push(comment);
  await saveJson(COMMENTS_FILE, allComments);
}

function getCbzPath(slug: string, chapterNumber: number): string {
  const mangaDir = getMangaDir();
  const file = new File(mangaDir, `${slug}_chapter_${chapterNumber}.cbz`);
  return file.uri;
}
