import { File, Directory, Paths } from "expo-file-system/next";
import {
  getImageListFromChapter,
  getImageUri,
  getChapterDir,
  saveChapterImages,
  chapterExists,
  deleteChapterDir,
  deleteMangaDir,
} from "./cbz";
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
 * Manga are stored as: manga/{slug}/ch{num}/{images}
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
    if (!(entry instanceof Directory)) continue;
    const slug = entry.name;

    // Count chapter directories (ch1, ch2, etc.)
    const slugDir = new Directory(baseDir, slug);
    const slugEntries = slugDir.list();
    let chapterCount = 0;

    for (const se of slugEntries) {
      if (se instanceof Directory && /^ch\d+$/.test(se.name)) {
        // Verify it has images
        const chDir = new Directory(slugDir, se.name);
        const chEntries = chDir.list();
        if (chEntries.some((e) => e instanceof File && /\.(jpe?g|png|webp|gif)$/i.test(e.name))) {
          chapterCount++;
        }
      }
    }

    if (chapterCount > 0) {
      result.push({
        slug,
        title: slugToTitle(slug),
        chapters: chapterCount,
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
  const slugDir = new Directory(baseDir, slug);
  if (!slugDir.exists) return null;

  const slugEntries = slugDir.list();
  const chapters: ChapterInfo[] = [];

  for (const entry of slugEntries) {
    if (!(entry instanceof Directory)) continue;
    const chMatch = entry.name.match(/^ch(\d+(?:\.\d+)?)$/);
    if (!chMatch) continue;

    const num = parseFloat(chMatch[1]);
    // Verify chapter has images
    if (chapterExists(slug, num)) {
      chapters.push({ number: num, file: entry.name });
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
): Promise<{ images: string[] } | null> {
  if (!chapterExists(slug, chapterNumber)) return null;
  return getImageListFromChapter(slug, chapterNumber);
}

/**
 * Get image as a file URI that React Native Image can display directly.
 */
export function getImageFileUri(
  slug: string,
  chapterNumber: number,
  imageName: string
): string | null {
  if (!chapterExists(slug, chapterNumber)) return null;
  return getImageUri(slug, chapterNumber, imageName);
}

/**
 * Delete an entire manga and its chapter directories.
 */
export async function deleteManga(slug: string): Promise<boolean> {
  deleteMangaDir(slug);

  const tracking = (await loadJson(TRACKING_FILE, {})) as Record<string, Record<string, unknown>>;
  delete tracking[slug];
  await saveJson(TRACKING_FILE, tracking);
  return true;
}

/**
 * Delete a single chapter.
 */
export async function deleteChapter(slug: string, chapterNumber: number): Promise<boolean> {
  if (!chapterExists(slug, chapterNumber)) return false;
  deleteChapterDir(slug, chapterNumber);
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
