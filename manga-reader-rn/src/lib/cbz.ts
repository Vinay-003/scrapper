import { File, Directory, Paths } from "expo-file-system/next";

export interface ImageDimensions {
  w: number;
  h: number;
}

const MAX_SEGMENT_HEIGHT = 2000;

/**
 * Get the directory for a chapter's images.
 */
export function getChapterDir(slug: string, chapterNumber: number): Directory {
  const mangaDir = new Directory(Paths.document, "manga");
  const slugDir = new Directory(mangaDir, slug);
  const chDir = new Directory(slugDir, `ch${chapterNumber}`);
  return chDir;
}

/**
 * Get file path for a chapter image.
 */
export function getChapterImagePath(slug: string, chapterNumber: number, index: number): string {
  const chDir = getChapterDir(slug, chapterNumber);
  const padded = String(index + 1).padStart(4, "0");
  const file = new File(chDir, `${padded}.jpg`);
  return file.uri;
}

/**
 * Read image list from a chapter directory.
 * Returns display names (with :sN suffix for segments) and a sizes map.
 */
export async function getImageListFromChapter(
  slug: string,
  chapterNumber: number
): Promise<{ images: string[]; sizes: Record<string, ImageDimensions> }> {
  const chDir = getChapterDir(slug, chapterNumber);
  if (!chDir.exists) return { images: [], sizes: {} };

  const entries = chDir.list();
  const imageFiles = entries
    .filter((e) => e instanceof File && /\.(jpe?g|png|webp|gif)$/i.test(e.name))
    .map((e) => (e as File).name)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

  const images: string[] = [];
  const sizes: Record<string, ImageDimensions> = {};

  for (const fileName of imageFiles) {
    // For now, serve images without segmentation (simpler, no OOM)
    // Segmentation can be added later with expo-image-manipulator
    images.push(fileName);
  }

  return { images, sizes };
}

/**
 * Get the URI for a chapter image (ready for <Image source={{uri}}>)
 */
export function getImageUri(slug: string, chapterNumber: number, imageName: string): string {
  const chDir = getChapterDir(slug, chapterNumber);
  const cleanName = imageName.replace(/:s\d+$/, "");
  const file = new File(chDir, cleanName);
  return file.uri;
}

/**
 * Save downloaded images to a chapter directory.
 * images: [{ name: string, data: Uint8Array }]
 */
export async function saveChapterImages(
  slug: string,
  chapterNumber: number,
  images: { name: string; data: Uint8Array }[]
): Promise<string> {
  const mangaDir = new Directory(Paths.document, "manga");
  if (!mangaDir.exists) mangaDir.create();

  const slugDir = new Directory(mangaDir, slug);
  if (!slugDir.exists) slugDir.create();

  const chDir = new Directory(slugDir, `ch${chapterNumber}`);
  if (!chDir.exists) chDir.create();

  for (let i = 0; i < images.length; i++) {
    const ext = getExtension(images[i].name);
    const padded = String(i + 1).padStart(4, "0");
    const file = new File(chDir, `${padded}.${ext}`);

    // Convert Uint8Array to base64 and write
    const base64 = uint8ArrayToBase64(images[i].data);
    await file.write(base64);
  }

  return chDir.uri;
}

/**
 * Check if a chapter exists on disk.
 */
export function chapterExists(slug: string, chapterNumber: number): boolean {
  const chDir = getChapterDir(slug, chapterNumber);
  if (!chDir.exists) return false;
  const entries = chDir.list();
  return entries.some((e) => e instanceof File && /\.(jpe?g|png|webp|gif)$/i.test(e.name));
}

/**
 * Delete a chapter directory.
 */
export function deleteChapterDir(slug: string, chapterNumber: number): void {
  const chDir = getChapterDir(slug, chapterNumber);
  if (chDir.exists) chDir.delete();
}

/**
 * Delete all chapter directories for a manga.
 */
export function deleteMangaDir(slug: string): void {
  const mangaDir = new Directory(Paths.document, "manga");
  if (!mangaDir.exists) return;
  const slugDir = new Directory(mangaDir, slug);
  if (slugDir.exists) slugDir.delete();
}

function getExtension(name: string): string {
  const m = name.match(/\.(\w+)$/);
  const ext = m ? m[1].toLowerCase() : "jpg";
  if (ext === "jpeg") return "jpg";
  return ext;
}

function uint8ArrayToBase64(arr: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < arr.length; i++) {
    binary += String.fromCharCode(arr[i]);
  }
  return btoa(binary);
}
