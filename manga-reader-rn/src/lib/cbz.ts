import { File, Directory, Paths } from "expo-file-system/next";
import { ImageManipulator } from "expo-image-manipulator";

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
 * Returns display names and a sizes map from segments.json.
 */
export async function getImageListFromChapter(
  slug: string,
  chapterNumber: number
): Promise<{ images: string[]; sizes: Record<string, ImageDimensions> }> {
  const chDir = getChapterDir(slug, chapterNumber);
  if (!chDir.exists) return { images: [], sizes: {} };

  const entries = chDir.list();
  const imageFiles = entries
    .filter(
      (e) =>
        e instanceof File &&
        /\.(jpe?g|png|webp|gif)$/i.test(e.name) &&
        !e.name.startsWith("_")
    )
    .map((e) => (e as File).name)
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

  const images: string[] = [];
  const sizes: Record<string, ImageDimensions> = {};

  for (const fileName of imageFiles) {
    images.push(fileName);
  }

  // Load dimensions from segments.json
  const metaFile = new File(chDir, "segments.json");
  if (metaFile.exists) {
    try {
      const json = await metaFile.text();
      const meta = JSON.parse(json) as Record<string, ImageDimensions>;
      for (const [name, dims] of Object.entries(meta)) {
        sizes[name] = dims;
      }
    } catch {}
  }

  return { images, sizes };
}

/**
 * Get the URI for a chapter image (ready for <Image source={{uri}}>)
 */
export function getImageUri(slug: string, chapterNumber: number, imageName: string): string {
  const chDir = getChapterDir(slug, chapterNumber);
  const file = new File(chDir, imageName);
  return file.uri;
}

/**
 * Save downloaded images to a chapter directory.
 * Tall images (>MAX_SEGMENT_HEIGHT) are split into segments.
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

  // Temp dir for manipulation
  const tmpDir = new Directory(chDir, "_tmp");
  if (!tmpDir.exists) tmpDir.create();

  const sizes: Record<string, ImageDimensions> = {};

  for (let i = 0; i < images.length; i++) {
    const ext = getExtension(images[i].name);
    const padded = String(i + 1).padStart(4, "0");

    // Write raw bytes to temp file so ImageManipulator can read it
    const tmpFile = new File(tmpDir, `${padded}.${ext}`);
    tmpFile.write(images[i].data);

    try {
      const ctx = ImageManipulator.manipulate(tmpFile.uri);
      // Load the image to get dimensions
      const ref = await ctx.renderAsync();
      const imgW = ref.width;
      const imgH = ref.height;

      if (imgH <= MAX_SEGMENT_HEIGHT) {
        // Short image — keep as-is
        const finalFile = new File(chDir, `${padded}.${ext}`);
        finalFile.write(images[i].data);
        sizes[`${padded}.${ext}`] = { w: imgW, h: imgH };
      } else {
        // Tall image — split into segments
        const segmentCount = Math.ceil(imgH / MAX_SEGMENT_HEIGHT);
        for (let s = 0; s < segmentCount; s++) {
          const originY = s * MAX_SEGMENT_HEIGHT;
          const segH = Math.min(MAX_SEGMENT_HEIGHT, imgH - originY);
          const segName = s === 0 ? `${padded}.${ext}` : `${padded}:s${s + 1}.${ext}`;

          const segCtx = ImageManipulator.manipulate(tmpFile.uri);
          segCtx.crop({ originX: 0, originY, width: imgW, height: segH });
          const segRef = await segCtx.renderAsync();
          const segResult = await segRef.saveAsync({ compress: 0.92 });

          const finalFile = new File(chDir, segName);
          const savedFile = new File(segResult.uri);
          finalFile.write(await savedFile.bytes());

          sizes[segName] = { w: segResult.width, h: segResult.height };
        }
      }
    } catch (e) {
      // Fallback: save original
      const finalFile = new File(chDir, `${padded}.${ext}`);
      finalFile.write(images[i].data);
      sizes[`${padded}.${ext}`] = { w: 0, h: 0 };
    }
  }

  // Save dimensions metadata
  const metaFile = new File(chDir, "segments.json");
  metaFile.write(JSON.stringify(sizes));

  // Cleanup temp dir
  try { tmpDir.delete(); } catch {}

  return chDir.uri;
}

/**
 * Check if a chapter exists on disk.
 */
export function chapterExists(slug: string, chapterNumber: number): boolean {
  const chDir = getChapterDir(slug, chapterNumber);
  if (!chDir.exists) return false;
  const entries = chDir.list();
  return entries.some(
    (e) =>
      e instanceof File &&
      /\.(jpe?g|png|webp|gif)$/i.test(e.name) &&
      !e.name.startsWith("_")
  );
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
