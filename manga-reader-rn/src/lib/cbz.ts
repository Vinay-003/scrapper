import { File, Directory, Paths } from "expo-file-system/next";
import { Platform, Image } from "react-native";
import { ImageManipulator } from "expo-image-manipulator";

const MAX_SEGMENT_HEIGHT = 2000;

function getSizeAsync(uri: string): Promise<{ w: number; h: number }> {
  return new Promise((resolve) => {
    Image.getSize(
      uri,
      (w, h) => resolve({ w, h }),
      () => resolve({ w: 0, h: 0 })
    );
  });
}

export function getChapterDir(slug: string, chapterNumber: number): Directory {
  const mangaDir = new Directory(Paths.document, "manga");
  const slugDir = new Directory(mangaDir, slug);
  const chDir = new Directory(slugDir, `ch${chapterNumber}`);
  return chDir;
}

export function getCompatUri(file: File): string {
  if (Platform.OS === "android") return file.contentUri;
  return file.uri;
}

export async function getImageListFromChapter(
  slug: string,
  chapterNumber: number
): Promise<{ names: string[]; uris: string[] }> {
  const chDir = getChapterDir(slug, chapterNumber);
  if (!chDir.exists) return { names: [], uris: [] };

  const entries = chDir.list();
  const imageFiles = entries
    .filter((e) => e instanceof File && /\.(jpe?g|png|webp|gif)$/i.test(e.name) && !e.name.startsWith("_"))
    .map((e) => e as File)
    .sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true }));

  return {
    names: imageFiles.map((f) => f.name),
    uris: imageFiles.map((f) => getCompatUri(f)),
  };
}

export function getImageUri(slug: string, chapterNumber: number, imageName: string): string {
  const chDir = getChapterDir(slug, chapterNumber);
  const file = new File(chDir, imageName);
  return getCompatUri(file);
}

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
    file.write(images[i].data);

    try {
      const { w: imgW, h: imgH } = await getSizeAsync(file.uri);
      console.log(`[CBZ] ${padded}.${ext}: ${imgW}x${imgH}`);

      if (imgH > 0 && imgW > 0 && imgH > MAX_SEGMENT_HEIGHT) {
        // Use renderAsync to get actual bitmap dimensions (may differ from Image.getSize)
        const probeCtx = ImageManipulator.manipulate(file.uri);
        const probeRef = await probeCtx.renderAsync();
        const actualH = probeRef.height;
        const actualW = probeRef.width;

        console.log(`[CBZ] actual bitmap: ${actualW}x${actualH} (getSize said ${imgW}x${imgH})`);

        const segmentCount = Math.ceil(actualH / MAX_SEGMENT_HEIGHT);
        console.log(`[CBZ] splitting into ${segmentCount} segments`);
        for (let s = 0; s < segmentCount; s++) {
          const originY = s * MAX_SEGMENT_HEIGHT;
          const segH = Math.min(MAX_SEGMENT_HEIGHT, actualH - originY);
          if (segH <= 0) break;
          const segName = s === 0 ? `${padded}.${ext}` : `${padded}_s${s + 1}.${ext}`;

          try {
            const segCtx = ImageManipulator.manipulate(file.uri);
            segCtx.crop({ originX: 0, originY, width: actualW, height: segH });
            const segRef = await segCtx.renderAsync();
            const segResult = await segRef.saveAsync({ compress: 1 });

            const segFile = new File(chDir, segName);
            const savedFile = new File(segResult.uri);
            segFile.write(await savedFile.bytes());
            console.log(`[CBZ] saved segment: ${segName} (${segResult.width}x${segResult.height})`);
          } catch (segErr: any) {
            console.log(`[CBZ] segment ${s + 1} failed: ${segErr.message}`);
            break;
          }
        }
      }
    } catch (e: any) {
      console.log(`[CBZ] segmentation failed for ${padded}.${ext}: ${e.message}`);
    }
  }

  return chDir.uri;
}

export function chapterExists(slug: string, chapterNumber: number): boolean {
  const chDir = getChapterDir(slug, chapterNumber);
  if (!chDir.exists) return false;
  const entries = chDir.list();
  return entries.some(
    (e) => e instanceof File && /\.(jpe?g|png|webp|gif)$/i.test(e.name) && !e.name.startsWith("_")
  );
}

export function deleteChapterDir(slug: string, chapterNumber: number): void {
  const chDir = getChapterDir(slug, chapterNumber);
  if (chDir.exists) chDir.delete();
}

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
