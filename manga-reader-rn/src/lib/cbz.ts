import { File, Directory, Paths } from "expo-file-system/next";
import JSZip from "jszip";

export interface ImageDimensions {
  w: number;
  h: number;
}

const MAX_SEGMENT_HEIGHT = 2000;

/**
 * Read image list and dimensions from a CBZ file.
 * Returns display names (with :sN suffix for segments) and a sizes map.
 */
export async function getImageListFromCbz(
  cbzPath: string
): Promise<{ images: string[]; sizes: Record<string, ImageDimensions> }> {
  const file = new File(cbzPath);
  if (!file.exists) return { images: [], sizes: {} };

  const base64 = await file.base64();
  const zip = await JSZip.loadAsync(base64, { base64: true });

  const imageFiles = Object.keys(zip.files)
    .filter((name) => !zip.files[name].dir && /\.(jpe?g|png|webp|gif)$/i.test(name))
    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));

  const images: string[] = [];
  const sizes: Record<string, ImageDimensions> = {};

  for (const fileName of imageFiles) {
    const blob = await zip.files[fileName].async("blob");
    const dims = await getImageDimensions(blob);

    if (!dims || dims.h <= MAX_SEGMENT_HEIGHT) {
      images.push(fileName);
      if (dims) sizes[fileName] = dims;
    } else {
      const segCount = Math.ceil(dims.h / MAX_SEGMENT_HEIGHT);
      for (let i = 0; i < segCount; i++) {
        const segName = `${fileName}:s${i}`;
        images.push(segName);
        const segH = Math.min(MAX_SEGMENT_HEIGHT, dims.h - i * MAX_SEGMENT_HEIGHT);
        sizes[segName] = { w: dims.w, h: segH };
      }
    }
  }

  return { images, sizes };
}

/**
 * Read a full image or a segment from a CBZ file.
 * Returns { base64, contentType } for use with RN Image.
 */
export async function readImageFromCbz(
  cbzPath: string,
  imageName: string
): Promise<{ base64: string; contentType: string }> {
  const segMatch = imageName.match(/^(.+):s(\d+)$/);
  const fileName = segMatch ? segMatch[1] : imageName;
  const segIndex = segMatch ? parseInt(segMatch[2], 10) : -1;

  const file = new File(cbzPath);
  if (!file.exists) throw new Error(`CBZ not found: ${cbzPath}`);

  const base64 = await file.base64();
  const zip = await JSZip.loadAsync(base64, { base64: true });
  const zipFile = zip.files[fileName];
  if (!zipFile) throw new Error(`Image not found: ${fileName}`);

  const contentType = detectContentType(fileName);

  if (segIndex === -1) {
    const arrayBuffer = await zipFile.async("arraybuffer");
    const resultBase64 = arrayBufferToBase64(arrayBuffer);
    return { base64: resultBase64, contentType };
  }

  // For segments, we need to extract and crop
  // This is a simplified version - in production, use expo-image-manipulator
  const arrayBuffer = await zipFile.async("arraybuffer");
  const resultBase64 = arrayBufferToBase64(arrayBuffer);
  return { base64: resultBase64, contentType };
}

/**
 * Create a CBZ file from a list of images.
 * images: [{ name: string, data: Uint8Array }]
 * Returns the path to the created CBZ.
 */
export async function createCbz(
  mangaDir: string,
  chapterNumber: number,
  images: { name: string; data: Uint8Array }[]
): Promise<string> {
  const zip = new JSZip();

  for (let i = 0; i < images.length; i++) {
    const ext = getExtension(images[i].name);
    const padded = String(i + 1).padStart(4, "0");
    const newName = `${padded}.${ext}`;
    zip.file(newName, images[i].data);
  }

  const content = await zip.generateAsync({ type: "uint8array", compression: "STORE" });

  const mangaDirPath = new Directory(Paths.document, "manga");
  if (!mangaDirPath.exists) {
    mangaDirPath.create();
  }

  const cbzFile = new File(mangaDirPath, `${mangaDir}_chapter_${chapterNumber}.cbz`);
  const base64 = uint8ArrayToBase64(content);
  await cbzFile.write(base64);

  return cbzFile.uri;
}

function getExtension(name: string): string {
  const m = name.match(/\.(\w+)$/);
  const ext = m ? m[1].toLowerCase() : "jpg";
  if (ext === "jpeg") return "jpg";
  if (ext === "png") return "png";
  if (ext === "webp") return "webp";
  if (ext === "gif") return "gif";
  return "jpg";
}

function detectContentType(fileName: string): string {
  const ext = fileName.split(".").pop()?.toLowerCase() || "";
  if (ext === "png") return "image/png";
  if (ext === "webp") return "image/webp";
  if (ext === "gif") return "image/gif";
  return "image/jpeg";
}

function arrayBufferToBase64(buffer: ArrayBuffer): string {
  const bytes = new Uint8Array(buffer);
  let binary = "";
  for (let i = 0; i < bytes.byteLength; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return btoa(binary);
}

function uint8ArrayToBase64(arr: Uint8Array): string {
  let binary = "";
  for (let i = 0; i < arr.length; i++) {
    binary += String.fromCharCode(arr[i]);
  }
  return btoa(binary);
}

async function getImageDimensions(_blob: Blob): Promise<ImageDimensions | null> {
  // In React Native, we can't easily get dimensions from a blob.
  // This is a placeholder. In practice, we'd use expo-image-manipulator
  // or a similar library for image operations.
  return null;
}
