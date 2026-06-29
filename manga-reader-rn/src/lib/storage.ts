import { File, Directory, Paths } from "expo-file-system/next";
import AsyncStorage from "@react-native-async-storage/async-storage";

const DATA_DIR = new Directory(Paths.document, "data");

export interface RecentEntry {
  manga: string;
  chapter: number;
  time: number;
}

export interface WishlistEntry {
  title: string;
  url: string;
  added: number;
}

/**
 * Load a JSON file from the data directory.
 */
export async function loadJson(
  filename: string,
  defaultValue: any = {}
): Promise<any> {
  try {
    const file = new File(DATA_DIR, filename);
    if (!file.exists) return defaultValue;
    const content = await file.text();
    return JSON.parse(content);
  } catch {
    return defaultValue;
  }
}

/**
 * Save a JSON file to the data directory.
 */
export async function saveJson(
  filename: string,
  data: any
): Promise<void> {
  try {
    if (!DATA_DIR.exists) {
      DATA_DIR.create();
    }
    const file = new File(DATA_DIR, filename);
    await file.write(JSON.stringify(data, null, 2));
  } catch (e) {
    console.error("saveJson error:", e);
  }
}

/**
 * Get recently read manga list.
 */
export async function getRecentlyRead(): Promise<RecentEntry[]> {
  const raw = await AsyncStorage.getItem("manga_recently_read");
  return raw ? JSON.parse(raw) : [];
}

/**
 * Save a manga to recently read list.
 */
export async function saveRecentlyRead(manga: string, chapter: number): Promise<void> {
  const list = await getRecentlyRead();
  const filtered = list.filter((r) => r.manga !== manga);
  filtered.unshift({ manga, chapter, time: Date.now() });
  await AsyncStorage.setItem("manga_recently_read", JSON.stringify(filtered.slice(0, 10)));
}

/**
 * Get wishlist.
 */
export async function getWishlist(): Promise<WishlistEntry[]> {
  const raw = await AsyncStorage.getItem("manga_wishlist");
  return raw ? JSON.parse(raw) : [];
}

/**
 * Add to wishlist.
 */
export async function addToWishlist(title: string, url: string): Promise<void> {
  const list = await getWishlist();
  if (!list.find((w) => w.url === url)) {
    list.push({ title, url, added: Date.now() });
    await AsyncStorage.setItem("manga_wishlist", JSON.stringify(list));
  }
}

/**
 * Remove from wishlist.
 */
export async function removeFromWishlist(url: string): Promise<void> {
  const list = await getWishlist();
  await AsyncStorage.setItem(
    "manga_wishlist",
    JSON.stringify(list.filter((w) => w.url !== url))
  );
}
