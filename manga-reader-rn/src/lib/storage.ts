import AsyncStorage from "@react-native-async-storage/async-storage";

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

export async function getRecentlyRead(): Promise<RecentEntry[]> {
  const raw = await AsyncStorage.getItem("manga_recently_read");
  return raw ? JSON.parse(raw) : [];
}

export async function saveRecentlyRead(manga: string, chapter: number): Promise<void> {
  const list = await getRecentlyRead();
  const filtered = list.filter((r) => r.manga !== manga);
  filtered.unshift({ manga, chapter, time: Date.now() });
  await AsyncStorage.setItem("manga_recently_read", JSON.stringify(filtered.slice(0, 10)));
}

export async function getWishlist(): Promise<WishlistEntry[]> {
  const raw = await AsyncStorage.getItem("manga_wishlist");
  return raw ? JSON.parse(raw) : [];
}

export async function addToWishlist(title: string, url: string): Promise<void> {
  const list = await getWishlist();
  if (!list.find((w) => w.url === url)) {
    list.push({ title, url, added: Date.now() });
    await AsyncStorage.setItem("manga_wishlist", JSON.stringify(list));
  }
}

export async function removeFromWishlist(url: string): Promise<void> {
  const list = await getWishlist();
  await AsyncStorage.setItem(
    "manga_wishlist",
    JSON.stringify(list.filter((w) => w.url !== url))
  );
}
