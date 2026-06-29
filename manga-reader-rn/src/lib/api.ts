import AsyncStorage from "@react-native-async-storage/async-storage";

const STORAGE_KEY = "manga_api_base";

let API_BASE = "http://10.47.169.128:8000";

export async function getApiBase(): Promise<string> {
  const stored = await AsyncStorage.getItem(STORAGE_KEY);
  if (stored) API_BASE = stored;
  return API_BASE;
}

export async function setApiBase(url: string): Promise<void> {
  API_BASE = url;
  await AsyncStorage.setItem(STORAGE_KEY, url);
}

function encodeSlug(slug: string): string {
  return encodeURIComponent(slug.replace(/%20/g, " ").replace(/\+/g, " "));
}

export async function api(path: string, opts?: RequestInit): Promise<any> {
  const base = await getApiBase();
  try {
    const r = await fetch(`${base}${path}`, {
      ...opts,
      headers: { "Content-Type": "application/json", ...opts?.headers },
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return await r.json();
  } catch (e) {
    console.error("API error:", path, e);
    return null;
  }
}

export { encodeSlug };
