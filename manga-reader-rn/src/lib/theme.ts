import AsyncStorage from "@react-native-async-storage/async-storage";

const THEME_KEY = "manga_theme";

export type Theme = "dark" | "light";

export const colors = {
  dark: {
    bg: "#0a0a0a",
    bg2: "#141414",
    bg3: "#1e1e1e",
    fg: "#f0f0f0",
    fg2: "#888",
    border: "#2a2a2a",
    accent: "#8b5cf6",
    accent2: "#7c3aed",
    danger: "#ef4444",
    success: "#22c55e",
  },
  light: {
    bg: "#f8f8f8",
    bg2: "#fff",
    bg3: "#eee",
    fg: "#1a1a1a",
    fg2: "#666",
    border: "#ddd",
    accent: "#7c3aed",
    accent2: "#6d28d9",
    danger: "#ef4444",
    success: "#22c55e",
  },
};

let currentTheme: Theme = "dark";

export async function loadTheme(): Promise<Theme> {
  const stored = await AsyncStorage.getItem(THEME_KEY);
  currentTheme = (stored as Theme) || "dark";
  return currentTheme;
}

export async function setTheme(t: Theme): Promise<void> {
  currentTheme = t;
  await AsyncStorage.setItem(THEME_KEY, t);
}

export function getTheme(): Theme {
  return currentTheme;
}

export function t() {
  return colors[currentTheme];
}
