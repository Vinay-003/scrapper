import AsyncStorage from "@react-native-async-storage/async-storage";

export interface ThemeColors {
  bg: string;
  bg2: string;
  bg3: string;
  fg: string;
  fg2: string;
  fg3: string;
  accent: string;
  accentDim: string;
  danger: string;
  success: string;
  border: string;
  card: string;
  cardBorder: string;
  shadow: string;
}

const DARK: ThemeColors = {
  bg: "#0a0a0c",
  bg2: "#131318",
  bg3: "#1c1c24",
  fg: "#f0f0f5",
  fg2: "#8a8a9a",
  fg3: "#55556a",
  accent: "#00e5c3",
  accentDim: "#00b89a",
  danger: "#ff4d6a",
  success: "#00e5c3",
  border: "#1e1e2a",
  card: "#131318",
  cardBorder: "#1e1e2a",
  shadow: "rgba(0,229,195,0.06)",
};

const LIGHT: ThemeColors = {
  bg: "#f5f5f7",
  bg2: "#ffffff",
  bg3: "#eaeaef",
  fg: "#0a0a0c",
  fg2: "#6a6a7a",
  fg3: "#9a9aaa",
  accent: "#008b76",
  accentDim: "#006b5a",
  danger: "#d93050",
  success: "#008b76",
  border: "#d8d8e0",
  card: "#ffffff",
  cardBorder: "#e0e0e8",
  shadow: "rgba(0,0,0,0.04)",
};

const STORAGE_KEY = "manga_theme";

let _dark = true;

export function t(): ThemeColors {
  return _dark ? DARK : LIGHT;
}

export function isDark(): boolean {
  return _dark;
}

export async function loadTheme(): Promise<ThemeColors> {
  const v = await AsyncStorage.getItem(STORAGE_KEY);
  _dark = v !== "light";
  return t();
}

export async function toggleTheme(): Promise<ThemeColors> {
  _dark = !_dark;
  await AsyncStorage.setItem(STORAGE_KEY, _dark ? "dark" : "light");
  return t();
}
