import { useEffect, useState, useCallback } from "react";
import {
  View,
  Text,
  FlatList,
  TouchableOpacity,
  StyleSheet,
  RefreshControl,
  StatusBar,
} from "react-native";
import { useRouter } from "expo-router";
import { api, setApiBase, getApiBase } from "../src/lib/api";
import { t, toggleTheme, isDark } from "../src/lib/theme";
import { getRecentlyRead, RecentEntry } from "../src/lib/storage";

interface Manga {
  slug: string;
  title: string;
  chapter_count: number;
}

export default function HomeScreen() {
  const router = useRouter();
  const [manga, setManga] = useState<Manga[]>([]);
  const [recent, setRecent] = useState<RecentEntry[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  const [colors, setColors] = useState(t());
  const [showSettings, setShowSettings] = useState(false);
  const [apiInput, setApiInput] = useState("");

  const load = useCallback(async () => {
    const [data, recentData] = await Promise.all([
      api("/api/manga"),
      getRecentlyRead(),
    ]);
    if (data?.manga) setManga(data.manga);
    setRecent(recentData);
    setColors(t());
  }, []);

  useEffect(() => {
    load();
    getApiBase().then(setApiInput);
  }, [load]);

  const onRefresh = async () => {
    setRefreshing(true);
    await load();
    setRefreshing(false);
  };

  const handleTheme = async () => {
    await toggleTheme();
    setColors(t());
  };

  return (
    <View style={[s.container, { backgroundColor: colors.bg }]}>
      <StatusBar barStyle={isDark() ? "light-content" : "dark-content"} />

      {/* Header */}
      <View style={[s.header, { backgroundColor: colors.bg }]}>
        <View style={s.headerRow}>
          <View>
            <Text style={[s.logo, { color: colors.accent }]}>Manga</Text>
            <Text style={[s.logoThin, { color: colors.fg }]}>Reader</Text>
          </View>
          <View style={s.headerActions}>
            <TouchableOpacity
              style={[s.iconBtn, { backgroundColor: colors.bg3 }]}
              onPress={handleTheme}
            >
              <Text style={[s.iconText, { color: colors.fg2 }]}>{isDark() ? "☀" : "☾"}</Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[s.iconBtn, { backgroundColor: colors.bg3 }]}
              onPress={() => router.push("/scraper")}
            >
              <Text style={[s.iconText, { color: colors.accent }]}>+</Text>
            </TouchableOpacity>
          </View>
        </View>
      </View>

      <FlatList
        data={[]}
        renderItem={() => null}
        ListHeaderComponent={
          <View style={s.content}>
            {/* Quick Actions */}
            <View style={s.quickRow}>
              <TouchableOpacity
                style={[s.quickCard, { backgroundColor: colors.accent }]}
                onPress={() => router.push("/scraper")}
                activeOpacity={0.8}
              >
                <Text style={s.quickIcon}>⬇</Text>
                <Text style={s.quickLabel}>Scraper</Text>
                <Text style={s.quickSub}>Download manga</Text>
              </TouchableOpacity>
              <TouchableOpacity
                style={[s.quickCard, { backgroundColor: colors.bg3, borderWidth: 1, borderColor: colors.border }]}
                onPress={() => setShowSettings(!showSettings)}
                activeOpacity={0.8}
              >
                <Text style={[s.quickIcon, { color: colors.fg2 }]}>⚙</Text>
                <Text style={[s.quickLabel, { color: colors.fg }]}>Settings</Text>
                <Text style={[s.quickSub, { color: colors.fg3 }]}>API & config</Text>
              </TouchableOpacity>
            </View>

            {/* Settings Panel */}
            {showSettings && (
              <View style={[s.settingsPanel, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
                <Text style={[s.settingsLabel, { color: colors.fg2 }]}>API Base URL</Text>
                <View style={s.settingsRow}>
                  <Text style={[s.settingsInput, { color: colors.fg, backgroundColor: colors.bg3, borderColor: colors.border }]} selectable>
                    {apiInput}
                  </Text>
                </View>
                <Text style={[s.settingsHint, { color: colors.fg3 }]}>Restart app after changing</Text>
              </View>
            )}

            {/* Recently Read */}
            {recent.length > 0 && (
              <>
                <Text style={[s.sectionTitle, { color: colors.fg2 }]}>RECENTLY READ</Text>
                {recent.slice(0, 5).map((r) => (
                  <TouchableOpacity
                    key={r.manga}
                    style={[s.recentCard, { backgroundColor: colors.bg2, borderColor: colors.border }]}
                    onPress={() => router.push({ pathname: "/manga", params: { slug: r.manga } })}
                    activeOpacity={0.7}
                  >
                    <View style={[s.recentDot, { backgroundColor: colors.accent }]} />
                    <View style={s.recentInfo}>
                      <Text style={[s.recentTitle, { color: colors.fg }]} numberOfLines={1}>
                        {r.manga}
                      </Text>
                      <Text style={[s.recentCh, { color: colors.fg3 }]}>
                        Chapter {r.chapter}
                      </Text>
                    </View>
                    <Text style={[s.recentArrow, { color: colors.fg3 }]}>→</Text>
                  </TouchableOpacity>
                ))}
              </>
            )}

            {/* Library */}
            <Text style={[s.sectionTitle, { color: colors.fg2 }]}>LIBRARY</Text>
            {manga.length === 0 ? (
              <View style={[s.emptyCard, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
                <Text style={[s.emptyIcon, { color: colors.fg3 }]}>📖</Text>
                <Text style={[s.emptyTitle, { color: colors.fg2 }]}>No manga yet</Text>
                <Text style={[s.emptySub, { color: colors.fg3 }]}>
                  Use the Scraper to download your first manga
                </Text>
              </View>
            ) : (
              manga.map((m) => (
                <TouchableOpacity
                  key={m.slug}
                  style={[s.mangaCard, { backgroundColor: colors.bg2, borderColor: colors.border }]}
                  onPress={() => router.push({ pathname: "/manga", params: { slug: m.slug } })}
                  activeOpacity={0.7}
                >
                  <View style={[s.mangaThumb, { backgroundColor: colors.bg3 }]}>
                    <Text style={[s.mangaThumbText, { color: colors.accent }]}>
                      {m.title.charAt(0)}
                    </Text>
                  </View>
                  <View style={s.mangaInfo}>
                    <Text style={[s.mangaTitle, { color: colors.fg }]} numberOfLines={1}>
                      {m.title}
                    </Text>
                    <Text style={[s.mangaChapters, { color: colors.fg3 }]}>
                      {m.chapter_count} chapters
                    </Text>
                  </View>
                  <Text style={[s.mangaArrow, { color: colors.fg3 }]}>→</Text>
                </TouchableOpacity>
              ))
            )}
          </View>
        }
        refreshControl={
          <RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={colors.accent} />
        }
        contentContainerStyle={{ paddingBottom: 40 }}
      />
    </View>
  );
}

const s = StyleSheet.create({
  container: { flex: 1 },
  header: { paddingTop: 56, paddingHorizontal: 20, paddingBottom: 8 },
  headerRow: { flexDirection: "row", justifyContent: "space-between", alignItems: "center" },
  logo: { fontSize: 28, fontWeight: "900", letterSpacing: -1 },
  logoThin: { fontSize: 28, fontWeight: "200", letterSpacing: -1, marginTop: -4 },
  headerActions: { flexDirection: "row", gap: 8 },
  iconBtn: { width: 40, height: 40, borderRadius: 12, justifyContent: "center", alignItems: "center" },
  iconText: { fontSize: 18, fontWeight: "700" },
  content: { padding: 20 },
  quickRow: { flexDirection: "row", gap: 12, marginBottom: 28 },
  quickCard: {
    flex: 1,
    padding: 18,
    borderRadius: 16,
    minHeight: 100,
    justifyContent: "flex-end",
  },
  quickIcon: { fontSize: 24, marginBottom: 8, color: "#fff" },
  quickLabel: { fontSize: 17, fontWeight: "800", color: "#fff" },
  quickSub: { fontSize: 12, color: "rgba(255,255,255,0.7)", marginTop: 2 },
  settingsPanel: {
    padding: 16,
    borderRadius: 14,
    borderWidth: 1,
    marginBottom: 24,
  },
  settingsLabel: { fontSize: 11, fontWeight: "700", textTransform: "uppercase", letterSpacing: 1, marginBottom: 8 },
  settingsRow: { flexDirection: "row", gap: 8 },
  settingsInput: {
    flex: 1,
    borderWidth: 1,
    borderRadius: 10,
    padding: 10,
    fontSize: 13,
    fontFamily: "monospace",
  },
  settingsHint: { fontSize: 11, marginTop: 6 },
  sectionTitle: {
    fontSize: 11,
    fontWeight: "800",
    textTransform: "uppercase",
    letterSpacing: 1.5,
    marginBottom: 12,
    marginTop: 8,
  },
  recentCard: {
    flexDirection: "row",
    alignItems: "center",
    padding: 14,
    borderRadius: 12,
    borderWidth: 1,
    marginBottom: 8,
  },
  recentDot: { width: 6, height: 6, borderRadius: 3, marginRight: 12 },
  recentInfo: { flex: 1 },
  recentTitle: { fontSize: 15, fontWeight: "600" },
  recentCh: { fontSize: 12, marginTop: 2 },
  recentArrow: { fontSize: 16 },
  emptyCard: {
    padding: 40,
    borderRadius: 16,
    borderWidth: 1,
    alignItems: "center",
  },
  emptyIcon: { fontSize: 36, marginBottom: 12 },
  emptyTitle: { fontSize: 17, fontWeight: "700", marginBottom: 4 },
  emptySub: { fontSize: 13, textAlign: "center", lineHeight: 18 },
  mangaCard: {
    flexDirection: "row",
    alignItems: "center",
    padding: 14,
    borderRadius: 12,
    borderWidth: 1,
    marginBottom: 8,
  },
  mangaThumb: {
    width: 44,
    height: 44,
    borderRadius: 10,
    justifyContent: "center",
    alignItems: "center",
    marginRight: 14,
  },
  mangaThumbText: { fontSize: 20, fontWeight: "900" },
  mangaInfo: { flex: 1 },
  mangaTitle: { fontSize: 15, fontWeight: "600" },
  mangaChapters: { fontSize: 12, marginTop: 2 },
  mangaArrow: { fontSize: 16 },
});
