import { useEffect, useState, useCallback } from "react";
import {
  View,
  Text,
  FlatList,
  TouchableOpacity,
  StyleSheet,
  RefreshControl,
  TextInput,
} from "react-native";
import { useRouter } from "expo-router";
import { api } from "../src/lib/api";
import { t, getTheme, setTheme, loadTheme } from "../src/lib/theme";
import { getRecentlyRead, type RecentEntry } from "../src/lib/storage";

interface Manga {
  slug: string;
  title: string;
  chapters: number;
  last_chapter: number | null;
}

export default function HomeScreen() {
  const router = useRouter();
  const [manga, setManga] = useState<Manga[]>([]);
  const [recent, setRecent] = useState<RecentEntry[]>([]);
  const [refreshing, setRefreshing] = useState(false);
  const [theme, setThemeState] = useState(getTheme());
  const colors = t();

  const load = useCallback(async () => {
    const [data, recentData] = await Promise.all([
      api("/api/manga"),
      getRecentlyRead(),
    ]);
    setManga(data?.manga || []);
    setRecent(recentData);
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const onRefresh = async () => {
    setRefreshing(true);
    await load();
    setRefreshing(false);
  };

  const toggleTheme = async () => {
    const next = theme === "dark" ? "light" : "dark";
    await setTheme(next);
    setThemeState(next);
  };

  return (
    <View style={[styles.container, { backgroundColor: colors.bg }]}>
      {/* Header */}
      <View style={styles.header}>
        <Text style={[styles.title, { color: colors.fg }]}>Manga Reader</Text>
        <View style={styles.headerRow}>
          <TouchableOpacity
            style={[styles.btn, { backgroundColor: colors.accent }]}
            onPress={() => router.push("/scraper")}
          >
            <Text style={styles.btnText}>Scraper</Text>
          </TouchableOpacity>
          <TouchableOpacity
            style={[styles.btn, { backgroundColor: colors.bg3 }]}
            onPress={toggleTheme}
          >
            <Text style={[styles.btnText, { color: colors.fg }]}>
              {theme === "dark" ? "Light" : "Dark"}
            </Text>
          </TouchableOpacity>
        </View>
      </View>

      {/* Recently Read */}
      {recent.length > 0 && (
        <View style={styles.section}>
          <Text style={[styles.sectionTitle, { color: colors.fg }]}>
            Recently Read
          </Text>
          <FlatList
            horizontal
            data={recent}
            keyExtractor={(item) => item.manga}
            showsHorizontalScrollIndicator={false}
            renderItem={({ item }) => (
              <TouchableOpacity
                style={[styles.recentCard, { backgroundColor: colors.bg2, borderColor: colors.border }]}
                onPress={() =>
                  router.push({ pathname: "/reader", params: { slug: item.manga, chapter: item.chapter } })
                }
              >
                <Text style={[styles.recentTitle, { color: colors.fg }]} numberOfLines={1}>
                  {item.manga.replace(/-/g, " ")}
                </Text>
                <Text style={[styles.recentChapter, { color: colors.fg2 }]}>
                  Ch {item.chapter}
                </Text>
              </TouchableOpacity>
            )}
          />
        </View>
      )}

      {/* Manga List */}
      <FlatList
        data={manga}
        keyExtractor={(item) => item.slug}
        contentContainerStyle={styles.list}
        refreshControl={
          <RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={colors.accent} />
        }
        ListEmptyComponent={
          <View style={styles.empty}>
            <Text style={[styles.emptyText, { color: colors.fg2 }]}>
              No manga found. Open the Scraper to download some.
            </Text>
          </View>
        }
        renderItem={({ item }) => (
          <TouchableOpacity
            style={[styles.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}
            onPress={() => router.push({ pathname: "/manga", params: { slug: item.slug } })}
          >
            <View style={[styles.cardIcon, { backgroundColor: colors.bg3 }]}>
              <Text style={[styles.cardIconText, { color: colors.accent }]}>
                {item.title.charAt(0)}
              </Text>
            </View>
            <View style={styles.cardBody}>
              <Text style={[styles.cardTitle, { color: colors.fg }]} numberOfLines={1}>
                {item.title}
              </Text>
              <Text style={[styles.cardMeta, { color: colors.fg2 }]}>
                {item.chapters} chapters
              </Text>
            </View>
          </TouchableOpacity>
        )}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  header: { paddingTop: 60, paddingHorizontal: 20, paddingBottom: 16 },
  title: { fontSize: 28, fontWeight: "800", marginBottom: 12 },
  headerRow: { flexDirection: "row", gap: 10 },
  btn: { paddingHorizontal: 16, paddingVertical: 10, borderRadius: 10 },
  btnText: { color: "#fff", fontWeight: "600", fontSize: 14 },
  section: { marginBottom: 16 },
  sectionTitle: { fontSize: 18, fontWeight: "700", paddingHorizontal: 20, marginBottom: 10 },
  recentCard: {
    width: 160,
    padding: 14,
    borderRadius: 12,
    marginLeft: 20,
    borderWidth: 1,
  },
  recentTitle: { fontSize: 14, fontWeight: "600", marginBottom: 4 },
  recentChapter: { fontSize: 12 },
  list: { padding: 20 },
  card: {
    flexDirection: "row",
    alignItems: "center",
    padding: 14,
    borderRadius: 12,
    marginBottom: 10,
    borderWidth: 1,
  },
  cardIcon: {
    width: 48,
    height: 48,
    borderRadius: 12,
    justifyContent: "center",
    alignItems: "center",
    marginRight: 14,
  },
  cardIconText: { fontSize: 20, fontWeight: "800" },
  cardBody: { flex: 1 },
  cardTitle: { fontSize: 16, fontWeight: "600", marginBottom: 2 },
  cardMeta: { fontSize: 13 },
  empty: { padding: 40, alignItems: "center" },
  emptyText: { fontSize: 15, textAlign: "center" },
});
