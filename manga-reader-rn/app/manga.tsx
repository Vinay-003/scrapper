import { useEffect, useState, useCallback } from "react";
import {
  View,
  Text,
  FlatList,
  TouchableOpacity,
  StyleSheet,
  Alert,
  StatusBar,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { t, isDark } from "../src/lib/theme";
import { getMangaDetail, deleteManga as deleteMangaLocal, deleteChapter as deleteChapterLocal, MangaDetail } from "../src/lib/manga";

interface Chapter {
  number: number;
  file: string;
}

export default function MangaScreen() {
  const { slug } = useLocalSearchParams<{ slug: string }>();
  const router = useRouter();
  const [detail, setDetail] = useState<MangaDetail | null>(null);
  const colors = t();

  const load = useCallback(async () => {
    if (!slug) return;
    const data = await getMangaDetail(slug);
    setDetail(data);
  }, [slug]);

  useEffect(() => { load(); }, [load]);

  const deleteManga = () => {
    if (!detail) return;
    Alert.alert("Delete", `Delete all chapters of ${detail.title}?`, [
      { text: "Cancel", style: "cancel" },
      {
        text: "Delete",
        style: "destructive",
        onPress: async () => {
          await deleteMangaLocal(slug!);
          router.back();
        },
      },
    ]);
  };

  const deleteChapter = (num: number) => {
    Alert.alert("Delete", `Delete chapter ${num}?`, [
      { text: "Cancel", style: "cancel" },
      {
        text: "Delete",
        style: "destructive",
        onPress: async () => {
          await deleteChapterLocal(slug!, num);
          load();
        },
      },
    ]);
  };

  const chapters = detail?.chapters || [];
  const title = detail?.title || slug || "";
  const lastChapter = detail?.progress?.last_chapter as number | null;

  return (
    <View style={[s.container, { backgroundColor: colors.bg }]}>
      <StatusBar barStyle={isDark() ? "light-content" : "dark-content"} />

      <View style={[s.header, { backgroundColor: colors.bg, borderBottomColor: colors.border }]}>
        <TouchableOpacity onPress={() => router.back()} style={s.backBtn}>
          <Text style={[s.backText, { color: colors.accent }]}>← Back</Text>
        </TouchableOpacity>
        <Text style={[s.title, { color: colors.fg }]} numberOfLines={2}>{title}</Text>
        <Text style={[s.meta, { color: colors.fg3 }]}>
          {chapters.length} chapters
        </Text>

        {lastChapter != null && (
          <TouchableOpacity
            style={[s.continueBtn, { backgroundColor: colors.accent }]}
            onPress={() => router.push({ pathname: "/reader", params: { slug: slug!, chapter: lastChapter } })}
            activeOpacity={0.8}
          >
            <Text style={s.continueText}>Continue Chapter {lastChapter}</Text>
          </TouchableOpacity>
        )}

        <TouchableOpacity style={[s.deleteBtn, { borderColor: colors.danger }]} onPress={deleteManga}>
          <Text style={[s.deleteText, { color: colors.danger }]}>Delete Manga</Text>
        </TouchableOpacity>
      </View>

      <FlatList
        data={[...chapters].reverse()}
        keyExtractor={(item) => item.number.toString()}
        contentContainerStyle={s.list}
        renderItem={({ item }) => (
          <TouchableOpacity
            style={[s.chapterRow, { backgroundColor: colors.bg2, borderColor: colors.border }]}
            onPress={() => router.push({ pathname: "/reader", params: { slug: slug!, chapter: item.number } })}
            onLongPress={() => deleteChapter(item.number)}
            activeOpacity={0.7}
          >
            <View style={[s.chNum, { backgroundColor: colors.bg3 }]}>
              <Text style={[s.chNumText, { color: colors.accent }]}>{item.number}</Text>
            </View>
            <Text style={[s.chName, { color: colors.fg }]}>Chapter {item.number}</Text>
            <TouchableOpacity onPress={() => deleteChapter(item.number)} hitSlop={{ top: 10, bottom: 10, left: 10, right: 10 }}>
              <Text style={[s.chDelete, { color: colors.fg3 }]}>×</Text>
            </TouchableOpacity>
          </TouchableOpacity>
        )}
      />
    </View>
  );
}

const s = StyleSheet.create({
  container: { flex: 1 },
  header: { paddingTop: 56, paddingHorizontal: 20, paddingBottom: 16, borderBottomWidth: 1 },
  backBtn: { marginBottom: 12 },
  backText: { fontSize: 15, fontWeight: "600" },
  title: { fontSize: 26, fontWeight: "900", letterSpacing: -0.5, marginBottom: 4 },
  meta: { fontSize: 13, marginBottom: 14 },
  continueBtn: { paddingVertical: 14, borderRadius: 12, alignItems: "center", marginBottom: 10 },
  continueText: { color: "#0a0a0c", fontWeight: "800", fontSize: 15 },
  deleteBtn: { paddingVertical: 10, borderRadius: 10, borderWidth: 1, alignItems: "center" },
  deleteText: { fontWeight: "600", fontSize: 13 },
  list: { padding: 20 },
  chapterRow: {
    flexDirection: "row",
    alignItems: "center",
    padding: 12,
    borderRadius: 10,
    marginBottom: 6,
    borderWidth: 1,
  },
  chNum: { width: 36, height: 36, borderRadius: 8, justifyContent: "center", alignItems: "center", marginRight: 12 },
  chNumText: { fontSize: 14, fontWeight: "800" },
  chName: { flex: 1, fontSize: 15, fontWeight: "500" },
  chDelete: { fontSize: 20, fontWeight: "300", paddingHorizontal: 8 },
});
