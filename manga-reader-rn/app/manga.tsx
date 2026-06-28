import { useEffect, useState, useCallback } from "react";
import {
  View,
  Text,
  FlatList,
  TouchableOpacity,
  StyleSheet,
  Alert,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { api } from "../src/lib/api";
import { t } from "../src/lib/theme";

interface Chapter {
  number: number;
  file: string;
}

export default function MangaScreen() {
  const { slug } = useLocalSearchParams<{ slug: string }>();
  const router = useRouter();
  const [chapters, setChapters] = useState<Chapter[]>([]);
  const [title, setTitle] = useState("");
  const [lastChapter, setLastChapter] = useState<number | null>(null);
  const colors = t();

  const load = useCallback(async () => {
    const data = await api(`/api/manga/${encodeURIComponent(slug!)}`);
    if (data) {
      setChapters(data.chapters || []);
      setTitle(data.title || slug);
      setLastChapter(data.progress?.last_chapter ?? null);
    }
  }, [slug]);

  useEffect(() => {
    load();
  }, [load]);

  const deleteManga = () => {
    Alert.alert("Delete Manga", `Delete all chapters of ${title}?`, [
      { text: "Cancel", style: "cancel" },
      {
        text: "Delete",
        style: "destructive",
        onPress: async () => {
          await api(`/api/manga/${encodeURIComponent(slug!)}/delete`, { method: "DELETE" });
          router.back();
        },
      },
    ]);
  };

  const deleteChapter = (num: number) => {
    Alert.alert("Delete Chapter", `Delete chapter ${num}?`, [
      { text: "Cancel", style: "cancel" },
      {
        text: "Delete",
        style: "destructive",
        onPress: async () => {
          await api(`/api/manga/${encodeURIComponent(slug!)}/chapter/${num}/delete`, {
            method: "DELETE",
          });
          load();
        },
      },
    ]);
  };

  return (
    <View style={[styles.container, { backgroundColor: colors.bg }]}>
      {/* Header */}
      <View style={[styles.header, { backgroundColor: colors.bg2, borderBottomColor: colors.border }]}>
        <TouchableOpacity onPress={() => router.back()} style={styles.backBtn}>
          <Text style={[styles.backText, { color: colors.accent }]}>Back</Text>
        </TouchableOpacity>
        <Text style={[styles.title, { color: colors.fg }]} numberOfLines={1}>
          {title}
        </Text>
        <Text style={[styles.meta, { color: colors.fg2 }]}>
          {chapters.length} chapters
        </Text>
        {lastChapter != null && (
          <TouchableOpacity
            style={[styles.continueBtn, { backgroundColor: colors.accent }]}
            onPress={() =>
              router.push({ pathname: "/reader", params: { slug: slug!, chapter: lastChapter } })
            }
          >
            <Text style={styles.continueText}>Continue Ch {lastChapter}</Text>
          </TouchableOpacity>
        )}
        <TouchableOpacity style={[styles.deleteBtn, { borderColor: colors.danger }]} onPress={deleteManga}>
          <Text style={[styles.deleteText, { color: colors.danger }]}>Delete Manga</Text>
        </TouchableOpacity>
      </View>

      {/* Chapters */}
      <FlatList
        data={[...chapters].reverse()}
        keyExtractor={(item) => item.number.toString()}
        contentContainerStyle={styles.list}
        renderItem={({ item }) => (
          <TouchableOpacity
            style={[styles.chapterRow, { backgroundColor: colors.bg2, borderColor: colors.border }]}
            onPress={() =>
              router.push({ pathname: "/reader", params: { slug: slug!, chapter: item.number } })
            }
            onLongPress={() => deleteChapter(item.number)}
          >
            <Text style={[styles.chapterNum, { color: colors.accent }]}>
              {item.number}
            </Text>
            <Text style={[styles.chapterName, { color: colors.fg }]} numberOfLines={1}>
              Chapter {item.number}
            </Text>
            <TouchableOpacity onPress={() => deleteChapter(item.number)}>
              <Text style={[styles.chapterDelete, { color: colors.danger }]}>X</Text>
            </TouchableOpacity>
          </TouchableOpacity>
        )}
      />
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  header: { paddingTop: 60, paddingHorizontal: 20, paddingBottom: 16, borderBottomWidth: 1 },
  backBtn: { marginBottom: 10 },
  backText: { fontSize: 16, fontWeight: "600" },
  title: { fontSize: 24, fontWeight: "800", marginBottom: 4 },
  meta: { fontSize: 14, marginBottom: 12 },
  continueBtn: { paddingVertical: 12, borderRadius: 10, alignItems: "center", marginBottom: 10 },
  continueText: { color: "#fff", fontWeight: "700", fontSize: 15 },
  deleteBtn: { paddingVertical: 10, borderRadius: 10, borderWidth: 1, alignItems: "center" },
  deleteText: { fontWeight: "600", fontSize: 14 },
  list: { padding: 20 },
  chapterRow: {
    flexDirection: "row",
    alignItems: "center",
    padding: 14,
    borderRadius: 10,
    marginBottom: 8,
    borderWidth: 1,
  },
  chapterNum: { fontSize: 18, fontWeight: "800", width: 50 },
  chapterName: { flex: 1, fontSize: 15 },
  chapterDelete: { fontSize: 16, fontWeight: "700", paddingHorizontal: 8 },
});
