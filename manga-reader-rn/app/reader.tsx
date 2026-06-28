import { useEffect, useState, useRef } from "react";
import {
  View,
  Text,
  ScrollView,
  Image,
  TouchableOpacity,
  StyleSheet,
  Dimensions,
  ActivityIndicator,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { api } from "../src/lib/api";
import { getApiBase } from "../src/lib/api";
import { t } from "../src/lib/theme";
import { saveRecentlyRead } from "../src/lib/storage";

const { width: SCREEN_WIDTH } = Dimensions.get("window");

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const scrollRef = useRef<ScrollView>(null);
  const [images, setImages] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [zoom, setZoom] = useState(1);
  const [progress, setProgress] = useState(0);
  const colors = t();
  const chapterNum = parseFloat(chapter!);

  useEffect(() => {
    (async () => {
      setLoading(true);
      const data = await api(
        `/api/manga/${encodeURIComponent(slug!)}/chapter/${chapterNum}`
      );
      if (data?.images) {
        const base = await getApiBase();
        const urls = data.images.map(
          (name: string) =>
            `${base}/api/manga/${encodeURIComponent(slug!)}/chapter/${chapterNum}/image/${encodeURIComponent(name)}`
        );
        setImages(urls);
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [slug, chapterNum]);

  const onScroll = (e: any) => {
    const { contentOffset, contentSize, layoutMeasurement } = e.nativeEvent;
    if (contentSize.height > 0) {
      setProgress(
        Math.round((contentOffset.y / (contentSize.height - layoutMeasurement.height)) * 100)
      );
    }
  };

  const navigateChapter = (delta: number) => {
    router.replace({ pathname: "/reader", params: { slug: slug!, chapter: chapterNum + delta } });
  };

  if (loading) {
    return (
      <View style={[styles.loadingContainer, { backgroundColor: colors.bg }]}>
        <ActivityIndicator size="large" color={colors.accent} />
        <Text style={[styles.loadingText, { color: colors.fg2 }]}>Loading chapter...</Text>
      </View>
    );
  }

  return (
    <View style={[styles.container, { backgroundColor: colors.bg }]}>
      {/* Top Bar */}
      <View style={[styles.topBar, { backgroundColor: colors.bg2, borderBottomColor: colors.border }]}>
        <TouchableOpacity onPress={() => router.back()} style={styles.barBtn}>
          <Text style={[styles.barBtnText, { color: colors.accent }]}>Back</Text>
        </TouchableOpacity>
        <Text style={[styles.barTitle, { color: colors.fg }]} numberOfLines={1}>
          Ch {chapterNum}
        </Text>
        <View style={styles.zoomRow}>
          <TouchableOpacity
            style={[styles.zoomBtn, { backgroundColor: colors.bg3 }]}
            onPress={() => setZoom(Math.max(0.5, zoom - 0.1))}
          >
            <Text style={[styles.zoomBtnText, { color: colors.fg }]}>-</Text>
          </TouchableOpacity>
          <Text style={[styles.zoomLabel, { color: colors.fg2 }]}>
            {Math.round(zoom * 100)}%
          </Text>
          <TouchableOpacity
            style={[styles.zoomBtn, { backgroundColor: colors.bg3 }]}
            onPress={() => setZoom(Math.min(3, zoom + 0.1))}
          >
            <Text style={[styles.zoomBtnText, { color: colors.fg }]}>+</Text>
          </TouchableOpacity>
        </View>
      </View>

      {/* Images */}
      <ScrollView
        ref={scrollRef}
        style={styles.scroll}
        onScroll={onScroll}
        scrollEventThrottle={16}
      >
        {images.map((uri, i) => (
          <Image
            key={i}
            source={{ uri }}
            style={[styles.image, { width: SCREEN_WIDTH * zoom }]}
            resizeMode="contain"
          />
        ))}
      </ScrollView>

      {/* Bottom Bar */}
      <View style={[styles.bottomBar, { backgroundColor: colors.bg2, borderTopColor: colors.border }]}>
        <TouchableOpacity
          style={[styles.navBtn, { backgroundColor: colors.bg3 }]}
          onPress={() => navigateChapter(-1)}
        >
          <Text style={[styles.navBtnText, { color: colors.fg }]}>Prev</Text>
        </TouchableOpacity>
        <Text style={[styles.progressText, { color: colors.fg2 }]}>{progress}%</Text>
        <TouchableOpacity
          style={[styles.navBtn, { backgroundColor: colors.bg3 }]}
          onPress={() => navigateChapter(1)}
        >
          <Text style={[styles.navBtnText, { color: colors.fg }]}>Next</Text>
        </TouchableOpacity>
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  loadingContainer: { flex: 1, justifyContent: "center", alignItems: "center" },
  loadingText: { marginTop: 12, fontSize: 15 },
  topBar: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingTop: 50,
    paddingHorizontal: 16,
    paddingBottom: 12,
    borderBottomWidth: 1,
  },
  barBtn: { padding: 4 },
  barBtnText: { fontSize: 16, fontWeight: "600" },
  barTitle: { fontSize: 16, fontWeight: "700", flex: 1, textAlign: "center" },
  zoomRow: { flexDirection: "row", alignItems: "center", gap: 6 },
  zoomBtn: { width: 32, height: 32, borderRadius: 8, justifyContent: "center", alignItems: "center" },
  zoomBtnText: { fontSize: 18, fontWeight: "700" },
  zoomLabel: { fontSize: 12, minWidth: 36, textAlign: "center" },
  scroll: { flex: 1 },
  image: { alignSelf: "center", minHeight: 400 },
  bottomBar: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
    paddingVertical: 12,
    borderTopWidth: 1,
  },
  navBtn: { paddingHorizontal: 20, paddingVertical: 10, borderRadius: 10 },
  navBtnText: { fontWeight: "600", fontSize: 14 },
  progressText: { fontSize: 14 },
});
