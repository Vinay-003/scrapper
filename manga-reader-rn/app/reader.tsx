import { useEffect, useState } from "react";
import {
  View,
  Text,
  ScrollView,
  Image,
  TouchableOpacity,
  StyleSheet,
  Dimensions,
  ActivityIndicator,
  StatusBar,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { api, getApiBase, encodeSlug } from "../src/lib/api";
import { t, isDark } from "../src/lib/theme";
import { saveRecentlyRead } from "../src/lib/storage";

const SCREEN = Dimensions.get("window");

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const [images, setImages] = useState<{ uri: string; w: number; h: number }[]>([]);
  const [loading, setLoading] = useState(true);
  const [zoom, setZoom] = useState(1);
  const [progress, setProgress] = useState(0);
  const [showUI, setShowUI] = useState(true);
  const colors = t();
  const chapterNum = parseFloat(chapter!);
  const safeSlug = encodeSlug(slug || "");

  useEffect(() => {
    (async () => {
      setLoading(true);
      setImages([]);
      const data = await api(`/api/manga/${safeSlug}/chapter/${chapterNum}`);
      if (data?.images) {
        const base = await getApiBase();
        const loaded = await Promise.all(
          data.images.map(async (name: string) => {
            const uri = `${base}/api/manga/${safeSlug}/chapter/${chapterNum}/image/${encodeURIComponent(name)}`;
            try {
              const dims = await new Promise<{ width: number; height: number }>((resolve) => {
                Image.getSize(uri, (w, h) => resolve({ width: w, height: h }), () => resolve({ width: 600, height: 900 }));
              });
              return { uri, w: dims.width, h: dims.height };
            } catch {
              return { uri, w: 600, h: 900 };
            }
          })
        );
        setImages(loaded);
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [safeSlug, slug, chapterNum]);

  const navigateChapter = (delta: number) => {
    router.replace({ pathname: "/reader", params: { slug: slug!, chapter: chapterNum + delta } });
  };

  const imgWidth = SCREEN.width * zoom;

  if (loading) {
    return (
      <View style={[s.loadingContainer, { backgroundColor: colors.bg }]}>
        <ActivityIndicator size="large" color={colors.accent} />
        <Text style={[s.loadingText, { color: colors.fg3 }]}>Loading...</Text>
      </View>
    );
  }

  return (
    <View style={[s.container, { backgroundColor: colors.bg }]}>
      <StatusBar barStyle={isDark() ? "light-content" : "dark-content"} hidden={!showUI} />

      {showUI && (
        <View style={[s.topBar, { backgroundColor: colors.bg, borderBottomColor: colors.border }]}>
          <TouchableOpacity onPress={() => router.back()} style={s.barBtn}>
            <Text style={[s.barBtnText, { color: colors.accent }]}>←</Text>
          </TouchableOpacity>
          <Text style={[s.barTitle, { color: colors.fg }]} numberOfLines={1}>
            Ch {chapterNum}
          </Text>
          <View style={s.zoomRow}>
            <TouchableOpacity
              style={[s.zoomBtn, { backgroundColor: colors.bg3 }]}
              onPress={() => setZoom((z) => Math.max(0.5, z - 0.15))}
            >
              <Text style={[s.zoomText, { color: colors.fg }]}>−</Text>
            </TouchableOpacity>
            <Text style={[s.zoomLabel, { color: colors.fg3 }]}>{Math.round(zoom * 100)}%</Text>
            <TouchableOpacity
              style={[s.zoomBtn, { backgroundColor: colors.bg3 }]}
              onPress={() => setZoom((z) => Math.min(3, z + 0.15))}
            >
              <Text style={[s.zoomText, { color: colors.fg }]}>+</Text>
            </TouchableOpacity>
          </View>
        </View>
      )}

      <ScrollView
        style={s.scroll}
        onScroll={(e) => {
          const { contentOffset, contentSize, layoutMeasurement } = e.nativeEvent;
          if (contentSize.height > layoutMeasurement.height) {
            setProgress(
              Math.round((contentOffset.y / (contentSize.height - layoutMeasurement.height)) * 100)
            );
          }
        }}
        scrollEventThrottle={16}
      >
        <TouchableOpacity
          activeOpacity={1}
          onPress={() => setShowUI((v) => !v)}
          style={{ alignItems: "center", paddingVertical: 4 }}
        >
          {images.map((img, i) => {
            const aspectRatio = img.w / img.h;
            const imgH = imgWidth / aspectRatio;
            return (
              <Image
                key={`${chapterNum}-${i}`}
                source={{ uri: img.uri }}
                style={{
                  width: imgWidth,
                  height: imgH,
                  marginBottom: 2,
                }}
                resizeMode="contain"
              />
            );
          })}
        </TouchableOpacity>
      </ScrollView>

      {showUI && (
        <View style={[s.bottomBar, { backgroundColor: colors.bg, borderTopColor: colors.border }]}>
          <TouchableOpacity
            style={[s.navBtn, { backgroundColor: colors.bg3 }]}
            onPress={() => navigateChapter(-1)}
          >
            <Text style={[s.navBtnText, { color: colors.fg }]}>← Prev</Text>
          </TouchableOpacity>
          <Text style={[s.progressText, { color: colors.fg3 }]}>{progress}%</Text>
          <TouchableOpacity
            style={[s.navBtn, { backgroundColor: colors.bg3 }]}
            onPress={() => navigateChapter(1)}
          >
            <Text style={[s.navBtnText, { color: colors.fg }]}>Next →</Text>
          </TouchableOpacity>
        </View>
      )}
    </View>
  );
}

const s = StyleSheet.create({
  container: { flex: 1 },
  loadingContainer: { flex: 1, justifyContent: "center", alignItems: "center" },
  loadingText: { marginTop: 12, fontSize: 14, fontWeight: "500" },
  scroll: { flex: 1 },
  topBar: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingTop: 50,
    paddingHorizontal: 16,
    paddingBottom: 10,
    borderBottomWidth: 1,
  },
  barBtn: { padding: 4 },
  barBtnText: { fontSize: 20, fontWeight: "700" },
  barTitle: { fontSize: 15, fontWeight: "700", flex: 1, textAlign: "center" },
  zoomRow: { flexDirection: "row", alignItems: "center", gap: 6 },
  zoomBtn: { width: 34, height: 34, borderRadius: 10, justifyContent: "center", alignItems: "center" },
  zoomText: { fontSize: 18, fontWeight: "700" },
  zoomLabel: { fontSize: 12, minWidth: 38, textAlign: "center" },
  bottomBar: {
    flexDirection: "row",
    alignItems: "center",
    justifyContent: "space-between",
    paddingHorizontal: 16,
    paddingVertical: 10,
    borderTopWidth: 1,
  },
  navBtn: { paddingHorizontal: 18, paddingVertical: 10, borderRadius: 10 },
  navBtnText: { fontWeight: "700", fontSize: 14 },
  progressText: { fontSize: 13 },
});
