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
  StatusBar,
} from "react-native";
import { Gesture, GestureDetector } from "react-native-gesture-handler";
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withTiming,
} from "react-native-reanimated";
import { useLocalSearchParams, useRouter } from "expo-router";
import { api, getApiBase, encodeSlug } from "../src/lib/api";
import { t, isDark } from "../src/lib/theme";
import { saveRecentlyRead } from "../src/lib/storage";

const SCREEN = Dimensions.get("window");
const MANGA_ASPECT = 2 / 3;

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const [imageUris, setImageUris] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [progress, setProgress] = useState(0);
  const [showUI, setShowUI] = useState(true);
  const [baseZoom, setBaseZoom] = useState(1);
  const colors = t();
  const chapterNum = parseFloat(chapter!);
  const safeSlug = encodeSlug(slug || "");

  const scale = useSharedValue(1);
  const savedScale = useSharedValue(1);

  const pinch = Gesture.Pinch()
    .onUpdate((e) => {
      scale.value = Math.max(0.5, Math.min(4, savedScale.value * e.scale));
    })
    .onEnd(() => {
      savedScale.value = scale.value;
    });

  const animatedStyle = useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
  }));

  useEffect(() => {
    (async () => {
      setLoading(true);
      setImageUris([]);
      scale.value = 1;
      savedScale.value = 1;
      setBaseZoom(1);
      const data = await api(`/api/manga/${safeSlug}/chapter/${chapterNum}`);
      if (data?.images) {
        const base = await getApiBase();
        const uris = data.images.map(
          (name: string) =>
            `${base}/api/manga/${safeSlug}/chapter/${chapterNum}/image/${encodeURIComponent(name)}`
        );
        setImageUris(uris);
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [safeSlug, slug, chapterNum]);

  const navigateChapter = (delta: number) => {
    router.replace({
      pathname: "/reader",
      params: { slug: slug!, chapter: chapterNum + delta },
    });
  };

  const zoomIn = () => setBaseZoom((z) => Math.min(3, z + 0.25));
  const zoomOut = () => setBaseZoom((z) => Math.max(0.5, z - 0.25));
  const resetZoom = () => {
    setBaseZoom(1);
    scale.value = withTiming(1);
    savedScale.value = 1;
  };

  const imgWidth = SCREEN.width * baseZoom;

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
      <StatusBar
        barStyle={isDark() ? "light-content" : "dark-content"}
        hidden={!showUI}
      />

      {showUI && (
        <View
          style={[
            s.topBar,
            { backgroundColor: colors.bg, borderBottomColor: colors.border },
          ]}
        >
          <TouchableOpacity onPress={() => router.back()} style={s.barBtn}>
            <Text style={[s.barBtnText, { color: colors.accent }]}>←</Text>
          </TouchableOpacity>
          <Text style={[s.barTitle, { color: colors.fg }]} numberOfLines={1}>
            Ch {chapterNum}
          </Text>
          <View style={s.zoomRow}>
            <TouchableOpacity
              style={[s.zoomBtn, { backgroundColor: colors.bg3 }]}
              onPress={zoomOut}
            >
              <Text style={[s.zoomText, { color: colors.fg }]}>−</Text>
            </TouchableOpacity>
            <TouchableOpacity onPress={resetZoom}>
              <Text style={[s.zoomLabel, { color: colors.fg3 }]}>
                {Math.round(baseZoom * 100)}%
              </Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[s.zoomBtn, { backgroundColor: colors.bg3 }]}
              onPress={zoomIn}
            >
              <Text style={[s.zoomText, { color: colors.fg }]}>+</Text>
            </TouchableOpacity>
          </View>
        </View>
      )}

      <GestureDetector gesture={pinch}>
        <Animated.View style={[{ flex: 1 }, animatedStyle]}>
          <ScrollView
            style={s.scroll}
            onScroll={(e) => {
              const { contentOffset, contentSize, layoutMeasurement } =
                e.nativeEvent;
              if (contentSize.height > layoutMeasurement.height) {
                setProgress(
                  Math.round(
                    (contentOffset.y /
                      (contentSize.height - layoutMeasurement.height)) *
                      100
                  )
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
              {imageUris.map((uri, i) => (
                <Image
                  key={`${chapterNum}-${i}`}
                  source={{ uri }}
                  style={{
                    width: imgWidth,
                    height: imgWidth * MANGA_ASPECT,
                    marginBottom: 2,
                  }}
                  resizeMode="contain"
                />
              ))}
            </TouchableOpacity>
          </ScrollView>
        </Animated.View>
      </GestureDetector>

      {showUI && (
        <View
          style={[
            s.bottomBar,
            { backgroundColor: colors.bg, borderTopColor: colors.border },
          ]}
        >
          <TouchableOpacity
            style={[s.navBtn, { backgroundColor: colors.bg3 }]}
            onPress={() => navigateChapter(-1)}
          >
            <Text style={[s.navBtnText, { color: colors.fg }]}>← Prev</Text>
          </TouchableOpacity>
          <Text style={[s.progressText, { color: colors.fg3 }]}>
            {progress}%
          </Text>
          <TouchableOpacity
            style={[s.navBtn, { backgroundColor: colors.bg3 }]}
            onPress={() => navigateChapter(1)}
          >
            <Text style={[s.navBtnText, { color: colors.fg }]}>
              Next →
            </Text>
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
  zoomBtn: {
    width: 34,
    height: 34,
    borderRadius: 10,
    justifyContent: "center",
    alignItems: "center",
  },
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
