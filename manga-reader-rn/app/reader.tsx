import { useEffect, useState, useRef, useCallback } from "react";
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
  GestureResponderEvent,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { api, getApiBase, encodeSlug } from "../src/lib/api";
import { t, isDark } from "../src/lib/theme";
import { saveRecentlyRead } from "../src/lib/storage";

const SCREEN = Dimensions.get("window");
const MANGA_ASPECT = 2 / 3;

function getDistance(t1: { pageX: number; pageY: number }, t2: { pageX: number; pageY: number }) {
  return Math.sqrt((t1.pageX - t2.pageX) ** 2 + (t1.pageY - t2.pageY) ** 2);
}

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const [imageUris, setImageUris] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [progress, setProgress] = useState(0);
  const [showUI, setShowUI] = useState(true);
  const [btnZoom, setBtnZoom] = useState(1);
  const colors = t();
  const chapterNum = parseFloat(chapter!);
  const safeSlug = encodeSlug(slug || "");

  const pinchRef = useRef({ initialDist: 0, baseScale: 1, active: false });
  const [pinchScale, setPinchScale] = useState(1);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setImageUris([]);
      setPinchScale(1);
      setBtnZoom(1);
      pinchRef.current = { initialDist: 0, baseScale: 1, active: false };
      const data = await api(`/api/manga/${safeSlug}/chapter/${chapterNum}`);
      if (data?.images) {
        const base = await getApiBase();
        setImageUris(
          data.images.map((name: string) =>
            `${base}/api/manga/${safeSlug}/chapter/${chapterNum}/image/${encodeURIComponent(name)}`
          )
        );
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [safeSlug, slug, chapterNum]);

  const navigateChapter = (delta: number) => {
    router.replace({ pathname: "/reader", params: { slug: slug!, chapter: chapterNum + delta } });
  };

  const onTouchStart = useCallback((e: GestureResponderEvent) => {
    const touches = e.nativeEvent.touches;
    if (touches.length === 2) {
      const dist = getDistance(touches[0], touches[1]);
      pinchRef.current = { initialDist: dist, baseScale: pinchRef.current.baseScale || 1, active: true };
      pinchRef.current.baseScale = pinchScale;
    }
  }, [pinchScale]);

  const onTouchMove = useCallback((e: GestureResponderEvent) => {
    const touches = e.nativeEvent.touches;
    if (touches.length === 2 && pinchRef.current.active && pinchRef.current.initialDist > 0) {
      const dist = getDistance(touches[0], touches[1]);
      const newScale = pinchRef.current.baseScale * (dist / pinchRef.current.initialDist);
      setPinchScale(Math.max(0.5, Math.min(4, newScale)));
    }
  }, []);

  const onTouchEnd = useCallback(() => {
    pinchRef.current.active = false;
  }, []);

  const zoomIn = () => setBtnZoom((z) => Math.min(3, z + 0.25));
  const zoomOut = () => setBtnZoom((z) => Math.max(0.5, z - 0.25));
  const resetZoom = () => {
    setBtnZoom(1);
    setPinchScale(1);
    pinchRef.current = { initialDist: 0, baseScale: 1, active: false };
  };

  const imgWidth = SCREEN.width * btnZoom * pinchScale;

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
            <TouchableOpacity style={[s.zoomBtn, { backgroundColor: colors.bg3 }]} onPress={zoomOut}>
              <Text style={[s.zoomText, { color: colors.fg }]}>−</Text>
            </TouchableOpacity>
            <TouchableOpacity onPress={resetZoom}>
              <Text style={[s.zoomLabel, { color: colors.fg3 }]}>
                {Math.round(btnZoom * pinchScale * 100)}%
              </Text>
            </TouchableOpacity>
            <TouchableOpacity style={[s.zoomBtn, { backgroundColor: colors.bg3 }]} onPress={zoomIn}>
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
            setProgress(Math.round((contentOffset.y / (contentSize.height - layoutMeasurement.height)) * 100));
          }
        }}
        scrollEventThrottle={16}
        onTouchStart={onTouchStart}
        onTouchMove={onTouchMove}
        onTouchEnd={onTouchEnd}
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
              style={{ width: imgWidth, height: imgWidth * MANGA_ASPECT, marginBottom: 2 }}
              resizeMode="contain"
            />
          ))}
        </TouchableOpacity>
      </ScrollView>

      {showUI && (
        <View style={[s.bottomBar, { backgroundColor: colors.bg, borderTopColor: colors.border }]}>
          <TouchableOpacity style={[s.navBtn, { backgroundColor: colors.bg3 }]} onPress={() => navigateChapter(-1)}>
            <Text style={[s.navBtnText, { color: colors.fg }]}>← Prev</Text>
          </TouchableOpacity>
          <Text style={[s.progressText, { color: colors.fg3 }]}>{progress}%</Text>
          <TouchableOpacity style={[s.navBtn, { backgroundColor: colors.bg3 }]} onPress={() => navigateChapter(1)}>
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
    flexDirection: "row", alignItems: "center", justifyContent: "space-between",
    paddingTop: 50, paddingHorizontal: 16, paddingBottom: 10, borderBottomWidth: 1,
  },
  barBtn: { padding: 4 },
  barBtnText: { fontSize: 20, fontWeight: "700" },
  barTitle: { fontSize: 15, fontWeight: "700", flex: 1, textAlign: "center" },
  zoomRow: { flexDirection: "row", alignItems: "center", gap: 6 },
  zoomBtn: { width: 34, height: 34, borderRadius: 10, justifyContent: "center", alignItems: "center" },
  zoomText: { fontSize: 18, fontWeight: "700" },
  zoomLabel: { fontSize: 12, minWidth: 38, textAlign: "center" },
  bottomBar: {
    flexDirection: "row", alignItems: "center", justifyContent: "space-between",
    paddingHorizontal: 16, paddingVertical: 10, borderTopWidth: 1,
  },
  navBtn: { paddingHorizontal: 18, paddingVertical: 10, borderRadius: 10 },
  navBtnText: { fontWeight: "700", fontSize: 14 },
  progressText: { fontSize: 13 },
});
