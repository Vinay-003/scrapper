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

function getDistance(t1: { pageX: number; pageY: number }, t2: { pageX: number; pageY: number }) {
  return Math.sqrt((t1.pageX - t2.pageX) ** 2 + (t1.pageY - t2.pageY) ** 2);
}

function MangaImage({ uri, width, dims }: { uri: string; width: number; dims?: { w: number; h: number } }) {
  const height = dims ? (width * dims.h) / dims.w : width * 1.5;
  return (
    <Image
      source={{ uri, cache: "reload" }}
      style={{ width, height }}
      resizeMode="contain"
    />
  );
}

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const [imageUris, setImageUris] = useState<string[]>([]);
  const [imageNames, setImageNames] = useState<string[]>([]);
  const [imageSizes, setImageSizes] = useState<Record<string, { w: number; h: number }>>({});
  const [loading, setLoading] = useState(true);
  const [progress, setProgress] = useState(0);
  const [showUI, setShowUI] = useState(true);
  const [zoom, setZoom] = useState(1);
  const [panX, setPanX] = useState(0);
  const [panY, setPanY] = useState(0);
  const colors = t();
  const chapterNum = parseFloat(chapter!);
  const safeSlug = encodeSlug(slug || "");

  const scrollYRef = useRef(0);
  const gestureRef = useRef({
    initialDist: 0,
    baseZoom: 1,
    basePanX: 0,
    basePanY: 0,
    panStartX: 0,
    panStartY: 0,
    mode: "none" as "none" | "pinch" | "pan",
    touchStartX: 0,
    touchStartY: 0,
  });

  useEffect(() => {
    (async () => {
      setLoading(true);
      setImageUris([]);
      setImageNames([]);
      setImageSizes({});
      setZoom(1);
      setPanX(0);
      setPanY(0);
      scrollYRef.current = 0;
      gestureRef.current.mode = "none";
      const data = await api(`/api/manga/${safeSlug}/chapter/${chapterNum}`);
      if (data?.images) {
        const base = await getApiBase();
        const ts = Date.now();
        setImageNames(data.images);
        setImageUris(
          data.images.map((name: string) =>
            `${base}/api/manga/${safeSlug}/chapter/${chapterNum}/image/${encodeURIComponent(name)}?_v=${ts}`
          )
        );
        if (data.sizes) {
          setImageSizes(data.sizes);
        }
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [safeSlug, slug, chapterNum]);

  const navigateChapter = (delta: number) => {
    router.replace({ pathname: "/reader", params: { slug: slug!, chapter: chapterNum + delta } });
  };

  const clampZoom = (z: number) => Math.max(0.5, Math.min(5, z));

  const onTouchStart = useCallback((e: GestureResponderEvent) => {
    const t = e.nativeEvent.touches;
    if (!t) return;
    gestureRef.current.touchStartX = t[0]?.pageX || 0;
    gestureRef.current.touchStartY = t[0]?.pageY || 0;

    if (t.length === 2) {
      gestureRef.current = {
        ...gestureRef.current,
        initialDist: getDistance(t[0], t[1]),
        baseZoom: zoom,
        basePanX: panX,
        basePanY: panY,
        mode: "pinch",
      };
    } else if (t.length === 1 && zoom > 1) {
      gestureRef.current = {
        ...gestureRef.current,
        panStartX: t[0].pageX,
        panStartY: t[0].pageY,
        basePanX: panX,
        basePanY: panY,
        mode: "pan",
      };
    }
  }, [zoom, panX, panY]);

  const onTouchMove = useCallback((e: GestureResponderEvent) => {
    const t = e.nativeEvent.touches;
    if (!t) return;

    if (t.length === 2 && gestureRef.current.mode === "pinch") {
      const dist = getDistance(t[0], t[1]);
      const ratio = dist / gestureRef.current.initialDist;
      const newZoom = clampZoom(gestureRef.current.baseZoom * ratio);
      const scale = newZoom / gestureRef.current.baseZoom;

      const fx = (t[0].pageX + t[1].pageX) / 2;
      const fy = (t[0].pageY + t[1].pageY) / 2;

      const newPanX = fx * (1 - scale) + gestureRef.current.basePanX * scale;
      const newPanY = (fy + scrollYRef.current) * (1 - scale) + gestureRef.current.basePanY * scale;

      setZoom(newZoom);
      setPanX(newPanX);
      setPanY(newPanY);
    } else if (t.length === 1 && gestureRef.current.mode === "pan") {
      const dx = t[0].pageX - gestureRef.current.panStartX;
      const dy = t[0].pageY - gestureRef.current.panStartY;
      setPanX(gestureRef.current.basePanX + dx);
      setPanY(gestureRef.current.basePanY + dy);
    }
  }, []);

  const onTouchEnd = useCallback((e: GestureResponderEvent) => {
    if (gestureRef.current.mode === "none" && zoom <= 1) {
      const dx = Math.abs(e.nativeEvent.pageX - gestureRef.current.touchStartX);
      const dy = Math.abs(e.nativeEvent.pageY - gestureRef.current.touchStartY);
      if (dx < 10 && dy < 10) {
        setShowUI((v) => !v);
      }
    }
    setTimeout(() => { gestureRef.current.mode = "none"; }, 50);
  }, [zoom]);

  const zoomIn = () => setZoom((z) => clampZoom(z + 0.5));
  const zoomOut = () => {
    setZoom((z) => {
      const nz = clampZoom(z - 0.5);
      if (nz <= 1) {
        setPanX(0);
        setPanY(0);
      }
      return nz;
    });
  };
  const resetZoom = () => {
    setZoom(1);
    setPanX(0);
    setPanY(0);
    gestureRef.current.mode = "none";
  };

  const scrollEnabled = zoom <= 1;

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
                {Math.round(zoom * 100)}%
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
        scrollEnabled={scrollEnabled}
        onScroll={(e) => {
          scrollYRef.current = e.nativeEvent.contentOffset.y;
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
        <View style={{ alignItems: "center" }}>
          <View
            style={{
              transform: [
                { translateX: panX },
                { translateY: panY },
                { scale: zoom },
              ],
            }}
          >
            {imageUris.map((uri, i) => (
              <MangaImage
                key={`${chapterNum}-${i}`}
                uri={uri}
                width={SCREEN.width}
                dims={imageSizes[imageNames[i]]}
              />
            ))}
          </View>
        </View>
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
