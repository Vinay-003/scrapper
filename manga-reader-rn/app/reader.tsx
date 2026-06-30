import { useEffect, useState, useRef } from "react";
import {
  View,
  Text,
  ScrollView,
  Image,
  TouchableOpacity,
  StyleSheet,
  useWindowDimensions,
  ActivityIndicator,
  StatusBar,
  GestureResponderEvent,
} from "react-native";
import { useLocalSearchParams, useRouter } from "expo-router";
import { t, isDark } from "../src/lib/theme";
import { saveRecentlyRead } from "../src/lib/storage";
import { getChapterImages } from "../src/lib/manga";

const ZOOM_STEPS = [5, 10, 25, 50];

function dist(a: { pageX: number; pageY: number }, b: { pageX: number; pageY: number }) {
  return Math.sqrt((a.pageX - b.pageX) ** 2 + (a.pageY - b.pageY) ** 2);
}

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const { width: SCREEN_WIDTH } = useWindowDimensions();
  const [imageUris, setImageUris] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [showUI, setShowUI] = useState(true);
  const [zoom, setZoom] = useState(1);
  const [panX, setPanX] = useState(0);
  const [panY, setPanY] = useState(0);
  const [zoomStep, setZoomStep] = useState(5);
  const [showStepPicker, setShowStepPicker] = useState(false);
  const [progressPct, setProgressPct] = useState(0);
  const colors = t();
  const chapterNum = parseFloat(chapter!);

  const scrollRef = useRef<ScrollView>(null);
  const scrollYRef = useRef(0);
  const progressRef = useRef(0);
  const lastProgressUpdate = useRef(0);
  const gestureRef = useRef({
    initialDist: 0,
    baseZoom: 1,
    basePanX: 0,
    basePanY: 0,
    baseScrollY: 0,
    panStartX: 0,
    panStartY: 0,
    mode: "none" as "none" | "pinch" | "pan",
    touchStartX: 0,
    touchStartY: 0,
    scrollWasEnabled: true,
  });
  const overshootRef = useRef(0);
  const gestureActiveRef = useRef(false);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setImageUris([]);
      setZoom(1);
      setPanX(0);
      setPanY(0);
      setProgressPct(0);
      progressRef.current = 0;
      lastProgressUpdate.current = 0;
      scrollYRef.current = 0;
      overshootRef.current = 0;
      gestureRef.current.mode = "none";
      gestureActiveRef.current = false;

      const data = await getChapterImages(slug!, chapterNum);
      if (data) {
        setImageUris(data.uris);
        console.log(`[READER] loaded ${data.uris.length} images`);
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [slug, chapterNum]);

  const navigateChapter = (delta: number) => {
    router.replace({ pathname: "/reader", params: { slug: slug!, chapter: chapterNum + delta } });
  };

  const onTouchStart = (e: GestureResponderEvent) => {
    const t2 = e.nativeEvent.touches;
    if (!t2 || t2.length === 0) return;
    gestureRef.current.touchStartX = t2[0].pageX;
    gestureRef.current.touchStartY = t2[0].pageY;

    if (t2.length === 2) {
      gestureRef.current.scrollWasEnabled = zoom <= 1;
      gestureRef.current.initialDist = dist(t2[0], t2[1]);
      gestureRef.current.baseZoom = zoom;
      gestureRef.current.basePanX = panX;
      gestureRef.current.basePanY = panY;
      gestureRef.current.baseScrollY = scrollYRef.current;
      gestureRef.current.mode = "pinch";
      gestureActiveRef.current = true;
      try { scrollRef.current?.setNativeProps({ scrollEnabled: false }); } catch {}
    } else if (t2.length === 1 && zoom > 1) {
      gestureRef.current.panStartX = t2[0].pageX;
      gestureRef.current.panStartY = t2[0].pageY;
      gestureRef.current.basePanX = panX;
      gestureRef.current.basePanY = panY;
      gestureRef.current.mode = "pan";
      gestureActiveRef.current = true;
      try { scrollRef.current?.setNativeProps({ scrollEnabled: false }); } catch {}
    }
  };

  const onTouchMove = (e: GestureResponderEvent) => {
    const t2 = e.nativeEvent.touches;
    if (!t2 || !gestureActiveRef.current) return;

    if (t2.length === 2 && gestureRef.current.mode === "pinch") {
      const d = dist(t2[0], t2[1]);
      const newZoom = Math.max(0.5, Math.min(5, gestureRef.current.baseZoom * (d / gestureRef.current.initialDist)));
      const s = newZoom / gestureRef.current.baseZoom;
      const fx = (t2[0].pageX + t2[1].pageX) / 2;
      const fy = (t2[0].pageY + t2[1].pageY) / 2;
      const bsy = gestureRef.current.baseScrollY;
      setZoom(newZoom);
      setPanX(fx * (1 - s) + gestureRef.current.basePanX * s);
      setPanY((fy + bsy) * (1 - s) + gestureRef.current.basePanY * s);
    } else if (t2.length === 1 && gestureRef.current.mode === "pan") {
      setPanX(gestureRef.current.basePanX + (t2[0].pageX - gestureRef.current.panStartX));
      setPanY(gestureRef.current.basePanY + (t2[0].pageY - gestureRef.current.panStartY));
    }
  };

  const onTouchEnd = (e: GestureResponderEvent) => {
    if (!gestureActiveRef.current && zoom <= 1) {
      const dx = Math.abs(e.nativeEvent.pageX - gestureRef.current.touchStartX);
      const dy = Math.abs(e.nativeEvent.pageY - gestureRef.current.touchStartY);
      if (dx < 10 && dy < 10) setShowUI((v) => !v);
    }
    const wasActive = gestureActiveRef.current;
    gestureRef.current.mode = "none";
    gestureActiveRef.current = false;

    if (wasActive) {
      setTimeout(() => {
        if (zoom <= 1) {
          try { scrollRef.current?.setNativeProps({ scrollEnabled: true }); } catch {}
        }
      }, 100);
    }
  };

  const zoomIn = () => {
    const cur = zoom * 100;
    if (overshootRef.current > 0) {
      const target = cur + overshootRef.current;
      overshootRef.current = 0;
      setZoom(Math.min(5, target / 100));
      return;
    }
    const next = cur + zoomStep;
    if (cur < 100 && next > 100) {
      overshootRef.current = next - 100;
      setZoom(1);
    } else {
      overshootRef.current = 0;
      setZoom(Math.min(5, next / 100));
    }
  };

  const zoomOut = () => {
    const cur = zoom * 100;
    if (overshootRef.current < 0) {
      const target = cur + overshootRef.current;
      overshootRef.current = 0;
      setZoom(Math.max(0.5, target / 100));
      return;
    }
    const next = cur - zoomStep;
    if (cur > 100 && next < 100) {
      overshootRef.current = next - 100;
      setZoom(1);
    } else {
      overshootRef.current = 0;
      setZoom(Math.max(0.5, next / 100));
    }
  };

  const resetZoom = () => {
    setZoom(1);
    setPanX(0);
    setPanY(0);
    overshootRef.current = 0;
    gestureRef.current.mode = "none";
    gestureActiveRef.current = false;
    try { scrollRef.current?.setNativeProps({ scrollEnabled: true }); } catch {}
  };

  const handleScroll = (e: any) => {
    scrollYRef.current = e.nativeEvent.contentOffset.y;
    const now = Date.now();
    if (now - lastProgressUpdate.current < 200) return;
    lastProgressUpdate.current = now;
    const { contentOffset, contentSize, layoutMeasurement } = e.nativeEvent;
    if (contentSize.height > layoutMeasurement.height) {
      const pct = Math.round((contentOffset.y / (contentSize.height - layoutMeasurement.height)) * 100);
      if (pct !== progressRef.current) {
        progressRef.current = pct;
        setProgressPct(pct);
      }
    }
  };

  const zoomPct = Math.round(zoom * 100);

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
            Ch {chapterNum} · {imageUris.length} imgs
          </Text>
          <View style={s.zoomRow}>
            <TouchableOpacity style={[s.zoomBtn, { backgroundColor: colors.bg3 }]} onPress={zoomOut}>
              <Text style={[s.zoomText, { color: colors.fg }]}>−</Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[s.stepBadge, { backgroundColor: colors.bg3 }]}
              onPress={() => setShowStepPicker((v) => !v)}
            >
              <Text style={[s.zoomLabel, { color: colors.fg3 }]}>
                {zoomPct}% <Text style={{ fontSize: 10 }}>({zoomStep}%)</Text>
              </Text>
            </TouchableOpacity>
            {showStepPicker && (
              <View style={[s.stepPicker, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
                {ZOOM_STEPS.map((step) => (
                  <TouchableOpacity
                    key={step}
                    style={[s.stepOption, zoomStep === step && { backgroundColor: colors.accent + "30" }]}
                    onPress={() => {
                      setZoomStep(step);
                      overshootRef.current = 0;
                      setShowStepPicker(false);
                    }}
                  >
                    <Text style={{ color: colors.fg, fontSize: 13, fontWeight: zoomStep === step ? "700" : "400" }}>
                      {step}%
                    </Text>
                  </TouchableOpacity>
                ))}
              </View>
            )}
            <TouchableOpacity style={[s.zoomBtn, { backgroundColor: colors.bg3 }]} onPress={zoomIn}>
              <Text style={[s.zoomText, { color: colors.fg }]}>+</Text>
            </TouchableOpacity>
          </View>
        </View>
      )}

      <ScrollView
        ref={scrollRef}
        style={s.scroll}
        scrollEnabled={zoom <= 1}
        onScroll={handleScroll}
        scrollEventThrottle={16}
        onTouchStart={onTouchStart}
        onTouchMove={onTouchMove}
        onTouchEnd={onTouchEnd}
      >
        <View
          style={{
            transformOrigin: "top left" as any,
            transform: [{ translateX: panX }, { translateY: panY }, { scale: zoom }],
          }}
        >
          {imageUris.map((uri, i) => (
            <Image
              key={`${chapterNum}-${i}`}
              source={{ uri }}
              style={{ width: SCREEN_WIDTH }}
              resizeMode="contain"
            />
          ))}
        </View>
      </ScrollView>

      {showUI && (
        <View style={[s.bottomBar, { backgroundColor: colors.bg, borderTopColor: colors.border }]}>
          <TouchableOpacity style={[s.navBtn, { backgroundColor: colors.bg3 }]} onPress={() => navigateChapter(-1)}>
            <Text style={[s.navBtnText, { color: colors.fg }]}>← Prev</Text>
          </TouchableOpacity>
          <Text style={[s.progressText, { color: colors.fg3 }]}>{progressPct}%</Text>
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
    paddingTop: 50, paddingHorizontal: 12, paddingBottom: 10, borderBottomWidth: 1,
  },
  barBtn: { padding: 4 },
  barBtnText: { fontSize: 20, fontWeight: "700" },
  barTitle: { fontSize: 15, fontWeight: "700", flex: 1, textAlign: "center" },
  zoomRow: { flexDirection: "row", alignItems: "center", gap: 4 },
  zoomBtn: { width: 32, height: 32, borderRadius: 8, justifyContent: "center", alignItems: "center" },
  zoomText: { fontSize: 18, fontWeight: "700" },
  stepBadge: { paddingHorizontal: 6, paddingVertical: 4, borderRadius: 6 },
  zoomLabel: { fontSize: 12, textAlign: "center" },
  stepPicker: {
    position: "absolute", top: 38, right: 0, borderRadius: 8,
    borderWidth: 1, paddingVertical: 4, zIndex: 10, minWidth: 70,
  },
  stepOption: { paddingHorizontal: 12, paddingVertical: 6, borderRadius: 4 },
  bottomBar: {
    flexDirection: "row", alignItems: "center", justifyContent: "space-between",
    paddingHorizontal: 16, paddingVertical: 10, borderTopWidth: 1,
  },
  navBtn: { paddingHorizontal: 18, paddingVertical: 10, borderRadius: 10 },
  navBtnText: { fontWeight: "700", fontSize: 14 },
  progressText: { fontSize: 13 },
});
