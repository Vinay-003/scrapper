import { useEffect, useState, useRef } from "react";
import {
  View,
  Text,
  TouchableOpacity,
  StyleSheet,
  ActivityIndicator,
  StatusBar,
} from "react-native";
import { WebView } from "react-native-webview";
import { useLocalSearchParams, useRouter } from "expo-router";
import { t, isDark } from "../src/lib/theme";
import { saveRecentlyRead } from "../src/lib/storage";
import { getChapterImages } from "../src/lib/manga";
import { getChapterDir } from "../src/lib/cbz";

function buildReaderHtml(filenames: string[], dark: boolean): string {
  const bg = dark ? "#0a0a0c" : "#ffffff";
  const imgTags = filenames
    .map((name) => `<img src="${name}" style="width:100%;display:block;margin:0;padding:0;" />`)
    .join("\n");

  return `<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=5.0, user-scalable=yes">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  html, body { background: ${bg}; width: 100%; overflow-x: hidden; }
  img { width: 100%; display: block; margin: 0; padding: 0; }
</style>
</head>
<body>
${imgTags}
</body>
</html>`;
}

export default function ReaderScreen() {
  const { slug, chapter } = useLocalSearchParams<{ slug: string; chapter: string }>();
  const router = useRouter();
  const [html, setHtml] = useState<string | null>(null);
  const [baseUrl, setBaseUrl] = useState("");
  const [loading, setLoading] = useState(true);
  const [showUI, setShowUI] = useState(true);
  const [imageCount, setImageCount] = useState(0);
  const colors = t();
  const chapterNum = parseFloat(chapter!);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setHtml(null);

      const data = await getChapterImages(slug!, chapterNum);
      if (data && data.names.length > 0) {
        setImageCount(data.names.length);
        setHtml(buildReaderHtml(data.names, isDark()));
        const dir = getChapterDir(slug!, chapterNum);
        setBaseUrl(dir.uri);
        console.log(`[READER] loaded ${data.names.length} images, base: ${dir.uri}`);
      }
      setLoading(false);
      saveRecentlyRead(slug!, chapterNum);
    })();
  }, [slug, chapterNum]);

  const navigateChapter = (delta: number) => {
    router.replace({ pathname: "/reader", params: { slug: slug!, chapter: chapterNum + delta } });
  };

  if (loading) {
    return (
      <View style={[s.loadingContainer, { backgroundColor: colors.bg }]}>
        <ActivityIndicator size="large" color={colors.accent} />
        <Text style={[s.loadingText, { color: colors.fg3 }]}>Loading...</Text>
      </View>
    );
  }

  if (!html) {
    return (
      <View style={[s.loadingContainer, { backgroundColor: colors.bg }]}>
        <Text style={[s.loadingText, { color: colors.fg3 }]}>No images found</Text>
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
            Ch {chapterNum} · {imageCount} imgs
          </Text>
          <View style={{ width: 40 }} />
        </View>
      )}

      <WebView
        originWhitelist={["*"]}
        source={{ html, baseUrl }}
        style={[s.webview, { backgroundColor: colors.bg }]}
        allowFileAccess
        allowUniversalAccessFromFileURLs
        scrollEnabled
        bounces
        nestedScrollEnabled
        javaScriptEnabled={false}
      />

      {showUI && (
        <View style={[s.bottomBar, { backgroundColor: colors.bg, borderTopColor: colors.border }]}>
          <TouchableOpacity style={[s.navBtn, { backgroundColor: colors.bg3 }]} onPress={() => navigateChapter(-1)}>
            <Text style={[s.navBtnText, { color: colors.fg }]}>← Prev</Text>
          </TouchableOpacity>
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
  webview: { flex: 1 },
  topBar: {
    flexDirection: "row", alignItems: "center", justifyContent: "space-between",
    paddingTop: 50, paddingHorizontal: 12, paddingBottom: 10, borderBottomWidth: 1,
  },
  barBtn: { padding: 4 },
  barBtnText: { fontSize: 20, fontWeight: "700" },
  barTitle: { fontSize: 15, fontWeight: "700", flex: 1, textAlign: "center" },
  bottomBar: {
    flexDirection: "row", alignItems: "center", justifyContent: "space-between",
    paddingHorizontal: 16, paddingVertical: 10, borderTopWidth: 1,
  },
  navBtn: { paddingHorizontal: 18, paddingVertical: 10, borderRadius: 10 },
  navBtnText: { fontWeight: "700", fontSize: 14 },
});
