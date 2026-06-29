import { useEffect, useState, useRef } from "react";
import {
  View,
  Text,
  TextInput,
  TouchableOpacity,
  ScrollView,
  StyleSheet,
  ActivityIndicator,
  Alert,
  StatusBar,
  Share,
} from "react-native";
import { useRouter } from "expo-router";
import { t, isDark } from "../src/lib/theme";
import { getWishlist, removeFromWishlist, addToWishlist } from "../src/lib/storage";
import { getAllSites, detectSite } from "../src/lib/scraper/registry";
import { startJob, getJobs, deleteJob as deleteJobLocal, ScraperJob, loadJobs } from "../src/lib/scraper";

interface SiteInfo {
  [domain: string]: string;
}

export default function ScraperScreen() {
  const router = useRouter();
  const [sites, setSites] = useState<SiteInfo>({});
  const [selectedSite, setSelectedSite] = useState("");
  const [url, setUrl] = useState("");
  const [startCh, setStartCh] = useState("");
  const [endCh, setEndCh] = useState("");
  const [imageWorkers, setImageWorkers] = useState("4");
  const [chapterWorkers, setChapterWorkers] = useState("2");
  const [jobs, setJobs] = useState<ScraperJob[]>([]);
  const [detected, setDetected] = useState("");
  const [detecting, setDetecting] = useState(false);
  const [wishlist, setWishlist] = useState<{ title: string; url: string }[]>([]);
  const [wishTitle, setWishTitle] = useState("");
  const [wishUrl, setWishUrl] = useState("");
  const colors = t();
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const detectTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    (async () => {
      await loadJobs();
      const sitesData = getAllSites();
      const jobsData = getJobs();
      const wishData = await getWishlist();
      setSites(sitesData);
      setJobs(jobsData);
      setWishlist(wishData);
    })();
  }, []);

  const handleDetectSite = (u: string) => {
    if (detectTimer.current) clearTimeout(detectTimer.current);
    if (!u) { setDetected(""); return; }
    detectTimer.current = setTimeout(() => {
      setDetecting(true);
      const result = detectSite(u);
      setDetecting(false);
      if (result) {
        setDetected(result.name);
        setSelectedSite(result.domain);
      } else {
        setDetected("Not supported");
      }
    }, 500);
  };

  const handleStartJob = async () => {
    if (!url.trim()) return Alert.alert("Error", "Enter a manga URL");
    if (detected === "Not supported") return Alert.alert("Error", "This site is not supported");
    const job = await startJob({
      url: url.trim(),
      site: selectedSite || undefined,
      start: startCh ? parseFloat(startCh) : undefined,
      end: endCh ? parseFloat(endCh) : undefined,
      workers: parseInt(imageWorkers) || 4,
      chapterWorkers: parseInt(chapterWorkers) || 2,
    });
    if (job) {
      Alert.alert("Started", `Download started on ${job.siteName}`);
      startPolling();
    }
  };

  const startPolling = () => {
    if (pollRef.current) return;
    pollRef.current = setInterval(() => {
      const currentJobs = getJobs();
      setJobs(currentJobs);
      const active = currentJobs.filter(
        (j) => j.status !== "completed" && j.status !== "failed"
      );
      if (active.length === 0 && pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    }, 2000);
  };

  const handleDeleteJob = async (id: string) => {
    deleteJobLocal(id);
    setJobs(getJobs());
  };

  const addWish = async () => {
    if (!wishTitle.trim() || !wishUrl.trim()) return;
    await addToWishlist(wishTitle.trim(), wishUrl.trim());
    setWishlist(await getWishlist());
    setWishTitle("");
    setWishUrl("");
  };

  const removeWish = async (u: string) => {
    await removeFromWishlist(u);
    setWishlist(await getWishlist());
  };

  const copyLogs = async (job: ScraperJob) => {
    const logText = [
      `URL: ${job.url}`,
      `Site: ${job.siteName}`,
      `Status: ${job.status}`,
      `Slug: ${job.mangaSlug}`,
      `Title: ${job.mangaTitle}`,
      `Progress: ${job.completedChapters}/${job.totalChapters}`,
      `Failed: ${job.failedChapters.join(", ") || "none"}`,
      `Error: ${job.error || "none"}`,
      "",
      "--- LOG ---",
      ...job.log,
    ].join("\n");
    try {
      await Share.share({ message: logText });
    } catch {}
  };

  return (
    <View style={[s.container, { backgroundColor: colors.bg }]}>
      <StatusBar barStyle={isDark() ? "light-content" : "dark-content"} />
      <ScrollView contentContainerStyle={s.scroll}>
        {/* Header */}
        <View style={s.header}>
          <TouchableOpacity onPress={() => router.back()} style={s.backBtn}>
            <Text style={[s.backText, { color: colors.accent }]}>← Back</Text>
          </TouchableOpacity>
          <Text style={[s.title, { color: colors.fg }]}>Scraper</Text>
          <Text style={[s.subtitle, { color: colors.fg3 }]}>Download manga from supported sites</Text>
        </View>

        {/* URL Input */}
        <View style={[s.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
          <Text style={[s.label, { color: colors.fg2 }]}>MANGA URL</Text>
          <TextInput
            style={[s.input, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg }]}
            placeholder="Paste manga or chapter URL..."
            placeholderTextColor={colors.fg3}
            value={url}
            onChangeText={(u) => { setUrl(u); handleDetectSite(u); }}
            autoCapitalize="none"
            autoCorrect={false}
            keyboardType="url"
          />
          {detecting ? (
            <View style={s.detectRow}>
              <ActivityIndicator size="small" color={colors.accent} />
              <Text style={[s.detectText, { color: colors.fg3 }]}> Detecting...</Text>
            </View>
          ) : detected ? (
            <View style={s.detectRow}>
              <View style={[s.detectDot, { backgroundColor: detected === "Not supported" ? colors.danger : colors.accent }]} />
              <Text style={[s.detectText, { color: detected === "Not supported" ? colors.danger : colors.accent }]}>
                {detected}
              </Text>
            </View>
          ) : null}
        </View>

        {/* Chapter Range */}
        <View style={[s.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
          <Text style={[s.label, { color: colors.fg2 }]}>CHAPTER RANGE</Text>
          <View style={s.row}>
            <View style={s.half}>
              <Text style={[s.smallLabel, { color: colors.fg3 }]}>Start</Text>
              <TextInput
                style={[s.input, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg }]}
                placeholder="Auto"
                placeholderTextColor={colors.fg3}
                keyboardType="numeric"
                value={startCh}
                onChangeText={setStartCh}
              />
            </View>
            <View style={s.half}>
              <Text style={[s.smallLabel, { color: colors.fg3 }]}>End</Text>
              <TextInput
                style={[s.input, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg }]}
                placeholder="Auto"
                placeholderTextColor={colors.fg3}
                keyboardType="numeric"
                value={endCh}
                onChangeText={setEndCh}
              />
            </View>
          </View>
        </View>

        {/* Workers */}
        <View style={[s.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
          <Text style={[s.label, { color: colors.fg2 }]}>WORKERS</Text>
          <View style={s.row}>
            <View style={s.half}>
              <Text style={[s.smallLabel, { color: colors.fg3 }]}>Image Workers</Text>
              <TextInput
                style={[s.input, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg }]}
                placeholder="4"
                placeholderTextColor={colors.fg3}
                keyboardType="numeric"
                value={imageWorkers}
                onChangeText={setImageWorkers}
              />
            </View>
            <View style={s.half}>
              <Text style={[s.smallLabel, { color: colors.fg3 }]}>Chapter Workers</Text>
              <TextInput
                style={[s.input, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg }]}
                placeholder="2"
                placeholderTextColor={colors.fg3}
                keyboardType="numeric"
                value={chapterWorkers}
                onChangeText={setChapterWorkers}
              />
            </View>
          </View>
          <Text style={[s.hint, { color: colors.fg3 }]}>Concurrent downloads per chapter / Parallel chapters</Text>
        </View>

        {/* Supported Sites */}
        <View style={[s.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
          <Text style={[s.label, { color: colors.fg2 }]}>SUPPORTED SITES</Text>
          <View style={s.siteGrid}>
            {Object.entries(sites).map(([domain, name]) => (
              <TouchableOpacity
                key={domain}
                style={[
                  s.siteChip,
                  { backgroundColor: colors.bg3, borderColor: colors.border },
                  selectedSite === domain && { backgroundColor: colors.accent, borderColor: colors.accent },
                ]}
                onPress={() => setSelectedSite(selectedSite === domain ? "" : domain)}
              >
                <Text
                  style={[
                    s.siteChipText,
                    { color: selectedSite === domain ? "#0a0a0c" : colors.fg2 },
                  ]}
                >
                  {name as string}
                </Text>
              </TouchableOpacity>
            ))}
          </View>
        </View>

        {/* Start Button */}
        <TouchableOpacity
          style={[
            s.startBtn,
            { backgroundColor: detected === "Not supported" ? colors.bg3 : colors.accent },
          ]}
          onPress={handleStartJob}
          disabled={detected === "Not supported"}
          activeOpacity={0.8}
        >
          <Text style={[s.startBtnText, { color: detected === "Not supported" ? colors.fg3 : "#0a0a0c" }]}>
            Start Download
          </Text>
        </TouchableOpacity>

        {/* Downloads */}
        <Text style={[s.sectionTitle, { color: colors.fg2 }]}>DOWNLOADS</Text>
        {jobs.length === 0 ? (
          <Text style={[s.empty, { color: colors.fg3 }]}>No active downloads</Text>
        ) : (
          jobs.map((job) => (
            <View key={job.id} style={[s.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
              <View style={s.jobHeader}>
                <Text style={[s.jobUrl, { color: colors.fg }]} numberOfLines={1}>
                  {job.url}
                </Text>
                {job.status === "completed" && (
                  <View style={[s.badge, { backgroundColor: colors.accent }]}>
                    <Text style={s.badgeText}>DONE</Text>
                  </View>
                )}
                {job.status === "failed" && (
                  <View style={[s.badge, { backgroundColor: colors.danger }]}>
                    <Text style={s.badgeText}>FAIL</Text>
                  </View>
                )}
                {job.status === "downloading" && (
                  <View style={[s.badge, { backgroundColor: colors.accent }]}>
                    <Text style={s.badgeText}>{job.progress}%</Text>
                  </View>
                )}
              </View>
              {/* Progress bar */}
              <View style={[s.progressBar, { backgroundColor: colors.bg3 }]}>
                <View
                  style={[
                    s.progressFill,
                    {
                      width: `${job.progress}%`,
                      backgroundColor: job.status === "failed" ? colors.danger : colors.accent,
                    },
                  ]}
                />
              </View>
              <View style={s.jobMeta}>
                <Text style={[s.jobMetaText, { color: colors.fg3 }]}>
                  {job.completedChapters}/{job.totalChapters} chapters
                </Text>
                {job.failedChapters.length > 0 && (
                  <Text style={[s.jobMetaText, { color: colors.danger }]}>
                    {job.failedChapters.length} failed
                  </Text>
                )}
              </View>
              {/* Log */}
              {job.log.length > 0 && (
                <View style={[s.logBox, { backgroundColor: colors.bg3 }]}>
                  {job.log.slice(-8).map((line, i) => (
                    <Text key={i} style={[s.logText, { color: colors.fg3 }]} numberOfLines={2}>
                      {line}
                    </Text>
                  ))}
                </View>
              )}
              <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
                <TouchableOpacity onPress={() => copyLogs(job)} style={s.removeBtn}>
                  <Text style={[s.removeText, { color: colors.accent }]}>Copy Logs</Text>
                </TouchableOpacity>
                <TouchableOpacity onPress={() => handleDeleteJob(job.id)} style={s.removeBtn}>
                  <Text style={[s.removeText, { color: colors.danger }]}>Remove</Text>
                </TouchableOpacity>
              </View>
            </View>
          ))
        )}

        {/* Wishlist */}
        <Text style={[s.sectionTitle, { color: colors.fg2, marginTop: 8 }]}>WISHLIST</Text>
        <View style={s.wishForm}>
          <TextInput
            style={[s.wishInput, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg }]}
            placeholder="Title"
            placeholderTextColor={colors.fg3}
            value={wishTitle}
            onChangeText={setWishTitle}
          />
          <TextInput
            style={[s.wishInput, { backgroundColor: colors.bg3, borderColor: colors.border, color: colors.fg, flex: 1.5 }]}
            placeholder="URL"
            placeholderTextColor={colors.fg3}
            value={wishUrl}
            onChangeText={setWishUrl}
            autoCapitalize="none"
          />
          <TouchableOpacity style={[s.addBtn, { backgroundColor: colors.accent }]} onPress={addWish}>
            <Text style={s.addBtnText}>+</Text>
          </TouchableOpacity>
        </View>
        {wishlist.map((w) => (
          <View key={w.url} style={[s.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
            <Text style={[s.wishTitle, { color: colors.fg }]} numberOfLines={1}>{w.title}</Text>
            <Text style={[s.wishUrl, { color: colors.fg3 }]} numberOfLines={1}>{w.url}</Text>
            <View style={s.wishActions}>
              <TouchableOpacity
                style={[s.useBtn, { backgroundColor: colors.accent }]}
                onPress={() => { setUrl(w.url); handleDetectSite(w.url); }}
              >
                <Text style={s.useBtnText}>Use</Text>
              </TouchableOpacity>
              <TouchableOpacity onPress={() => removeWish(w.url)}>
                <Text style={[s.removeText, { color: colors.danger }]}>Remove</Text>
              </TouchableOpacity>
            </View>
          </View>
        ))}
      </ScrollView>
    </View>
  );
}

const s = StyleSheet.create({
  container: { flex: 1 },
  scroll: { padding: 20, paddingBottom: 40 },
  header: { marginBottom: 20 },
  backBtn: { marginBottom: 12 },
  backText: { fontSize: 15, fontWeight: "600" },
  title: { fontSize: 32, fontWeight: "900", letterSpacing: -1 },
  subtitle: { fontSize: 14, marginTop: 4 },
  card: { padding: 16, borderRadius: 14, marginBottom: 12, borderWidth: 1 },
  label: { fontSize: 10, fontWeight: "800", textTransform: "uppercase", letterSpacing: 1.5, marginBottom: 10 },
  smallLabel: { fontSize: 11, fontWeight: "600", marginBottom: 6 },
  input: { borderWidth: 1, borderRadius: 10, padding: 12, fontSize: 15, marginBottom: 4 },
  row: { flexDirection: "row", gap: 12 },
  half: { flex: 1 },
  detectRow: { flexDirection: "row", alignItems: "center", marginTop: 6 },
  detectDot: { width: 6, height: 6, borderRadius: 3, marginRight: 6 },
  detectText: { fontSize: 12, fontWeight: "600" },
  hint: { fontSize: 11, marginTop: 6 },
  siteGrid: { flexDirection: "row", flexWrap: "wrap", gap: 8 },
  siteChip: { paddingHorizontal: 12, paddingVertical: 8, borderRadius: 8, borderWidth: 1 },
  siteChipText: { fontSize: 12, fontWeight: "600" },
  startBtn: { paddingVertical: 16, borderRadius: 12, alignItems: "center", marginBottom: 24 },
  startBtnText: { fontSize: 16, fontWeight: "800", letterSpacing: 0.5 },
  sectionTitle: {
    fontSize: 10,
    fontWeight: "800",
    textTransform: "uppercase",
    letterSpacing: 1.5,
    marginBottom: 10,
  },
  empty: { fontSize: 14, marginBottom: 16 },
  jobHeader: { flexDirection: "row", justifyContent: "space-between", alignItems: "center", marginBottom: 8 },
  jobUrl: { fontSize: 13, fontWeight: "600", flex: 1 },
  badge: { paddingHorizontal: 8, paddingVertical: 3, borderRadius: 6, marginLeft: 8 },
  badgeText: { fontSize: 10, fontWeight: "800", color: "#0a0a0c" },
  progressBar: { height: 4, borderRadius: 2, overflow: "hidden", marginBottom: 8 },
  progressFill: { height: "100%", borderRadius: 2 },
  jobMeta: { flexDirection: "row", gap: 12, marginBottom: 8 },
  jobMetaText: { fontSize: 12 },
  logBox: { padding: 8, borderRadius: 8, marginBottom: 8 },
  logText: { fontSize: 11, fontFamily: "monospace", lineHeight: 16 },
  removeBtn: { alignItems: "flex-end" },
  removeText: { fontSize: 13, fontWeight: "600" },
  wishForm: { flexDirection: "row", gap: 8, marginBottom: 12, alignItems: "center" },
  wishInput: { flex: 1, borderWidth: 1, borderRadius: 10, padding: 10, fontSize: 14 },
  addBtn: { width: 40, height: 40, borderRadius: 10, justifyContent: "center", alignItems: "center" },
  addBtnText: { fontSize: 20, fontWeight: "800", color: "#0a0a0c" },
  wishTitle: { fontSize: 15, fontWeight: "600", marginBottom: 2 },
  wishUrl: { fontSize: 12, marginBottom: 10 },
  wishActions: { flexDirection: "row", gap: 12, alignItems: "center" },
  useBtn: { paddingHorizontal: 14, paddingVertical: 6, borderRadius: 8 },
  useBtnText: { color: "#0a0a0c", fontWeight: "700", fontSize: 13 },
});
