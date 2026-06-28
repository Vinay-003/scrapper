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
} from "react-native";
import { useRouter } from "expo-router";
import { api } from "../src/lib/api";
import { t } from "../src/lib/theme";
import { getWishlist, removeFromWishlist, addToWishlist } from "../src/lib/storage";

interface Job {
  id: string;
  url: string;
  site: string;
  site_name: string;
  status: string;
  progress: number;
  completed_chapters: number;
  total_chapters: number;
  failed_chapters: number[];
  log: string[];
}

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
  const [jobs, setJobs] = useState<Job[]>([]);
  const [detected, setDetected] = useState("");
  const [wishlist, setWishlist] = useState<{ title: string; url: string }[]>([]);
  const [wishTitle, setWishTitle] = useState("");
  const [wishUrl, setWishUrl] = useState("");
  const colors = t();
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    (async () => {
      const [sitesData, jobsData, wishData] = await Promise.all([
        api("/api/sites"),
        api("/api/scraper/jobs"),
        getWishlist(),
      ]);
      if (sitesData?.sites) setSites(sitesData.sites);
      if (jobsData?.jobs) setJobs(jobsData.jobs);
      setWishlist(wishData);
    })();
  }, []);

  const detectSite = async (u: string) => {
    if (!u) { setDetected(""); return; }
    const data = await api("/api/scraper/detect", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url: u }),
    });
    if (data?.detected) {
      setDetected(data.name);
      setSelectedSite(data.domain);
    } else {
      setDetected("Not supported");
    }
  };

  const startJob = async () => {
    if (!url.trim()) return Alert.alert("Error", "Enter a manga URL");
    const config: any = {
      url: url.trim(),
      site: selectedSite || undefined,
      start: startCh ? parseFloat(startCh) : null,
      end: endCh ? parseFloat(endCh) : null,
    };
    const data = await api("/api/scraper/start", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(config),
    });
    if (data) {
      Alert.alert("Started", `Download started on ${data.site}`);
      startPolling();
    }
  };

  const startPolling = () => {
    if (pollRef.current) return;
    pollRef.current = setInterval(async () => {
      const data = await api("/api/scraper/jobs");
      if (data?.jobs) setJobs(data.jobs);
      const active = (data?.jobs || []).filter(
        (j: Job) => j.status !== "completed" && j.status !== "failed"
      );
      if (active.length === 0 && pollRef.current) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    }, 2000);
  };

  const deleteJob = async (id: string) => {
    await api(`/api/scraper/jobs/${id}`, { method: "DELETE" });
    setJobs((prev) => prev.filter((j) => j.id !== id));
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

  return (
    <ScrollView style={[styles.container, { backgroundColor: colors.bg }]} contentContainerStyle={{ padding: 20 }}>
      {/* Header */}
      <View style={styles.header}>
        <TouchableOpacity onPress={() => router.back()}>
          <Text style={[styles.backText, { color: colors.accent }]}>Back</Text>
        </TouchableOpacity>
        <Text style={[styles.title, { color: colors.fg }]}>Manga Scraper</Text>
      </View>

      {/* Config */}
      <View style={[styles.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
        <Text style={[styles.label, { color: colors.fg2 }]}>Site</Text>
        <ScrollView horizontal showsHorizontalScrollIndicator={false} style={styles.siteRow}>
          <TouchableOpacity
            style={[styles.sitePill, !selectedSite && { backgroundColor: colors.accent }]}
            onPress={() => setSelectedSite("")}
          >
            <Text style={[styles.sitePillText, !selectedSite ? { color: "#fff" } : { color: colors.fg }]}>
              Auto-detect
            </Text>
          </TouchableOpacity>
          {Object.entries(sites).map(([domain, name]) => (
            <TouchableOpacity
              key={domain}
              style={[styles.sitePill, selectedSite === domain && { backgroundColor: colors.accent }]}
              onPress={() => setSelectedSite(domain)}
            >
              <Text
                style={[
                  styles.sitePillText,
                  selectedSite === domain ? { color: "#fff" } : { color: colors.fg },
                ]}
              >
                {name as string}
              </Text>
            </TouchableOpacity>
          ))}
        </ScrollView>

        <Text style={[styles.label, { color: colors.fg2, marginTop: 14 }]}>Manga URL</Text>
        <TextInput
          style={[styles.input, { backgroundColor: colors.bg, borderColor: colors.border, color: colors.fg }]}
          placeholder="https://manhuaplus.com/manga/your-manga/"
          placeholderTextColor={colors.fg2}
          value={url}
          onChangeText={(u) => { setUrl(u); detectSite(u); }}
          autoCapitalize="none"
          autoCorrect={false}
        />
        {detected ? (
          <Text style={[styles.hint, { color: detected === "Not supported" ? colors.danger : colors.success }]}>
            {detected}
          </Text>
        ) : null}

        <View style={styles.row}>
          <View style={styles.halfField}>
            <Text style={[styles.label, { color: colors.fg2 }]}>Start Ch</Text>
            <TextInput
              style={[styles.input, { backgroundColor: colors.bg, borderColor: colors.border, color: colors.fg }]}
              placeholder="Auto"
              placeholderTextColor={colors.fg2}
              keyboardType="numeric"
              value={startCh}
              onChangeText={setStartCh}
            />
          </View>
          <View style={styles.halfField}>
            <Text style={[styles.label, { color: colors.fg2 }]}>End Ch</Text>
            <TextInput
              style={[styles.input, { backgroundColor: colors.bg, borderColor: colors.border, color: colors.fg }]}
              placeholder="Auto"
              placeholderTextColor={colors.fg2}
              keyboardType="numeric"
              value={endCh}
              onChangeText={setEndCh}
            />
          </View>
        </View>

        <TouchableOpacity style={[styles.startBtn, { backgroundColor: colors.accent }]} onPress={startJob}>
          <Text style={styles.startBtnText}>Start Download</Text>
        </TouchableOpacity>
      </View>

      {/* Jobs */}
      <Text style={[styles.sectionTitle, { color: colors.fg }]}>Downloads</Text>
      {jobs.length === 0 ? (
        <Text style={[styles.empty, { color: colors.fg2 }]}>No active downloads</Text>
      ) : (
        jobs.map((job) => (
          <View key={job.id} style={[styles.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
            <Text style={[styles.jobUrl, { color: colors.fg }]} numberOfLines={1}>
              {job.url}
            </Text>
            <View style={styles.progressBar}>
              <View style={[styles.progressFill, { width: `${job.progress}%`, backgroundColor: colors.accent }]} />
            </View>
            <View style={styles.jobRow}>
              <Text style={[styles.jobMeta, { color: colors.fg2 }]}>
                {job.completed_chapters}/{job.total_chapters} chapters
              </Text>
              <Text style={[styles.jobMeta, { color: colors.fg2 }]}>{job.progress}%</Text>
              {job.status === "completed" && (
                <Text style={[styles.jobStatus, { color: colors.success }]}>Done</Text>
              )}
              {job.status === "failed" && (
                <Text style={[styles.jobStatus, { color: colors.danger }]}>Failed</Text>
              )}
            </View>
            <TouchableOpacity onPress={() => deleteJob(job.id)}>
              <Text style={[styles.removeBtn, { color: colors.danger }]}>Remove</Text>
            </TouchableOpacity>
          </View>
        ))
      )}

      {/* Wishlist */}
      <Text style={[styles.sectionTitle, { color: colors.fg, marginTop: 24 }]}>Wishlist</Text>
      <View style={styles.wishForm}>
        <TextInput
          style={[styles.input, { backgroundColor: colors.bg, borderColor: colors.border, color: colors.fg, flex: 1 }]}
          placeholder="Title"
          placeholderTextColor={colors.fg2}
          value={wishTitle}
          onChangeText={setWishTitle}
        />
        <TextInput
          style={[styles.input, { backgroundColor: colors.bg, borderColor: colors.border, color: colors.fg, flex: 1 }]}
          placeholder="URL"
          placeholderTextColor={colors.fg2}
          value={wishUrl}
          onChangeText={setWishUrl}
          autoCapitalize="none"
        />
        <TouchableOpacity style={[styles.addBtn, { backgroundColor: colors.accent }]} onPress={addWish}>
          <Text style={styles.addBtnText}>Add</Text>
        </TouchableOpacity>
      </View>
      {wishlist.map((w) => (
        <View key={w.url} style={[styles.card, { backgroundColor: colors.bg2, borderColor: colors.border }]}>
          <Text style={[styles.wishTitle, { color: colors.fg }]}>{w.title}</Text>
          <Text style={[styles.wishUrl, { color: colors.fg2 }]} numberOfLines={1}>
            {w.url}
          </Text>
          <View style={styles.wishActions}>
            <TouchableOpacity
              style={[styles.scrapeBtn, { backgroundColor: colors.accent }]}
              onPress={() => { setUrl(w.url); detectSite(w.url); }}
            >
              <Text style={styles.scrapeBtnText}>Use</Text>
            </TouchableOpacity>
            <TouchableOpacity onPress={() => removeWish(w.url)}>
              <Text style={[styles.removeBtn, { color: colors.danger }]}>Remove</Text>
            </TouchableOpacity>
          </View>
        </View>
      ))}
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1 },
  header: { marginBottom: 16 },
  backText: { fontSize: 16, fontWeight: "600", marginBottom: 10 },
  title: { fontSize: 24, fontWeight: "800" },
  card: { padding: 16, borderRadius: 12, marginBottom: 12, borderWidth: 1 },
  label: { fontSize: 13, marginBottom: 6, fontWeight: "600" },
  input: {
    borderWidth: 1,
    borderRadius: 10,
    padding: 12,
    fontSize: 15,
    marginBottom: 4,
  },
  hint: { fontSize: 12, marginBottom: 8 },
  siteRow: { flexDirection: "row", marginBottom: 4 },
  sitePill: { paddingHorizontal: 14, paddingVertical: 8, borderRadius: 20, marginRight: 8 },
  sitePillText: { fontSize: 13, fontWeight: "600" },
  row: { flexDirection: "row", gap: 12, marginTop: 10 },
  halfField: { flex: 1 },
  startBtn: { paddingVertical: 14, borderRadius: 10, alignItems: "center", marginTop: 14 },
  startBtnText: { color: "#fff", fontWeight: "700", fontSize: 16 },
  sectionTitle: { fontSize: 18, fontWeight: "700", marginBottom: 10 },
  empty: { fontSize: 14, marginBottom: 16 },
  jobUrl: { fontSize: 13, marginBottom: 8 },
  progressBar: { height: 6, backgroundColor: "#333", borderRadius: 3, overflow: "hidden", marginBottom: 8 },
  progressFill: { height: "100%", borderRadius: 3 },
  jobRow: { flexDirection: "row", gap: 12, marginBottom: 8 },
  jobMeta: { fontSize: 12 },
  jobStatus: { fontSize: 12, fontWeight: "700" },
  removeBtn: { fontSize: 13, fontWeight: "600" },
  wishForm: { flexDirection: "row", gap: 8, marginBottom: 12, alignItems: "center" },
  addBtn: { paddingHorizontal: 16, paddingVertical: 12, borderRadius: 10 },
  addBtnText: { color: "#fff", fontWeight: "700" },
  wishTitle: { fontSize: 15, fontWeight: "600", marginBottom: 2 },
  wishUrl: { fontSize: 12, marginBottom: 8 },
  wishActions: { flexDirection: "row", gap: 12, alignItems: "center" },
  scrapeBtn: { paddingHorizontal: 14, paddingVertical: 6, borderRadius: 8 },
  scrapeBtnText: { color: "#fff", fontWeight: "600", fontSize: 13 },
});
