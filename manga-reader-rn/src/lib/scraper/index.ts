import { Directory, Paths } from "expo-file-system/next";
import { getAdapter, detectSite } from "./registry";
import { BaseSiteAdapter, ChapterInfo } from "./base";
import { createCbz } from "../cbz";
import { loadJson, saveJson } from "../storage";

const JOBS_FILE = "scraper_jobs.json";
const MANGA_DIR = new Directory(Paths.document, "manga");

export interface ScraperJob {
  id: string;
  url: string;
  site: string;
  siteName: string;
  start: number | null;
  end: number | null;
  workers: number;
  chapterWorkers: number;
  status: "starting" | "fetching" | "downloading" | "completed" | "failed";
  progress: number;
  currentChapter: number | null;
  totalChapters: number;
  completedChapters: number;
  failedChapters: number[];
  log: string[];
  createdAt: string;
  error: string | null;
  mangaSlug: string | null;
  mangaTitle: string | null;
  chapters: ChapterInfo[];
  abortController: AbortController | null;
}

export interface ScraperConfig {
  url: string;
  start?: number;
  end?: number;
  site?: string;
  workers?: number;
  chapterWorkers?: number;
}

const activeJobs = new Map<string, ScraperJob>();

function addLog(job: ScraperJob, msg: string) {
  const ts = new Date().toLocaleTimeString();
  job.log.push(`[${ts}] ${msg}`);
  if (job.log.length > 200) job.log.shift();
}

function updateProgress(job: ScraperJob) {
  if (job.totalChapters > 0) {
    job.progress = Math.round((job.completedChapters / job.totalChapters) * 100);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithTimeout(
  url: string,
  options: RequestInit = {},
  timeoutMs = 30000
): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function downloadImage(url: string, adapter: BaseSiteAdapter): Promise<Uint8Array | null> {
  try {
    const normalizedUrl = adapter.normalizeImageUrl(url);
    const resp = await fetchWithTimeout(normalizedUrl, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        Referer: `https://${adapter.domain}/`,
      },
    });
    if (!resp.ok) return null;
    const blob = await resp.blob();
    const reader = new FileReader();
    return new Promise((resolve) => {
      reader.onload = () => {
        const buf = reader.result as ArrayBuffer;
        resolve(new Uint8Array(buf));
      };
      reader.onerror = () => resolve(null);
      reader.readAsArrayBuffer(blob);
    });
  } catch {
    return null;
  }
}

async function scrapeChapterImages(
  adapter: BaseSiteAdapter,
  chapterUrl: string
): Promise<string[]> {
  try {
    const resp = await fetchWithTimeout(chapterUrl, {
      headers: adapter["headers"],
    });
    if (!resp.ok) return [];
    const html = await resp.text();
    return adapter.getImageUrlsFromPage(html);
  } catch {
    return [];
  }
}

async function runScraperJob(job: ScraperJob): Promise<void> {
  addLog(job, `Starting scrape for ${job.url}`);
  job.status = "fetching";

  const detected = detectSite(job.url);
  const adapter = detected ? getAdapter(detected.domain) : null;

  if (!adapter) {
    job.status = "failed";
    job.error = "Unsupported site or could not detect site";
    addLog(job, "Failed: unsupported site");
    return;
  }

  // Resolve manga slug
  const slug = await adapter.resolveMangaSlug(job.url);
  if (!slug) {
    job.status = "failed";
    job.error = "Could not resolve manga slug from URL";
    addLog(job, "Failed: could not resolve manga slug");
    return;
  }

  job.mangaSlug = slug;
  job.mangaTitle = slug.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  addLog(job, `Manga: ${job.mangaTitle} (slug: ${slug})`);

  // Get manga page and chapter list
  const mangaUrl = adapter.getMangaUrl(slug);
  addLog(job, `Fetching manga page: ${mangaUrl}`);
  let chapters: ChapterInfo[] = [];
  try {
    const resp = await fetchWithTimeout(mangaUrl, { headers: adapter["headers"] });
    addLog(job, `Manga page status: ${resp.status}`);
    if (resp.ok) {
      const html = await resp.text();
      addLog(job, `Manga page HTML length: ${html.length}`);
      chapters = adapter.getAvailableChapters(html);
      addLog(job, `Chapters from main page: ${chapters.length}`);
    } else {
      addLog(job, `Manga page returned ${resp.status}`);
    }
  } catch (err: any) {
    addLog(job, `Manga page fetch error: ${err.message}`);
  }

  // Fallback to all chapters page
  if (chapters.length === 0) {
    const allUrl = adapter.getAllChaptersUrl(slug);
    if (allUrl) {
      addLog(job, `Trying all chapters page: ${allUrl}`);
      try {
        const resp = await fetchWithTimeout(allUrl, { headers: adapter["headers"] });
        addLog(job, `All chapters page status: ${resp.status}`);
        if (resp.ok) {
          const html = await resp.text();
          addLog(job, `All chapters HTML length: ${html.length}`);
          chapters = adapter.getAvailableChapters(html);
          addLog(job, `Chapters from all-chapters page: ${chapters.length}`);
        }
      } catch (err: any) {
        addLog(job, `All chapters page error: ${err.message}`);
      }
    }
  }

  if (chapters.length === 0) {
    job.status = "failed";
    job.error = "No chapters found";
    addLog(job, "Failed: no chapters found");
    return;
  }

  // Sort and filter by range
  chapters.sort((a, b) => a.number - b.number);
  if (job.start !== null && job.start !== undefined) {
    chapters = chapters.filter((c) => c.number >= job.start!);
  }
  if (job.end !== null && job.end !== undefined) {
    chapters = chapters.filter((c) => c.number <= job.end!);
  }

  job.totalChapters = chapters.length;
  job.chapters = chapters;
  addLog(job, `Found ${chapters.length} chapters to download`);

  // Download chapters
  job.status = "downloading";
  const semaphore = { count: 0, max: job.chapterWorkers || 2 };

  async function downloadChapter(chapter: ChapterInfo): Promise<void> {
    while (semaphore.count >= semaphore.max) {
      await sleep(200);
    }
    semaphore.count++;

    try {
      addLog(job, `Chapter ${chapter.number}: fetching image list`);
      job.currentChapter = chapter.number;

      let imageUrls = await scrapeChapterImages(adapter!, chapter.url);

      // If no images found, try constructing the URL directly
      if (imageUrls.length === 0) {
        const directUrl = adapter!.getChapterUrl(slug!, chapter.number);
        if (directUrl !== chapter.url) {
          imageUrls = await scrapeChapterImages(adapter!, directUrl);
        }
      }

      if (imageUrls.length === 0) {
        addLog(job, `Chapter ${chapter.number}: no images found, skipping`);
        job.failedChapters.push(chapter.number);
        return;
      }

      addLog(job, `Chapter ${chapter.number}: downloading ${imageUrls.length} images`);

      // Download all images with concurrency
      const images: { name: string; data: Uint8Array }[] = [];
      const imgSemaphore = { count: 0, max: job.workers || 4 };

      async function downloadImg(url: string, index: number): Promise<void> {
        while (imgSemaphore.count >= imgSemaphore.max) {
          await sleep(100);
        }
        imgSemaphore.count++;

        try {
          const ext = getExtFromUrl(url);
          const name = `${String(index + 1).padStart(4, "0")}.${ext}`;
          const data = await downloadImage(url, adapter!);
          if (data) {
            images.push({ name, data });
          }
        } finally {
          imgSemaphore.count--;
        }
      }

      await Promise.all(imageUrls.map((url, i) => downloadImg(url, i)));
      images.sort((a, b) => a.name.localeCompare(b.name));

      if (images.length > 0) {
        // Ensure manga directory exists
        const dirPath = MANGA_DIR;
        if (!dirPath.exists) {
          dirPath.create();
        }

        await createCbz(slug!, chapter.number, images);
        addLog(job, `Chapter ${chapter.number}: saved (${images.length} images)`);
        job.completedChapters++;
      } else {
        addLog(job, `Chapter ${chapter.number}: all image downloads failed`);
        job.failedChapters.push(chapter.number);
      }
    } catch (err: any) {
      addLog(job, `Chapter ${chapter.number}: error - ${err.message}`);
      job.failedChapters.push(chapter.number);
    } finally {
      semaphore.count--;
      updateProgress(job);
    }
  }

  // Run downloads with chapter concurrency
  const chapterQueue = [...chapters];
  const running: Promise<void>[] = [];

  for (let i = 0; i < (job.chapterWorkers || 2); i++) {
    running.push(
      (async () => {
        while (chapterQueue.length > 0) {
          if (job.abortController?.signal.aborted) break;
          const chapter = chapterQueue.shift();
          if (chapter) await downloadChapter(chapter);
        }
      })()
    );
  }

  await Promise.all(running);

  if (job.abortController?.signal.aborted) {
    job.status = "failed";
    job.error = "Cancelled";
    addLog(job, "Job cancelled");
  } else {
    job.status = "completed";
    addLog(
      job,
      `Done: ${job.completedChapters}/${job.totalChapters} chapters, ${job.failedChapters.length} failed`
    );
  }

  await saveJobs();
}

function getExtFromUrl(url: string): string {
  const m = url.match(/\.(jpe?g|png|webp|gif)(\?|$)/i);
  if (m) {
    const ext = m[1].toLowerCase();
    return ext === "jpeg" ? "jpg" : ext;
  }
  return "jpg";
}

export async function startJob(config: ScraperConfig): Promise<ScraperJob | null> {
  const detected = detectSite(config.url);
  if (!detected) return null;

  const job: ScraperJob = {
    id: Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
    url: config.url,
    site: detected.domain,
    siteName: detected.name,
    start: config.start ?? null,
    end: config.end ?? null,
    workers: config.workers ?? 4,
    chapterWorkers: config.chapterWorkers ?? 2,
    status: "starting",
    progress: 0,
    currentChapter: null,
    totalChapters: 0,
    completedChapters: 0,
    failedChapters: [],
    log: [],
    createdAt: new Date().toISOString(),
    error: null,
    mangaSlug: null,
    mangaTitle: null,
    chapters: [],
    abortController: new AbortController(),
  };

  activeJobs.set(job.id, job);
  await saveJobs();

  // Run in background
  runScraperJob(job).then(() => saveJobs());

  return job;
}

export function getJobs(): ScraperJob[] {
  return Array.from(activeJobs.values());
}

export function getJob(id: string): ScraperJob | undefined {
  return activeJobs.get(id);
}

export function deleteJob(id: string): boolean {
  const job = activeJobs.get(id);
  if (job) {
    job.abortController?.abort();
    activeJobs.delete(id);
    saveJobs();
    return true;
  }
  return false;
}

async function saveJobs(): Promise<void> {
  const jobs = Array.from(activeJobs.values()).map((j) => ({
    ...j,
    abortController: undefined,
    chapters: undefined,
  }));
  await saveJson(JOBS_FILE, jobs);
}

export async function loadJobs(): Promise<void> {
  const data = await loadJson(JOBS_FILE, []);
  if (Array.isArray(data)) {
    for (const saved of data) {
      if (saved.status !== "completed" && saved.status !== "failed") {
        saved.status = "failed";
        saved.error = "Interrupted by app restart";
      }
      saved.abortController = null;
      activeJobs.set(saved.id, saved);
    }
  }
}
