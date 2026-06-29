import { BaseSiteAdapter } from "./base";
import { MadaraAdapter, ManhuaPlusAdapter, ManhwaTopAdapter, ManhuascanAdapter, ManhuaPlusTopAdapter } from "./madara";
import { AsuraAdapter } from "./asura";
import { ArenaAdapter } from "./arena";
import { MgekoAdapter } from "./mgeko";
import { ThunderscansAdapter, RoliascanAdapter } from "./thunderscans";

const SITE_REGISTRY: Record<string, () => BaseSiteAdapter> = {
  "arenascans.com": () => new ArenaAdapter(),
  "asurascanz.com": () => new AsuraAdapter(),
  "manhuaplus.com": () => new ManhuaPlusAdapter(),
  "manhuascan.us": () => new ManhuascanAdapter(),
  "mgeko.cc": () => new MgekoAdapter(),
  "en-thunderscans.com": () => new ThunderscansAdapter(),
  "roliascan.com": () => new RoliascanAdapter(),
  "manhwatop.com": () => new ManhwaTopAdapter(),
  "manhuaplus.top": () => new ManhuaPlusTopAdapter(),
};

const SITE_NAMES: Record<string, string> = {
  "arenascans.com": "Arenascans",
  "asurascanz.com": "Asura Scans",
  "manhuaplus.com": "ManhuaPlus",
  "manhuascan.us": "Manhuascan",
  "mgeko.cc": "Mgeko",
  "en-thunderscans.com": "Thunder Scans",
  "roliascan.com": "Roliascan",
  "manhwatop.com": "ManhwaTop",
  "manhuaplus.top": "ManhuaPlusTop",
};

export function getAdapter(domain: string): BaseSiteAdapter | null {
  const factory = SITE_REGISTRY[domain];
  return factory ? factory() : null;
}

export function getAllSites(): Record<string, string> {
  const result: Record<string, string> = {};
  for (const [domain, name] of Object.entries(SITE_NAMES)) {
    result[domain] = name;
  }
  return result;
}

export function isSupportedSite(url: string): boolean {
  return !!detectSite(url);
}

export function detectSite(url: string): { domain: string; name: string } | null {
  for (const [domain, name] of Object.entries(SITE_NAMES)) {
    if (url.includes(domain)) {
      return { domain, name };
    }
  }
  return null;
}
