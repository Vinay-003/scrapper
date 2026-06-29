/**
 * HTML parsing utilities for React Native scraping.
 * Uses node-html-parser — a real DOM parser with no Node.js dependencies.
 */
import { parse, HTMLElement } from "node-html-parser";

/**
 * Parse HTML string into a root HTMLElement.
 */
export function parseHtml(html: string): HTMLElement {
  return parse(html);
}

/**
 * Find the inner HTML of an element by ID.
 */
export function findById(root: HTMLElement, id: string): HTMLElement | null {
  return root.querySelector(`#${id}`);
}

/**
 * Find first element matching a CSS selector.
 */
export function querySelector(root: HTMLElement, selector: string): HTMLElement | null {
  return root.querySelector(selector);
}

/**
 * Find all elements matching a CSS selector.
 */
export function querySelectorAll(root: HTMLElement, selector: string): HTMLElement[] {
  return root.querySelectorAll(selector);
}

/**
 * Extract image URLs from a container element.
 * Tries data-src/data-lazy-src first, falls back to src.
 */
export function extractImages(container: HTMLElement, preferDataSrc = true): string[] {
  const imgs = container.querySelectorAll("img");
  const results: string[] = [];

  for (const img of imgs) {
    let src = null;
    if (preferDataSrc) {
      src = img.getAttribute("data-src") || img.getAttribute("data-lazy-src") || img.getAttribute("src");
    } else {
      src = img.getAttribute("src") || img.getAttribute("data-src") || img.getAttribute("data-lazy-src");
    }
    if (src && !src.startsWith("data:") && !src.includes("1x1") && !src.includes("spacer")) {
      results.push(src);
    }
  }

  return results;
}

/**
 * Extract <a> links from a container element.
 */
export function extractLinks(
  container: HTMLElement,
  selector: string,
  filter?: (href: string, text: string) => boolean
): { href: string; text: string }[] {
  const links = container.querySelectorAll(selector);
  const results: { href: string; text: string }[] = [];

  for (const a of links) {
    const href = a.getAttribute("href");
    const text = a.textContent.trim().replace(/\s+/g, " ");
    if (href && (!filter || filter(href, text))) {
      results.push({ href, text });
    }
  }

  return results;
}
