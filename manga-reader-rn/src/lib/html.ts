/**
 * Lightweight HTML utilities for React Native scraping.
 * No Node.js dependencies — pure regex matching for the patterns we actually need.
 */

// --- Image extraction ---

/**
 * Find all <img> tags in HTML and extract their src.
 * Tries data-src/data-lazy-src first, falls back to src.
 */
export function findImages(html: string, preferDataSrc = true): string[] {
  const results: string[] = [];
  const imgRegex = /<img\s+[^>]*?>/gi;
  let match;

  while ((match = imgRegex.exec(html)) !== null) {
    const tag = match[0];
    let src = null;

    if (preferDataSrc) {
      src = getAttr(tag, "data-src") || getAttr(tag, "data-lazy-src") || getAttr(tag, "src");
    } else {
      src = getAttr(tag, "src") || getAttr(tag, "data-src") || getAttr(tag, "data-lazy-src");
    }

    if (src && !src.startsWith("data:") && !src.includes("1x1") && !src.includes("spacer")) {
      results.push(src);
    }
  }

  return results;
}

// --- Element extraction ---

/**
 * Extract the inner HTML of an element found by ID.
 * Uses tag depth counting for correct nesting.
 * e.g., findById(html, 'chapter-reader') → inner HTML string
 */
export function findById(html: string, id: string): string | null {
  // Find opening tag with this id
  const tagRegex = new RegExp(`<([\\w-]+)\\s[^>]*?id=["']${escapeRegex(id)}["'][^>]*?>`, "i");
  const openMatch = tagRegex.exec(html);
  if (!openMatch) return null;

  const tagName = openMatch[1].toLowerCase();
  const startPos = openMatch.index + openMatch[0].length;

  // Use depth counting to find the matching close tag
  return extractInnerByTag(html, tagName, startPos);
}

/**
 * Extract inner HTML of elements matching a CSS class selector.
 * Supports: '.class', 'tag.class', '.class1 .class2'
 * e.g., selectInner(html, '.reading-content') → inner HTML string
 */
export function selectInner(html: string, selector: string): string | null {
  const parts = selector.trim().split(/\s+/);
  let currentHtml = html;

  for (const part of parts) {
    const result = findFirstMatching(currentHtml, part);
    if (!result) return null;
    currentHtml = result;
  }

  return currentHtml;
}

/**
 * Select all <a> tags matching a selector and extract href + text.
 * e.g., selectLinks(html, '.chapter-list a') → [{href, text}]
 */
export function selectLinks(
  html: string,
  selector: string,
  filter?: (href: string, text: string) => boolean
): { href: string; text: string }[] {
  const results: { href: string; text: string }[] = [];

  // Find the container first
  const parts = selector.trim().split(/\s+/);
  let containerHtml = html;

  // Everything except the last part is a container
  for (let i = 0; i < parts.length - 1; i++) {
    const result = findFirstMatching(containerHtml, parts[i]);
    if (!result) return [];
    containerHtml = result;
  }

  // Last part is the tag to extract (usually 'a')
  const lastPart = parts[parts.length - 1];
  const tagRegex = /<a\s+[^>]*?>[\s\S]*?<\/a>/gi;
  let match;

  while ((match = tagRegex.exec(containerHtml)) !== null) {
    const tag = match[0];
    const href = getAttr(tag, "href");
    const linkText = tag.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
    if (href && (!filter || filter(href, linkText))) {
      results.push({ href, text: linkText });
    }
  }

  return results;
}

// --- Attribute extraction ---

/**
 * Get an attribute value from an HTML tag string.
 */
export function getAttr(tag: string, name: string): string | null {
  const regex = new RegExp(`${name}\\s*=\\s*["']([^"']*)["']`, "i");
  const m = tag.match(regex);
  if (m) return m[1];

  // Boolean attribute (just present, no value)
  const boolRegex = new RegExp(`\\s${name}(?:\\s|>)`, "i");
  if (boolRegex.test(tag)) return "";

  return null;
}

// --- Internal helpers ---

function findFirstMatching(html: string, selector: string): string | null {
  // Parse selector: tag, .class, #id, tag.class, tag#id
  const tagMatch = selector.match(/^(\w+)?/);
  const classMatch = selector.match(/\.([\w-]+)/);
  const idMatch = selector.match(/#([\w-]+)/);

  const tagName = tagMatch?.[1] || (classMatch || idMatch ? "*" : null);

  // Build a regex to find the opening tag
  let tagPattern: RegExp;

  if (idMatch) {
    // Find by ID
    tagPattern = new RegExp(
      `<${tagName || "\\w+"}\\s[^>]*?id=["']${escapeRegex(idMatch[1])}["'][^>]*?>`,
      "gi"
    );
  } else if (classMatch) {
    // Find by class
    tagPattern = new RegExp(
      `<${tagName || "\\w+"}\\s[^>]*?class=["'][^"']*\\b${escapeRegex(classMatch[1])}\\b[^"']*["'][^>]*?>`,
      "gi"
    );
  } else if (tagName) {
    tagPattern = new RegExp(`<${tagName}(?:\\s[^>]*)?>`, "gi");
  } else {
    return null;
  }

  const openMatch = tagPattern.exec(html);
  if (!openMatch) return null;

  const actualTag = openMatch[1]?.toLowerCase() || tagName?.toLowerCase() || "div";
  const startPos = openMatch.index + openMatch[0].length;

  return extractInnerByTag(html, actualTag, startPos);
}

/**
 * Extract inner HTML by counting tag depth.
 * Starting from `startPos`, finds the matching closing tag.
 */
function extractInnerByTag(html: string, tagName: string, startPos: number): string | null {
  let depth = 1;
  const closeTagLower = `</${tagName}`;
  const openTagLower = `<${tagName}`;

  // Match both opening and closing tags of this element type
  const tagPattern = new RegExp(`<(/?)(${escapeRegex(tagName)})(\\s|>|/>)`, "gi");
  tagPattern.lastIndex = startPos;

  while (depth > 0) {
    const m = tagPattern.exec(html);
    if (!m) break;

    const isClosing = m[1] === "/";
    if (isClosing) {
      depth--;
      if (depth === 0) {
        return html.substring(startPos, m.index);
      }
    } else {
      // Don't count self-closing tags
      if (!m[0].endsWith("/>")) {
        depth++;
      }
    }
  }

  // If we didn't find a matching close tag, return everything after start
  return html.substring(startPos);
}

function escapeRegex(str: string): string {
  return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
