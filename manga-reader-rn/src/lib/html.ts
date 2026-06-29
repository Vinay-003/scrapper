/**
 * Lightweight HTML parser for React Native.
 * No Node.js dependencies - pure regex string matching.
 */

/**
 * Find all elements matching a CSS-like selector.
 * Supports: tag, .class, #id, tag.class, tag#id, [attr]
 */
export function select(html: string, selector: string): string[] {
  const elements = splitElements(html);
  return elements.filter((el) => matchesSelector(el, selector));
}

/**
 * Get attribute value from an HTML element string.
 */
export function attr(element: string, name: string): string | null {
  // Match attribute with value: attr="value" or attr='value' or attr=value
  const regex = new RegExp(`${name}\\s*=\\s*["']([^"']*)["']`, "i");
  const m = element.match(regex);
  if (m) return m[1];

  // Boolean attribute (just present)
  const boolRegex = new RegExp(`\\s${name}(?:\\s|>)`, "i");
  if (boolRegex.test(element)) return "";

  return null;
}

/**
 * Get text content from an HTML element (strips tags).
 */
export function text(element: string): string {
  return element.replace(/<[^>]+>/g, "").replace(/\s+/g, " ").trim();
}

/**
 * Find all img tags in HTML and extract their src/data-src.
 */
export function findImages(html: string, preferDataSrc = true): string[] {
  const results: string[] = [];
  const imgRegex = /<img\s+[^>]*?>/gi;
  let match;

  while ((match = imgRegex.exec(html)) !== null) {
    const tag = match[0];
    let src = null;

    if (preferDataSrc) {
      src = attr(tag, "data-src") || attr(tag, "data-lazy-src") || attr(tag, "src");
    } else {
      src = attr(tag, "src") || attr(tag, "data-src") || attr(tag, "data-lazy-src");
    }

    if (src && !src.startsWith("data:") && !src.includes("1x1") && !src.includes("spacer")) {
      results.push(src);
    }
  }

  return results;
}

/**
 * Find all anchor tags and extract href + text.
 */
export function findLinks(
  html: string,
  filter?: (href: string, text: string) => boolean
): { href: string; text: string }[] {
  const results: { href: string; text: string }[] = [];
  const linkRegex = /<a\s+[^>]*?>[\s\S]*?<\/a>/gi;
  let match;

  while ((match = linkRegex.exec(html)) !== null) {
    const tag = match[0];
    const href = attr(tag, "href");
    const linkText = text(tag);
    if (href && (!filter || filter(href, linkText))) {
      results.push({ href, text: linkText });
    }
  }

  return results;
}

/**
 * Find elements inside a specific container.
 * Returns the inner HTML of the first matching container.
 */
export function selectOne(html: string, selector: string): string | null {
  const results = select(html, selector);
  return results.length > 0 ? results[0] : null;
}

/**
 * Find a container element and return its inner HTML.
 * Works by finding the opening tag and extracting until the matching close.
 */
export function selectInner(html: string, selector: string): string | null {
  const tagRegex = /<(\w+)([^>]*)>/i;
  const selectorParts = selector.split(/\s*>\s*/);

  let currentHtml = html;

  for (const part of selectorParts) {
    const tagMatch = tagRegex.exec(part);
    if (!tagMatch) return null;

    const tagName = tagMatch[1].toLowerCase();
    let classFilter: string | null = null;
    let idFilter: string | null = null;

    const classMatch = part.match(/\.([\w-]+)/);
    if (classMatch) classFilter = classMatch[1];

    const idMatch = part.match(/#([\w-]+)/);
    if (idMatch) idFilter = idMatch[1];

    // Find the tag with optional class/id filter
    const tagOpenRegex = new RegExp(
      `<${tagName}\\s+[^>]*?(?:class\\s*=\\s*["'][^"']*\\b${classFilter || tagName}\\b[^"']*["'])?[^>]*?>`,
      "gi"
    );

    let found = false;
    let openMatch;
    while ((openMatch = tagOpenRegex.exec(currentHtml)) !== null) {
      const tagStr = openMatch[0];

      // Check class filter
      if (classFilter) {
        const classAttr = attr(tagStr, "class") || "";
        if (!classAttr.split(/\s+/).includes(classFilter)) continue;
      }

      // Check id filter
      if (idFilter) {
        const idAttr = attr(tagStr, "id");
        if (idAttr !== idFilter) continue;
      }

      // Found matching open tag, extract inner HTML
      const startPos = openMatch.index + tagStr.length;
      const closeTag = `</${tagName}>`;
      const closePos = currentHtml.toLowerCase().indexOf(closeTag, startPos);

      if (closePos !== -1) {
        currentHtml = currentHtml.substring(startPos, closePos);
        found = true;
        break;
      }
    }

    if (!found) return null;
  }

  return currentHtml;
}

// --- Internal helpers ---

function splitElements(html: string): string[] {
  const elements: string[] = [];
  const tagRegex = /<(\w+)[^>]*?>[\s\S]*?<\/\1>/gi;
  let match;

  while ((match = tagRegex.exec(html)) !== null) {
    elements.push(match[0]);
  }

  return elements;
}

function matchesSelector(element: string, selector: string): boolean {
  const tagMatch = element.match(/^<(\w+)/i);
  if (!tagMatch) return false;

  const tagName = tagMatch[1].toLowerCase();
  const parts = selector.toLowerCase().split(/[\s.#]/);

  // First part should match tag name
  if (parts[0] && parts[0] !== tagName) return false;

  // Check class
  const classMatch = selector.match(/\.([\w-]+)/);
  if (classMatch) {
    const classAttr = attr(element, "class") || "";
    if (!classAttr.split(/\s+/).includes(classMatch[1])) return false;
  }

  // Check id
  const idMatch = selector.match(/#([\w-]+)/);
  if (idMatch) {
    const idAttr = attr(element, "id");
    if (idAttr !== idMatch[1]) return false;
  }

  return true;
}
