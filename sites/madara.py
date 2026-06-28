import re
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from .base import BaseSiteAdapter, ChapterInfo, extract_chapter_number, normalize_url


class MadaraAdapter(BaseSiteAdapter):
    """
    Adapter for WordPress + Madara theme sites.
    
    This covers many manga/manhwa sites including:
    - manhuaplus.com
    - manhwatop.com
    - Many other Madara-based sites
    
    To add a new Madara site, just create a new class inheriting from this
    and set the class attributes.
    """
    
    # Override these in subclasses
    name: str = "Madara"
    domain: str = ""
    manga_path: str = "/manga/"  # Path prefix for manga pages
    chapter_pattern: str = "chapter-{num}"  # Chapter URL pattern
    
    # CSS selectors for Madara theme
    READ_CONTAINER_SELECTOR = ".read-container img"
    READING_CONTENT_SELECTOR = ".reading-content .page-break img"
    READER_AREA_SELECTOR = "#readerarea img"  # Alternative Madara variant (manhuascan.us)
    CHAPTER_LIST_SELECTORS = [
        ".listing-chapters_wrap li.wp-manga-chapter a",
        ".eph-num a",  # Alternative Madara variant (manhuascan.us)
    ]
    
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL"""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        # Handle URLs like /manga/slug/ or /manga/slug/chapter-X/
        if self.manga_path in path:
            parts = path.split(self.manga_path)
            if len(parts) > 1:
                slug = parts[1].split('/')[0]
                return slug
        
        return None
    
    def get_manga_url(self, slug: str) -> str:
        """Get manga page URL from slug"""
        return f"https://{self.domain}{self.manga_path}{slug}/"
    
    def get_chapter_url(self, manga_slug: str, chapter_number: float) -> str:
        """Get chapter URL from manga slug and chapter number"""
        # Format chapter number (remove .0 for whole numbers)
        if chapter_number == int(chapter_number):
            chapter_str = str(int(chapter_number))
        else:
            chapter_str = str(chapter_number)
        
        chapter_slug = self.chapter_pattern.format(num=chapter_str)
        return f"https://{self.domain}{self.manga_path}{manga_slug}/{chapter_slug}/"
    
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter page HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        
        # Try multiple selectors
        selectors = [
            self.READ_CONTAINER_SELECTOR,
            self.READING_CONTENT_SELECTOR,
            self.READER_AREA_SELECTOR,
        ]
        
        for selector in selectors:
            img_tags = soup.select(selector)
            if img_tags:
                for img in img_tags:
                    # Get image URL - try data-src first (lazy loading), then src
                    url = img.get('data-src') or img.get('src')
                    if url and not url.startswith('data:'):
                        url = normalize_url(url, f"https://{self.domain}")
                        images.append(url)
                break
        
        return images
    
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        
        # Try multiple selectors for chapter list
        chapter_links = []
        for selector in self.CHAPTER_LIST_SELECTORS:
            chapter_links = soup.select(selector)
            if chapter_links:
                break
        
        for link in chapter_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Extract chapter number - prefer URL (more reliable)
            chapter_num = extract_chapter_number(href)
            if chapter_num is None:
                chapter_num = extract_chapter_number(text)
            
            if chapter_num is not None:
                chapters.append(ChapterInfo(
                    number=chapter_num,
                    url=normalize_url(href, f"https://{self.domain}"),
                    title=text
                ))
        
        # Sort by chapter number
        chapters.sort(key=lambda x: x.number)
        
        return chapters
    
    def get_search_url(self, query: str) -> str:
        """Get search URL for Madara sites"""
        return f"https://{self.domain}/?s={query}&post_type=wp-manga"


class ManhuaPlusAdapter(MadaraAdapter):
    """Adapter for manhuaplus.com"""
    name = "ManhuaPlus"
    domain = "manhuaplus.com"


class ManhwaTopAdapter(MadaraAdapter):
    """Adapter for manhwatop.com"""
    name = "ManhwaTop"
    domain = "manhwatop.com"


class ManhuascanAdapter(MadaraAdapter):
    """Adapter for manhuascan.us"""
    name = "Manhuascan"
    domain = "manhuascan.us"


class ManhuaPlusTopAdapter(MadaraAdapter):
    """Adapter for manhuaplus.top (v2.0 mirror)"""
    name = "ManhuaPlus V2"
    domain = "manhuaplus.top"


# Registry of Madara-based sites
MADARA_SITES = {
    "manhuaplus.com": ManhuaPlusAdapter,
    "manhwatop.com": ManhwaTopAdapter,
    "manhuascan.us": ManhuascanAdapter,
    "manhuaplus.top": ManhuaPlusTopAdapter,
}


def get_madara_adapter(domain: str) -> Optional[MadaraAdapter]:
    """Get a Madara adapter for a domain"""
    adapter_class = MADARA_SITES.get(domain)
    if adapter_class:
        return adapter_class()
    return None
