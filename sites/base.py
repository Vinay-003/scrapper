from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import re


@dataclass
class ChapterInfo:
    """Information about a chapter to scrape"""
    number: float
    url: str
    title: str = ""


@dataclass
class ScrapeResult:
    """Result from scraping a chapter"""
    success: bool
    images: List[str] = None
    error: str = ""
    chapter_number: float = 0
    
    def __post_init__(self):
        if self.images is None:
            self.images = []


class BaseSiteAdapter(ABC):
    """Base class for all site adapters"""
    
    # Class attributes to override in subclasses
    name: str = "Unknown"
    domain: str = ""
    requires_referer: bool = True
    
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
        if self.requires_referer:
            self.headers["Referer"] = f"https://{self.domain}/"
    
    @abstractmethod
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL"""
        pass
    
    @abstractmethod
    def get_manga_url(self, slug: str) -> str:
        """Get manga page URL from slug"""
        pass
    
    @abstractmethod
    def get_chapter_url(self, manga_slug: str, chapter_number: float) -> str:
        """Get chapter URL from manga slug and chapter number"""
        pass
    
    @abstractmethod
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter page HTML"""
        pass
    
    @abstractmethod
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters"""
        pass
    
    async def resolve_manga_slug(self, session, url: str) -> Optional[str]:
        """Resolve the correct manga slug from any URL (including chapter URLs).
        
        Default implementation just uses get_manga_slug(). Override in adapters
        where chapter URLs have different slugs than manga page URLs.
        """
        return self.get_manga_slug(url)
    
    def normalize_image_url(self, url: str) -> str:
        """Normalize/transform image URL if needed (e.g., base64 decoding)"""
        return url
    
    def get_search_url(self, query: str) -> str:
        """Get search URL for the site (if supported)"""
        return ""
    
    def get_api_endpoints(self) -> Dict[str, str]:
        """Return any API endpoints the site uses"""
        return {}


def extract_chapter_number(text: str) -> Optional[float]:
    """Extract chapter number from text using common patterns"""
    patterns = [
        r'chapter[\s-]*(\d+(?:\.\d+)?)',
        r'ch[\s.]*(\d+(?:\.\d+)?)',
        r'(\d+(?:\.\d+)?)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return float(match.group(1))
    return None


def normalize_url(url: str, base_url: str = "") -> str:
    """Normalize a URL (handle relative URLs, protocols, etc.)"""
    if not url:
        return ""
    
    # Handle protocol-relative URLs
    if url.startswith("//"):
        return "https:" + url
    
    # Handle relative URLs
    if url.startswith("/") and base_url:
        from urllib.parse import urlparse
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}{url}"
    
    return url
