import re
import json
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from .base import BaseSiteAdapter, ChapterInfo, extract_chapter_number, normalize_url


class ThunderscansAdapter(BaseSiteAdapter):
    """
    Adapter for en-thunderscans.com
    Uses ts_reader.run() JavaScript call to extract chapter images.
    URL pattern: /comics/{slug}/ for manga pages
    Chapter pattern: /{slug}-chapter-{num}/
    """
    
    name = "Thunderscans"
    domain = "en-thunderscans.com"
    manga_path = "/comics/"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL"""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        # Handle /comics/{slug}/
        if self.manga_path in path:
            parts = path.split(self.manga_path)
            if len(parts) > 1:
                slug = parts[1].split('/')[0]
                return slug
        
        # Handle chapter URLs like /{slug}-chapter-{num}/
        chapter_match = re.match(r'^/(.+?)-chapter-\d+', path)
        if chapter_match:
            return chapter_match.group(1)
        
        return None
    
    def get_manga_url(self, slug: str) -> str:
        """Get manga page URL from slug"""
        return f"https://{self.domain}{self.manga_path}{slug}/"
    
    def get_chapter_url(self, manga_slug: str, chapter_number: float) -> str:
        """Get chapter URL from manga slug and chapter number"""
        if chapter_number == int(chapter_number):
            chapter_str = str(int(chapter_number))
        else:
            chapter_str = str(chapter_number)
        
        return f"https://{self.domain}/{manga_slug}-chapter-{chapter_str}/"
    
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter page HTML using ts_reader data"""
        # Extract ts_reader.run() data
        match = re.search(r'ts_reader\.run\(({.*?})\);', html, re.DOTALL)
        if not match:
            return []
        
        try:
            data_str = match.group(1)
            # Fix trailing commas for valid JSON
            data_str = re.sub(r',\s*}', '}', data_str)
            data_str = re.sub(r',\s*]', ']', data_str)
            data = json.loads(data_str)
            
            images = []
            for source in data.get('sources', []):
                images.extend(source.get('images', []))
            
            return images
        except (json.JSONDecodeError, KeyError):
            return []
    
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        
        # Find chapter links - try multiple selectors
        selectors = [
            '.chapter-list a[href*="chapter"]',
            'a[href*="-chapter-"]',
            '.version-chap a',
        ]
        
        chapter_links = []
        for selector in selectors:
            chapter_links = soup.select(selector)
            if chapter_links:
                break
        
        for link in chapter_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Extract chapter number from text or URL
            chapter_num = extract_chapter_number(text)
            if chapter_num is None:
                chapter_num = extract_chapter_number(href)
            
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
        """Get search URL"""
        return f"https://{self.domain}/?s={query}"


class RoliascanAdapter(BaseSiteAdapter):
    """
    Adapter for roliascan.com
    Uses mangataro.yachts CDN for chapter images.
    URL pattern: /read/{slug}/ch{num}-{id}
    """
    
    name = "Roliascan"
    domain = "roliascan.com"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }
    
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL"""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        # Handle /read/{slug}/ch{num}-{id}
        if '/read/' in path:
            parts = path.split('/read/')
            if len(parts) > 1:
                slug = parts[1].split('/')[0]
                return slug
        
        # Handle /manga/{slug}/
        if '/manga/' in path:
            parts = path.split('/manga/')
            if len(parts) > 1:
                slug = parts[1].split('/')[0]
                return slug
        
        return None
    
    def get_manga_url(self, slug: str) -> str:
        """Get manga page URL from slug"""
        return f"https://{self.domain}/manga/{slug}/"
    
    def get_chapter_url(self, manga_slug: str, chapter_number: float) -> str:
        """Get chapter URL - requires chapter ID, so we use the manga page to find it"""
        # This site uses /read/{slug}/ch{num}-{id} format
        # We need to get the chapter list first to find the ID
        return f"https://{self.domain}/manga/{manga_slug}/"
    
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter page HTML"""
        # Find mangataro.yachts URLs
        img_urls = re.findall(r'https://mangataro\.yachts/storage/chapters/[^\s\"<>\']+', html)
        
        # Clean up URLs (remove trailing quotes)
        cleaned = []
        for url in img_urls:
            url = url.rstrip("'\"")
            if url not in cleaned:
                cleaned.append(url)
        
        return cleaned
    
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        
        # Find chapter links
        chapter_links = soup.select('a[href*="/read/"]')
        
        for link in chapter_links:
            href = link.get('href', '')
            text = link.get_text(strip=True)
            
            # Extract chapter number
            chapter_num = extract_chapter_number(text)
            if chapter_num is None:
                chapter_num = extract_chapter_number(href)
            
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
        """Get search URL"""
        return f"https://{self.domain}/?s={query}"