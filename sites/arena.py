import re
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from .base import BaseSiteAdapter, ChapterInfo, extract_chapter_number, normalize_url


class ArenaAdapter(BaseSiteAdapter):
    """
    Adapter for Arenascans (arenascans.com)
    
    Uses custom CMS with CDN at cdn.arenascan.com
    """
    
    name: str = "Arenascans"
    domain: str = "arenascans.com"
    cdn_domain: str = "cdn.arenascan.com"
    
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL"""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        # Handle URLs like /manga/slug/ or /manga/slug/chapter-X/
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
        """Get chapter URL from manga slug and chapter number"""
        if chapter_number == int(chapter_number):
            chapter_str = str(int(chapter_number))
        else:
            chapter_str = str(chapter_number)
        
        return f"https://{self.domain}/manga/{manga_slug}/chapter-{chapter_str}/"
    
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter page HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        
        # Arenascans puts images in .reading-content
        reading_content = soup.select_one('.reading-content')
        if not reading_content:
            reading_content = soup
        
        # Find all img tags
        img_tags = reading_content.find_all('img')
        
        for img in img_tags:
            url = img.get('data-src') or img.get('src')
            if url and not url.startswith('data:'):
                url = normalize_url(url, f"https://{self.domain}")
                images.append(url)
        
        return images
    
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        
        # Arenascans chapter list
        chapter_links = soup.select('.eplister ul li a, .listing-chapters_wrap li a')
        
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
