import re
import base64
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from .base import BaseSiteAdapter, ChapterInfo, extract_chapter_number, normalize_url


class AsuraAdapter(BaseSiteAdapter):
    """
    Adapter for Asura Scans (asurascanz.com)
    
    Asura Scans uses a custom CMS with:
    - CDN at asurascans.imagemanga.online
    - Base64 encoded image paths
    - Custom chapter URL pattern
    """
    
    name: str = "AsuraScans"
    domain: str = "asurascanz.com"
    cdn_domain: str = "asurascans.imagemanga.online"
    
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL"""
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        # Handle URLs like /manga/slug/ or /slug-chapter-X/
        parts = path.split('/')
        
        # If it's a manga page (has /manga/ prefix)
        if '/manga/' in path:
            manga_index = parts.index('manga')
            if manga_index + 1 < len(parts):
                return parts[manga_index + 1]
        
        # If it's a chapter page, extract slug from pattern {slug}-chapter-{num}
        chapter_match = re.search(r'(.+)-chapter-\d+', path)
        if chapter_match:
            return chapter_match.group(1)
        
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
        
        return f"https://{self.domain}/{manga_slug}-chapter-{chapter_str}/"
    
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter page HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        
        # Asura uses #readerarea for the image container
        reader_area = soup.select_one('#readerarea')
        if not reader_area:
            reader_area = soup
        
        # Find all img tags in the reader area
        img_tags = reader_area.find_all('img')
        
        for img in img_tags:
            url = img.get('data-src') or img.get('src')
            if url and not url.startswith('data:'):
                # Asura uses base64 encoded paths in some cases
                url = self._decode_asura_url(url)
                images.append(url)
        
        return images
    
    def _decode_asura_url(self, url: str) -> str:
        """Decode Asura's base64 encoded image URLs"""
        # Check if URL contains base64 encoded path
        if 'aW1hZ2VtYW5nYS5vbmxpbmU' in url or 'imagemanga' not in url:
            # Try to extract and decode base64 part
            try:
                # Some URLs have base64 encoded paths
                if '/' in url:
                    parts = url.split('/')
                    for i, part in enumerate(parts):
                        try:
                            decoded = base64.b64decode(part + '==').decode('utf-8')
                            if 'http' in decoded or '.' in decoded:
                                parts[i] = decoded
                        except:
                            pass
                    url = '/'.join(parts)
            except:
                pass
        
        # Ensure URL is properly formatted
        if url.startswith('//'):
            url = 'https:' + url
        elif not url.startswith('http'):
            url = f'https://{self.cdn_domain}/{url.lstrip("/")}'
        
        return url
    
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters"""
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        
        # Asura uses .eplister for chapter list
        chapter_list = soup.select('.eplister ul li a')
        
        for link in chapter_list:
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
    
    def normalize_image_url(self, url: str) -> str:
        """Normalize Asura image URL"""
        return self._decode_asura_url(url)
