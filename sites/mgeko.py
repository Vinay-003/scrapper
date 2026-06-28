import re
from typing import List, Optional
from bs4 import BeautifulSoup
from urllib.parse import urlparse

from .base import BaseSiteAdapter, ChapterInfo, extract_chapter_number, normalize_url


class MgekoAdapter(BaseSiteAdapter):
    """
    Adapter for mgeko.cc (Django-based).
    
    Structure:
    - Manga page: /manga/{slug}/
    - Chapter reader: /reader/en/{slug}-chapter-{num}-{lang}/
    - Images: imgsrv4.com CDN, inline <img> in #chapter-reader
    - Chapter list: <ul class="chapter-list"> with <li> > <a>
    - All chapters page: /manga/{slug}/all-chapters/
    """
    
    name = "Mgeko"
    domain = "mgeko.cc"
    requires_referer = True
    
    def get_manga_slug(self, url: str) -> Optional[str]:
        """Extract manga slug from URL.
        
        Supports:
        - /manga/{slug}/
        - /reader/en/{slug}-chapter-{num}-{lang}/
        - Full URLs with domain
        """
        parsed = urlparse(url)
        path = parsed.path.rstrip('/')
        
        # /manga/{slug}/
        if '/manga/' in path:
            parts = path.split('/manga/')
            if len(parts) > 1:
                slug = parts[1].strip('/')
                if slug:
                    return slug
        
        # /reader/en/{slug}-chapter-{num}-{lang}/
        if '/reader/' in path:
            parts = path.split('/reader/')
            if len(parts) > 1:
                reader_part = parts[1].strip('/')
                # Remove language prefix
                if reader_part.startswith('en/'):
                    reader_part = reader_part[3:]
                # Extract slug before -chapter-
                match = re.match(r'^(.+?)-chapter-\d+', reader_part)
                if match:
                    return match.group(1)
        
        return None
    
    async def resolve_manga_slug(self, session, url: str) -> Optional[str]:
        """Resolve correct manga slug from any URL.
        
        Chapter URLs like /reader/en/{slug}-chapter-1-eng-li/ have a different
        slug than the manga page (e.g., sword-sheath-s-child vs sword-sheath-s-child-mg1).
        Fetch the chapter page and extract the manga link from <h1><a href="/manga/{slug}/">.
        """
        parsed = urlparse(url)
        
        # Only need to resolve if it's a reader URL
        if '/reader/' not in parsed.path:
            return self.get_manga_slug(url)
        
        try:
            async with session.get(url) as resp:
                if resp.status != 200:
                    return self.get_manga_slug(url)
                html = await resp.text()
            
            soup = BeautifulSoup(html, 'html.parser')
            # The chapter page has <h1><a href="/manga/{slug}/">
            h1 = soup.find('h1')
            if h1:
                a = h1.find('a')
                if a:
                    href = a.get('href', '')
                    if '/manga/' in href:
                        slug = href.split('/manga/')[-1].strip('/')
                        if slug:
                            return slug
        except Exception:
            pass
        
        return self.get_manga_slug(url)
    
    def get_manga_url(self, slug: str) -> str:
        """Get manga page URL from slug"""
        return f"https://{self.domain}/manga/{slug}/"
    
    def get_chapter_url(self, manga_slug: str, chapter_number: float) -> str:
        """Get chapter URL from manga slug and chapter number.
        
        Note: mgeko.cc chapter URLs have a language suffix that we can't
        predict from the chapter number alone. This is a best-effort guess.
        Better to use get_available_chapters() to get actual URLs.
        """
        ch_str = str(int(chapter_number)) if chapter_number == int(chapter_number) else str(chapter_number)
        return f"https://{self.domain}/reader/en/{manga_slug}-chapter-{ch_str}-eng-li/"
    
    def get_image_urls_from_page(self, html: str) -> List[str]:
        """Extract image URLs from a chapter reader page HTML.
        
        Images are inline <img> tags inside #chapter-reader div.
        The last image is usually a credits image (skip it).
        """
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        
        # Find the chapter reader container
        reader = soup.find('div', id='chapter-reader')
        if not reader:
            reader = soup
        
        for img in reader.find_all('img'):
            src = img.get('src', '')
            if not src or src.startswith('data:'):
                continue
            
            src = normalize_url(src, f"https://{self.domain}")
            
            # Skip credits/watermark images
            if imgsrv4_url_is_credits(src):
                continue
            
            # Only include actual manga page images (imgsrv4.com CDN)
            if 'imgsrv4.com' in src:
                images.append(src)
        
        return images
    
    def get_available_chapters(self, html: str) -> List[ChapterInfo]:
        """Parse manga page HTML to get list of available chapters.
        
        Chapter links are in <ul class="chapter-list"> with <li> > <a>.
        URLs look like /reader/en/{slug}-chapter-{num}-{lang}/
        """
        soup = BeautifulSoup(html, 'html.parser')
        chapters = []
        
        # Try the chapter list first
        chapter_list = soup.find('ul', class_='chapter-list')
        if chapter_list:
            for li in chapter_list.find_all('li'):
                a = li.find('a')
                if not a:
                    continue
                
                href = a.get('href', '')
                if not href:
                    continue
                
                # Extract chapter number from URL: -chapter-{num}-
                match = re.search(r'-chapter-(\d+(?:\.\d+)?)', href)
                if match:
                    num = float(match.group(1))
                    chapters.append(ChapterInfo(
                        number=num,
                        url=normalize_url(href, f"https://{self.domain}"),
                        title=f"Chapter {num}"
                    ))
        
        # If no chapters found in chapter-list, try the select dropdown
        if not chapters:
            select = soup.find('select', id='cars')
            if select:
                for option in select.find_all('option'):
                    value = option.get('value', '')
                    text = option.get_text(strip=True)
                    if not value or not text:
                        continue
                    
                    # Skip the currently selected chapter (empty value)
                    if value == '':
                        continue
                    
                    match = re.search(r'-chapter-(\d+(?:\.\d+)?)', value)
                    if match:
                        num = float(match.group(1))
                        chapters.append(ChapterInfo(
                            number=num,
                            url=normalize_url(value, f"https://{self.domain}"),
                            title=text
                        ))
        
        # Deduplicate by chapter number
        seen = set()
        unique_chapters = []
        for ch in chapters:
            if ch.number not in seen:
                seen.add(ch.number)
                unique_chapters.append(ch)
        
        # Sort by chapter number
        unique_chapters.sort(key=lambda x: x.number)
        
        return unique_chapters
    
    def get_all_chapters_url(self, slug: str) -> str:
        """Get the URL for the all-chapters page"""
        return f"https://{self.domain}/manga/{slug}/all-chapters/"
    
    def get_search_url(self, query: str) -> str:
        """Get search URL for mgeko.cc"""
        return f"https://{self.domain}/search/?inputContent={query}"


def imgsrv4_url_is_credits(url: str) -> bool:
    """Check if an imgsrv4.com URL is a credits/watermark image."""
    lower = url.lower()
    if 'credits' in lower or 'watermark' in lower or 'logo' in lower:
        return True
    return False
