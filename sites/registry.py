from typing import Dict, Type, Optional
from .base import BaseSiteAdapter
from .madara import ManhuaPlusAdapter, ManhwaTopAdapter, MadaraAdapter, ManhuascanAdapter, ManhuaPlusTopAdapter
from .asura import AsuraAdapter
from .arena import ArenaAdapter
from .mgeko import MgekoAdapter


# Registry of all available site adapters
SITE_REGISTRY: Dict[str, Type[BaseSiteAdapter]] = {
    "arenascans.com": ArenaAdapter,
    "asurascanz.com": AsuraAdapter,
    "manhuaplus.com": ManhuaPlusAdapter,
    "manhwatop.com": ManhwaTopAdapter,
    "manhuascan.us": ManhuascanAdapter,
    "manhuaplus.top": ManhuaPlusTopAdapter,
    "mgeko.cc": MgekoAdapter,
}

# Site display names for UI
SITE_NAMES = {
    "arenascans.com": "Arenascans",
    "asurascanz.com": "Asura Scans",
    "manhuaplus.com": "ManhuaPlus",
    "manhwatop.com": "ManhwaTop",
    "manhuascan.us": "Manhuascan",
    "manhuaplus.top": "ManhuaPlus V2",
    "mgeko.cc": "Mgeko",
}


def get_adapter(domain: str) -> Optional[BaseSiteAdapter]:
    """Get a site adapter by domain"""
    adapter_class = SITE_REGISTRY.get(domain)
    if adapter_class:
        return adapter_class()
    return None


def get_all_sites() -> Dict[str, str]:
    """Get all available sites as {domain: name}"""
    return {domain: SITE_NAMES.get(domain, domain) for domain in SITE_REGISTRY}


def is_supported_site(url: str) -> bool:
    """Check if a URL is from a supported site"""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc.replace('www.', '')
    return domain in SITE_REGISTRY


def detect_site(url: str) -> Optional[str]:
    """Detect which site a URL is from"""
    from urllib.parse import urlparse
    parsed = urlparse(url)
    domain = parsed.netloc.replace('www.', '')
    
    # Check exact match
    if domain in SITE_REGISTRY:
        return domain
    
    # Check if any registered domain is contained in the URL domain
    for registered_domain in SITE_REGISTRY:
        if registered_domain in domain:
            return registered_domain
    
    return None
