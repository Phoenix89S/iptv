#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VK IPTV / M3U COLLECTOR + ULTRA CHECKER  v3.1
=============================================

Основа: полный VK-коллектор
  - обход публичной ленты VK (offset / mobile / classic)
  - извлечение ВСЕХ постов
  - встроенный M3U в тексте поста
  - вложенные плейлисты
  - БЕЗ дедупликации записей/потоков
  - диагностика (jsonl, stats, posts и т.д.)

Дополнено логикой Ultra IPTV Checker:
  - ПОЛНЫЙ список PUBLIC_INTERNET_SOURCES (iptv-org, Free-TV, dearbulut, smolnp, naggdd + RU/СНГ)
  - fuzzy-grouping + adult-фильтр
  - async HTTP-проверка + deep-ffprobe
  - scoring (latency / resolution / bitrate / codec)
  - выходные плейлисты: best / stable / online / all_with_alts
  - до 12 резервных потоков на канал
  - --discover: активный поиск альтернатив по всем публичным источникам

Зависимости:
    pip install requests beautifulsoup4 aiohttp rich

Примеры:
    # VK + все публичные источники + проверка + 12 резервов
    python vk_ultra_collector_v3.py \\
        --url "https://vk.ru/club228871429" \\
        --discover --check --deep --top 12 --workers 80 \\
        --output vk_out

    # Только публичные источники (без VK)
    python vk_ultra_collector_v3.py --discover --check --top 12 --output discover_out
"""

from __future__ import annotations

import argparse
import asyncio
import html
import json
import logging
import re
import shutil
import subprocess
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import aiohttp
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from rich.console import Console
    from rich.logging import RichHandler
    from rich.progress import (
        Progress, SpinnerColumn, BarColumn, TextColumn,
        TimeRemainingColumn, MofNCompleteColumn,
    )
    from rich.table import Table
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

# ============================================================================
# DEFAULTS
# ============================================================================

DEFAULT_URL = "https://vk.ru/club228871429"
DEFAULT_OUTPUT = "vk_iptv_output"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36 "
    "VK-IPTV-Ultra/3.1"
)

DEFAULT_TIMEOUT = 10
DEFAULT_WORKERS = 80
DEFAULT_TOP_N = 12          # резервов потоков на канал
DEFAULT_FUZZY = 0.78
FFPROBE_TIMEOUT = 12
DEFAULT_MAX_ALTS = 40       # максимум кандидатов до проверки

REQUEST_TIMEOUT = (12, 35)
PLAYLIST_TIMEOUT = (12, 45)
MAX_HTML_BYTES = 25 * 1024 * 1024
MAX_PLAYLIST_BYTES = 50 * 1024 * 1024
VK_DELAY = 0.7
PLAYLIST_DELAY = 0.15
DEFAULT_MAX_PAGES = 1000
DEFAULT_MAX_PLAYLIST_DEPTH = 2
OFFSET_STEP = 20
EMPTY_PAGE_LIMIT = 4

# ============================================================================
# PUBLIC INTERNET SOURCES — максимум + расширенный RU/СНГ
# ============================================================================

PUBLIC_INTERNET_SOURCES = [
    # === Глобальные ===
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/index.category.m3u",
    "https://iptv-org.github.io/iptv/index.country.m3u",
    "https://iptv-org.github.io/iptv/index.language.m3u",
    "https://iptv-org.github.io/iptv/languages/rus.m3u",
    "https://iptv-org.github.io/iptv/regions/cis.m3u",
    "https://iptv-org.github.io/iptv/regions/cas.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",

    # dearbulut
    "https://dearbulut.github.io/iptv/playlists/best.m3u",
    "https://dearbulut.github.io/iptv/playlists/online.m3u",
    "https://dearbulut.github.io/iptv/playlists/index.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/ru.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/uz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kg.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/tj.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/mn.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/by.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/ua.m3u",

    # === Россия (максимум площадок) ===
    "https://iptv-org.github.io/iptv/countries/ru.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVru.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVstable.m3u8",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVmir.m3u8",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPTVdonor.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/gh-pages/IPRadio.m3u",
    "https://naggdd.github.io/iptv/ru.m3u",
    "https://naggdd.github.io/iptv/music.m3u",
    "https://naggdd.github.io/iptv/cartoons.m3u",
    "https://raw.githubusercontent.com/IPTVRU2026/IPTVMIR/main/IPTV_MEGA_PLAYLIST.m3u",
    "https://myplaylists.github.io/iptv/ru.m3u",
    "https://iptv.org.ua/iptv/avto.m3u",
    "https://iptv.org.ua/iptv/avto-full.m3u",
    "https://iptv.org.ua/iptv/tva1.m3u",
    "https://iptv.org.ua/iptv/provayder.m3u",
    "https://iptv.org.ua/iptv/avtomini.m3u",

    # === Казахстан ===
    "https://iptv-org.github.io/iptv/countries/kz.m3u",
    "https://aidoseg.github.io/qazaqiptv/playlist.m3u8",
    "https://raw.githubusercontent.com/Monoloshka/iptv/main/BeeTV.m3u",
    "https://raw.githubusercontent.com/Monoloshka/iptv/main/full-iptv.m3u",
    "https://raw.githubusercontent.com/Monoloshka/iptv/main/tv.m3u",

    # === Узбекистан / Кыргызстан / Таджикистан / Монголия ===
    "https://iptv-org.github.io/iptv/countries/uz.m3u",
    "https://iptv-org.github.io/iptv/countries/kg.m3u",
    "https://iptv-org.github.io/iptv/countries/tj.m3u",
    "https://iptv-org.github.io/iptv/countries/mn.m3u",

    # === Украина + остальные СНГ ===
    "https://iptv-org.github.io/iptv/countries/ua.m3u",
    "https://myplaylists.github.io/iptv/ua.m3u",
    "https://iptv-org.github.io/iptv/countries/by.m3u",
    "https://iptv-org.github.io/iptv/countries/am.m3u",
    "https://iptv-org.github.io/iptv/countries/ge.m3u",
    "https://iptv-org.github.io/iptv/countries/az.m3u",
    "https://iptv-org.github.io/iptv/countries/md.m3u",
    "https://iptv-org.github.io/iptv/countries/tm.m3u",

    # === Категории ===
    "https://iptv-org.github.io/iptv/categories/news.m3u",
    "https://iptv-org.github.io/iptv/categories/sports.m3u",
    "https://iptv-org.github.io/iptv/categories/movies.m3u",
    "https://iptv-org.github.io/iptv/categories/entertainment.m3u",
    "https://iptv-org.github.io/iptv/categories/kids.m3u",
    "https://iptv-org.github.io/iptv/categories/music.m3u",
    "https://iptv-org.github.io/iptv/categories/documentary.m3u",
    "https://iptv-org.github.io/iptv/categories/general.m3u",
]

ADULT_KEYWORDS = [
    "xxx", "porn", "porno", "erotica", "эротика", "эротический", "adult",
    "18+", "18 +", "+18", "sex", "sexy", "hentai", "brazzers", "playboy",
    "blue hentai", "redlight", "red light", "private", "reality kings",
    "bonga", "cam4", "chaturbate", "onlyfans", "xhamster", "xvideos",
    "pornhub", "youporn", "redtube", "tube8", "spankbang", "xnxx",
    "nude", "naked", "strip", "striptease", "fetish", "bdsm", "hardcore",
    "softcore", "amateur", "milf", "teen sex", "lesbian", "gay porn",
    "busty", "big tits", "anal", "oral", "cum", "squirting",
    "русская эротика", "русское порно", "adult channel", "adult tv",
    "night club", "nightclub", "sexy night", "hot night", "private gold",
    "dorcel", "private platinum", "vivid", "hustler", "penthouse",
]

WALL_RE = re.compile(r"(?:https?://[^/\s]+)?/(?:wall|w=wall)(-?\d+_\d+)", re.I)
WALL_ID_RE = re.compile(r"(?:wall(?:_|%5F)|w=wall(?:_|%5F))(-?\d+_\d+)", re.I)
URL_RE = re.compile(r"""(?ix)(?:https?://|//)[^\s<>"'\\]+""")
ATTR_RE = re.compile(r"""(?is)([a-zA-Z][a-zA-Z0-9_-]*)\s*=\s*(?:"([^"]*)"|'([^']*)')""")
QUALITY_RE = re.compile(
    r"[\s\-_]*(4[Kk]|UHD|FHD|HD|SD|HEVC|H\.?265|H\.?264|AVC|"
    r"50[Ff]ps|60[Ff]ps|\d{3,4}[pP]|HQ|LQ|Full\s*HD)[\s\-_]*", re.I)
EMOJI_RE = re.compile(
    "[" "\U0001F600-\U0001F64F" "\U0001F300-\U0001F5FF" "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF" "\U00002702-\U000027B0" "\U000024C2-\U0001F251" "]+",
    flags=re.UNICODE)
GEO_RE = re.compile(
    r"[\s\-_]*(Geo[\s\-]?blocked|Only\s*(RU|BY|KZ|UA|EU|US)|\[(RU|BY|KZ|UA)\])[\s\-_]*", re.I)
BRACKETS_RE = re.compile(r"[\(\[\{].*?[\)\]\}]")

M3U_EXTENSIONS = (".m3u", ".m3u8")
DIRECT_STREAM_EXTENSIONS = (
    ".m3u8", ".m3u", ".ts", ".m4s", ".aac", ".mp3", ".mp4", ".mkv", ".flv", ".webm", ".mpd",
)
DIRECT_STREAM_MARKERS = (
    "/hls/", "/hls?", "/live/", "/live?", "/stream/", "/stream?", "/playlist/",
    "/manifest", "/chunklist", "format=m3u8", "type=m3u8", "output=m3u8",
)
NON_STREAM_HOST_MARKERS = (
    "vk.ru", "vk.com", "m.vk.com", "youtube.com", "youtu.be", "rutube.ru",
    "t.me", "telegram.me", "instagram.com", "facebook.com", "twitter.com",
    "x.com", "github.com", "gitlab.com", "google.com", "yandex.ru",
)

console = Console() if HAS_RICH else None
LOG = logging.getLogger("vk_ultra")


def setup_logging(output_dir: Path, verbose: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    LOG.setLevel(logging.DEBUG)
    LOG.handlers.clear()
    LOG.propagate = False
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    fh = logging.FileHandler(output_dir / "errors.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(formatter)
    LOG.addHandler(fh)
    LOG.addHandler(ch)
    if HAS_RICH and verbose:
        LOG.addHandler(RichHandler(console=console, rich_tracebacks=True))


@dataclass
class Post:
    post_id: str
    url: str
    text: str
    html_fragment: str = ""
    page_url: str = ""
    discovered_by: str = ""


@dataclass
class Record:
    sequence: int
    name: str
    url: str
    source_type: str
    source_page: str = ""
    source_post: str = ""
    playlist_url: str = ""
    playlist_depth: int = 0
    extinf: str = ""
    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""
    raw_text: str = ""
    latency_ms: float = 99999.0
    http_ok: bool = False
    status_code: int = 0
    resolution: str = ""
    width: int = 0
    height: int = 0
    codec: str = ""
    bitrate_kbps: float = 0.0
    score: float = 0.0
    error: str = ""


@dataclass
class StreamInfo:
    url: str
    source: str = ""
    latency_ms: float = 99999.0
    http_ok: bool = False
    status_code: int = 0
    resolution: str = ""
    width: int = 0
    height: int = 0
    codec: str = ""
    bitrate_kbps: float = 0.0
    score: float = 0.0
    error: str = ""


@dataclass
class Channel:
    name: str
    group: str = "Undefined"
    logo: str = ""
    tvg_id: str = ""
    streams: list[StreamInfo] = field(default_factory=list)

    @property
    def best_stream(self) -> Optional[StreamInfo]:
        alive = [s for s in self.streams if s.http_ok]
        return max(alive, key=lambda s: s.score) if alive else None

    @property
    def alive_streams(self) -> list[StreamInfo]:
        return sorted(
            [s for s in self.streams if s.http_ok],
            key=lambda s: s.score,
            reverse=True,
        )


@dataclass
class CollectorStats:
    pages_requested: int = 0
    pages_ok: int = 0
    pages_failed: int = 0
    posts_found: int = 0
    posts_processed: int = 0
    urls_found_in_posts: int = 0
    all_links_found: int = 0
    direct_stream_urls: int = 0
    non_stream_links: int = 0
    playlist_urls_found: int = 0
    playlists_requested: int = 0
    playlists_ok: int = 0
    playlists_failed: int = 0
    playlists_not_m3u: int = 0
    playlist_records: int = 0
    nested_playlist_urls: int = 0
    total_records: int = 0
    errors: int = 0
    repeated_playlist_urls: int = 0
    repeated_post_ids: int = 0
    streams_checked: int = 0
    streams_alive: int = 0
    channels_after_fuzzy: int = 0
    public_sources_loaded: int = 0


def normalize_name(name: str) -> str:
    if not name:
        return ""
    n = name.strip()
    n = EMOJI_RE.sub(" ", n)
    n = QUALITY_RE.sub(" ", n)
    n = GEO_RE.sub(" ", n)
    n = BRACKETS_RE.sub(" ", n)
    n = re.sub(r"[^\w\sа-яА-ЯёЁ\-]", " ", n, flags=re.UNICODE)
    n = re.sub(r"\s+", " ", n).strip().lower()
    return n


def fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def find_best_key(name: str, existing_keys: dict[str, str], threshold: float) -> Optional[str]:
    norm = normalize_name(name)
    if not norm:
        return None
    if norm in existing_keys:
        return norm
    best_key, best_score = None, 0.0
    for key in existing_keys:
        score = fuzzy_ratio(norm, key)
        if score > best_score and score >= threshold:
            best_score = score
            best_key = key
    return best_key


def is_adult(name: str, group: str = "") -> bool:
    name_l = (name or "").lower()
    norm = normalize_name(name)
    if any(kw in name_l or kw in norm for kw in ADULT_KEYWORDS):
        return True
    group_l = (group or "").lower()
    return any(kw in group_l for kw in ("adult", "xxx", "erotica", "эротика", "18+", "porn"))


def clean_url(raw: str) -> str:
    value = html.unescape(str(raw or "")).strip()
    value = value.replace("&amp;", "&").replace("\\/", "/")
    value = value.strip("\"'<>")
    while value and value[-1] in ".,;:)]}>":
        value = value[:-1]
    while value.startswith("(") and value.endswith(")"):
        value = value[1:-1].strip()
    return value


def normalize_protocol_relative(url: str, base_url: str) -> str:
    if url.startswith("//"):
        base = urlparse(base_url)
        return f"{base.scheme or 'https'}:{url}"
    return url


def is_http_url(url: str) -> bool:
    try:
        return urlparse(url).scheme.lower() in {"http", "https"}
    except Exception:
        return False


def canonical_page_url(url: str) -> str:
    value = clean_url(url)
    try:
        p = urlparse(value)
        return urlunparse((p.scheme.lower(), p.netloc.lower(), p.path or "/", "", p.query, ""))
    except Exception:
        return value


def add_query_param(url: str, key: str, value: str | int) -> str:
    p = urlparse(url)
    query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() != key.lower()]
    query.append((key, str(value)))
    return urlunparse((p.scheme, p.netloc, p.path, p.params, urlencode(query), p.fragment))


def extract_urls(text: str, base_url: str = "") -> list[str]:
    source = html.unescape(str(text or ""))
    result: list[str] = []
    for match in URL_RE.finditer(source):
        value = clean_url(match.group(0))
        value = normalize_protocol_relative(value, base_url)
        if is_http_url(value):
            result.append(value)
    for match in re.finditer(r"""(?is)\b(?:href|src)\s*=\s*(?:"([^"]+)"|'([^']+)')""", source):
        value = html.unescape(match.group(1) or match.group(2) or "").strip()
        if value.startswith("//"):
            value = normalize_protocol_relative(value, base_url)
        elif value.startswith("/") and base_url:
            value = urljoin(base_url, value)
        value = clean_url(value)
        if is_http_url(value):
            result.append(value)
    return result


def looks_like_playlist_url(url: str) -> bool:
    try:
        p = urlparse(url)
        path, query = p.path.lower(), p.query.lower()
    except Exception:
        path, query = url.lower(), ""
    if any(path.endswith(ext) for ext in M3U_EXTENSIONS):
        return True
    if "/index.m3u8" in path or "/index.m3u" in path:
        return True
    return any(m in query for m in (
        "format=m3u", "type=m3u", "output=m3u", "playlist=m3u",
        "format=m3u8", "type=m3u8",
    ))


def looks_like_direct_stream(url: str) -> bool:
    if not is_http_url(url):
        return False
    if looks_like_playlist_url(url):
        return True
    p = urlparse(url)
    host, path, query = p.netloc.lower(), p.path.lower(), p.query.lower()
    if any(m in host for m in NON_STREAM_HOST_MARKERS):
        return False
    if any(path.endswith(ext) for ext in DIRECT_STREAM_EXTENSIONS):
        return True
    if any(m in path or m in query for m in DIRECT_STREAM_MARKERS):
        return True
    return any(x in query for x in (
        "stream=", "channel=", "channel_id=", "stream_id=", "manifest=", "hls=", "dash=",
    ))


def looks_like_embedded_m3u(text: str) -> bool:
    sample = (text or "")[:100_000].lstrip("\ufeff \t\r\n")
    if not sample:
        return False
    upper = sample.upper()
    return (
        upper.startswith("#EXTM3U")
        or "#EXTINF:" in upper
        or re.search(r"(?im)^\s*#EXTINF", sample) is not None
    )


def html_to_text(fragment: str) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)


def normalize_text(text: str) -> str:
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in (text or "").splitlines()]
    return "\n".join(x for x in lines if x)


def post_id_from(value: str) -> str:
    match = WALL_ID_RE.search(html.unescape(str(value or "")))
    return match.group(1) if match else ""


def post_url_from_id(post_id: str, page_url: str) -> str:
    return urljoin(page_url, f"/wall{post_id}")


def safe_fragment_text(node) -> str:
    try:
        return normalize_text(html_to_text(str(node)))
    except Exception:
        return ""


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4, connect=4, read=4, status=4, backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False, respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=32)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    return session


def _wall_ids_in_node(node) -> set[str]:
    fragment = str(node)
    ids = set(WALL_ID_RE.findall(fragment))
    for attr_name in ("data-post-id", "data-postid", "data-post_id"):
        value = node.get(attr_name) if hasattr(node, "get") else None
        if value and re.fullmatch(r"-?\d+_\d+", str(value).strip()):
            ids.add(str(value).strip())
    return ids


def _candidate_containers_for_wall_anchor(anchor):
    candidates = []
    current = anchor
    for level in range(1, 12):
        current = current.parent
        if current is None:
            break
        ids = _wall_ids_in_node(current)
        if len(ids) == 1:
            candidates.append((level, current, ids))
            if len(safe_fragment_text(current)) > 25000:
                break
    return candidates


def extract_posts(page_html: str, page_url: str, discovered_by: str = "") -> list[Post]:
    soup = BeautifulSoup(page_html, "html.parser")
    candidates: dict[str, list[tuple[int, object]]] = {}

    for attr_name in ("data-post-id", "data-postid", "data-post_id"):
        for node in soup.find_all(attrs={attr_name: True}):
            pid = str(node.get(attr_name) or "").strip()
            if re.fullmatch(r"-?\d+_\d+", pid):
                candidates.setdefault(pid, []).append((0, node))

    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "")
        pid = post_id_from(href)
        if not pid:
            continue
        for level, node, ids in _candidate_containers_for_wall_anchor(a):
            if pid in ids:
                candidates.setdefault(pid, []).append((level, node))
                if len(candidates[pid]) >= 5:
                    break

    all_pids = set(WALL_ID_RE.findall(page_html))
    for pid in all_pids:
        candidates.setdefault(pid, [])

    found: list[Post] = []
    for pid in sorted(candidates.keys(), key=lambda x: int(x.split("_")[-1])):
        options = candidates[pid]
        best_node, best_score = None, None
        seen_nodes = set()
        for level, node in options:
            marker = id(node)
            if marker in seen_nodes:
                continue
            seen_nodes.add(marker)
            text = safe_fragment_text(node)
            fragment = str(node)
            if not text and not URL_RE.search(fragment):
                continue
            ids = _wall_ids_in_node(node)
            one_post_bonus = 100000 if len(ids) == 1 else 0
            text_score = min(len(text), 15000)
            size_penalty = max(0, len(fragment) - 30000)
            score = one_post_bonus + text_score - size_penalty - level * 100
            if best_score is None or score > best_score:
                best_score = score
                best_node = node
        if best_node is not None:
            fragment = str(best_node)
            text = safe_fragment_text(best_node)
        else:
            fragment, text = "", ""
        found.append(Post(
            post_id=pid,
            url=post_url_from_id(pid, page_url),
            text=text,
            html_fragment=fragment,
            page_url=page_url,
            discovered_by=discovered_by or "wall-id",
        ))

    if not found:
        for attr_name in ("data-post-id", "data-postid", "data-post_id"):
            for node in soup.find_all(attrs={attr_name: True}):
                pid = str(node.get(attr_name) or "").strip()
                if pid:
                    found.append(Post(
                        post_id=pid,
                        url=post_url_from_id(pid, page_url),
                        text=safe_fragment_text(node),
                        html_fragment=str(node),
                        page_url=page_url,
                        discovered_by="data-post-id",
                    ))

    result, seen_ids = [], set()
    for post in found:
        if post.post_id not in seen_ids:
            seen_ids.add(post.post_id)
            result.append(post)
    return result


def infer_post_name(text: str, url: str) -> str:
    lines = [re.sub(r"\s+", " ", line).strip() for line in (text or "").splitlines() if line.strip()]
    target = clean_url(url)
    for i, line in enumerate(lines):
        if target in line or url in line:
            if i > 0:
                prev = lines[i - 1]
                if not is_http_url(prev) and not prev.startswith("#") and len(prev) <= 300:
                    return prev
    marker = re.compile(r"(?i)^(?:канал|название|channel|tv|name)\s*[:\-]\s*(.+)$")
    for line in lines:
        m = marker.match(line)
        if m:
            return m.group(1).strip()[:300]
    for line in lines:
        if not is_http_url(line) and not line.startswith("#") and len(line) > 1:
            return line[:300]
    return ""


def post_urls(post: Post, page_url: str) -> list[str]:
    source = post.html_fragment or post.text
    urls = extract_urls(source, page_url)
    if not urls and post.text:
        urls = extract_urls(post.text, page_url)
    return [u for u in urls if clean_url(u) != clean_url(post.url)]


def pagination_links(page_html: str, page_url: str) -> list[str]:
    soup = BeautifulSoup(page_html or "", "html.parser")
    result = []
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        text = safe_fragment_text(a).lower()
        absolute = clean_url(urljoin(page_url, href))
        if not is_http_url(absolute):
            continue
        query = urlparse(absolute).query.lower()
        is_page = any(m in query for m in (
            "offset=", "page=", "start_from=", "cursor=", "section=", "w=wall",
        ))
        is_more = any(m in text for m in (
            "далее", "ещё", "еще", "показать ещё", "показать еще",
            "загрузить ещё", "загрузить еще", "next", "more",
        ))
        if is_page or is_more:
            result.append(absolute)
    return result


def generate_page_variants(base_url: str, offset: int) -> list[str]:
    variants = [add_query_param(base_url, "offset", offset)]
    if offset:
        variants.append(add_query_param(base_url, "page", max(1, offset // OFFSET_STEP + 1)))
    p = urlparse(base_url)
    if p.netloc.lower() == "vk.ru":
        for host in ("m.vk.ru", "vk.com"):
            mobile = urlunparse((p.scheme or "https", host, p.path, p.params, p.query, p.fragment))
            variants.append(add_query_param(mobile, "offset", offset))
    out, seen = [], set()
    for url in variants:
        key = canonical_page_url(url)
        if key not in seen:
            seen.add(key)
            out.append(url)
    return out


def download_text(
    session: requests.Session,
    url: str,
    timeout,
    max_bytes: int,
) -> tuple[Optional[str], str, Optional[str], str]:
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True, stream=True)
        final_url = clean_url(response.url or url)
        content_type = response.headers.get("Content-Type", "")
        if response.status_code >= 400:
            response.close()
            return None, content_type, f"HTTP {response.status_code}", final_url
        chunks, total = [], 0
        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > max_bytes:
                response.close()
                return None, content_type, f"response exceeds {max_bytes} bytes", final_url
            chunks.append(chunk)
        response.close()
        raw = b"".join(chunks)
        for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
            try:
                return raw.decode(encoding), content_type, None, final_url
            except UnicodeDecodeError:
                pass
        return raw.decode("utf-8", errors="replace"), content_type, None, final_url
    except Exception as exc:
        return None, "", f"{type(exc).__name__}: {exc}", url


def is_m3u_content(text: str, content_type: str, url: str = "") -> bool:
    sample = (text or "")[:250000].lstrip("\ufeff \t\r\n")
    ct = (content_type or "").lower()
    if sample.startswith("#EXTM3U") or "#EXTINF:" in sample.upper():
        return True
    if any(x in ct for x in ("mpegurl", "x-mpegurl", "application/vnd.apple.mpegurl")):
        return True
    return any(urlparse(url or "").path.lower().endswith(x) for x in M3U_EXTENSIONS)


def parse_extinf_attributes(extinf: str) -> dict[str, str]:
    attrs = {}
    for match in ATTR_RE.finditer(extinf or ""):
        key = match.group(1).lower()
        value = match.group(2) if match.group(2) is not None else match.group(3)
        attrs[key] = html.unescape(value or "")
    return attrs


def extinf_display_name(extinf: str) -> str:
    return extinf.split(",", 1)[1].strip() if "," in (extinf or "") else ""


def parse_m3u(
    text: str,
    playlist_url: str,
    source_page: str,
    source_post: str,
    depth: int,
    post_text: str,
) -> tuple[list[Record], list[str]]:
    lines = (text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    records, nested = [], []
    current_extinf, attrs = "", {}
    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue
        if line.upper().startswith("#EXTINF"):
            current_extinf = line
            attrs = parse_extinf_attributes(line)
            continue
        if line.startswith("#"):
            continue
        if line.startswith("//"):
            line = normalize_protocol_relative(line, playlist_url)
        if not is_http_url(line):
            continue
        stream_url = clean_url(line)
        display_name = attrs.get("tvg-name") or extinf_display_name(current_extinf) or ""
        output_extinf = current_extinf if current_extinf else f"#EXTINF:-1,{display_name or 'Unknown'}"
        records.append(Record(
            sequence=0,
            name=display_name,
            url=stream_url,
            source_type="playlist_record",
            source_page=source_page,
            source_post=source_post,
            playlist_url=playlist_url,
            playlist_depth=depth,
            extinf=output_extinf,
            tvg_id=attrs.get("tvg-id", ""),
            tvg_name=attrs.get("tvg-name", ""),
            tvg_logo=attrs.get("tvg-logo", ""),
            group_title=attrs.get("group-title", ""),
            raw_text=post_text,
        ))
        if looks_like_playlist_url(stream_url):
            nested.append(stream_url)
        current_extinf, attrs = "", {}
    return records, nested


def run_ffprobe(url: str, timeout: int = FFPROBE_TIMEOUT) -> dict:
    if not shutil.which("ffprobe"):
        return {}
    cmd = [
        "ffprobe", "-v", "quiet", "-print_format", "json",
        "-show_streams", "-show_format",
        "-probesize", "500000", "-analyzeduration", "2000000",
        "-timeout", str(timeout * 1_000_000), url,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout + 2)
        if result.returncode != 0:
            return {"error": (result.stderr or "ffprobe failed")[:100]}
        data = json.loads(result.stdout)
        video = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
        fmt = data.get("format", {})
        info = {}
        if video:
            info["width"] = int(video.get("width") or 0)
            info["height"] = int(video.get("height") or 0)
            info["codec"] = video.get("codec_name", "")
            info["resolution"] = f"{info['width']}x{info['height']}" if info["width"] else ""
        if fmt.get("bit_rate"):
            try:
                info["bitrate_kbps"] = round(int(fmt["bit_rate"]) / 1000, 1)
            except Exception:
                pass
        return info
    except subprocess.TimeoutExpired:
        return {"error": "ffprobe timeout"}
    except Exception as e:
        return {"error": str(e)[:80]}


def calculate_score(stream: StreamInfo | Record) -> float:
    if not stream.http_ok:
        return 0.0
    lat = stream.latency_ms
    lat_score = 40 if lat < 200 else 35 if lat < 500 else 25 if lat < 1000 else 15 if lat < 2000 else 5
    h = stream.height
    res_score = 40 if h >= 2160 else 35 if h >= 1080 else 25 if h >= 720 else 15 if h >= 480 else 8 if h > 0 else 10
    br = stream.bitrate_kbps
    br_score = 15 if br > 5000 else 12 if br > 2500 else 8 if br > 1000 else 4 if br > 0 else 5
    codec_bonus = 5 if stream.codec in ("h264", "avc", "hevc", "h265") else 0
    return lat_score + res_score + br_score + codec_bonus


async def check_http(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float,
    headers: dict,
    ssl_verify: bool,
) -> tuple[bool, int, float, str]:
    start = time.perf_counter()
    try:
        async with session.get(
            url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout),
            ssl=ssl_verify, allow_redirects=True,
        ) as resp:
            await resp.content.read(1024)
            latency = (time.perf_counter() - start) * 1000
            ok = 200 <= resp.status < 400
            return ok, resp.status, latency, "" if ok else f"HTTP {resp.status}"
    except asyncio.TimeoutError:
        return False, 0, (time.perf_counter() - start) * 1000, "timeout"
    except aiohttp.ClientError as e:
        return False, 0, (time.perf_counter() - start) * 1000, str(e)[:80]
    except Exception as e:
        return False, 0, (time.perf_counter() - start) * 1000, str(e)[:80]


async def check_one(session, item: StreamInfo | Record, timeout, headers, deep, ssl_verify, sem):
    async with sem:
        ok, status, latency, err = await check_http(session, item.url, timeout, headers, ssl_verify)
        item.http_ok = ok
        item.status_code = status
        item.latency_ms = latency
        item.error = err
        if ok and deep:
            loop = asyncio.get_running_loop()
            probe = await loop.run_in_executor(None, run_ffprobe, item.url)
            if "error" not in probe:
                item.width = probe.get("width", 0)
                item.height = probe.get("height", 0)
                item.resolution = probe.get("resolution", "")
                item.codec = probe.get("codec", "")
                item.bitrate_kbps = probe.get("bitrate_kbps", 0.0)
            else:
                item.error = probe.get("error", "")
        item.score = calculate_score(item)


async def process_streams(
    items: list,
    workers: int,
    timeout: float,
    deep: bool,
    user_agent: str,
    ssl_verify: bool,
) -> None:
    if not items:
        return
    headers = {"User-Agent": user_agent}
    sem = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, ttl_dns_cache=300, ssl=ssl_verify)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout + 5)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
        if HAS_RICH:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task_id = progress.add_task("Checking streams...", total=len(items))

                async def wrapped(item):
                    await check_one(session, item, timeout, headers, deep, ssl_verify, sem)
                    progress.advance(task_id)

                await asyncio.gather(*(wrapped(i) for i in items))
        else:
            await asyncio.gather(
                *(check_one(session, i, timeout, headers, deep, ssl_verify, sem) for i in items)
            )


class Collector:
    def __init__(
        self,
        page_url: str,
        output_dir: Path,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_playlist_depth: int = DEFAULT_MAX_PLAYLIST_DEPTH,
    ):
        self.page_url = clean_url(page_url) if page_url else ""
        self.output_dir = output_dir
        self.max_pages = max_pages
        self.max_playlist_depth = max_playlist_depth
        self.session = build_session()
        self.stats = CollectorStats()
        self.posts: list[Post] = []
        self.records: list[Record] = []
        self.playlist_events: list[dict] = []
        self.playlist_occurrences: list[dict] = []
        self.all_post_links: list[dict] = []
        self.seen_post_ids: set[str] = set()
        self.active_playlist_chain: list[str] = []

    def fetch_page(self, url: str) -> Optional[str]:
        self.stats.pages_requested += 1
        try:
            response = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if response.status_code >= 400:
                self.stats.pages_failed += 1
                self.stats.errors += 1
                LOG.error("PAGE HTTP %s: %s", response.status_code, url)
                return None
            if len(response.content) > MAX_HTML_BYTES:
                self.stats.pages_failed += 1
                self.stats.errors += 1
                LOG.error("PAGE TOO LARGE: %s", url)
                return None
            response.encoding = response.encoding or "utf-8"
            self.stats.pages_ok += 1
            return response.text
        except Exception as exc:
            self.stats.pages_failed += 1
            self.stats.errors += 1
            LOG.error("PAGE ERROR: %s | %s", url, exc)
            return None

    def collect_playlist(self, url: str, source_post: str, depth: int, post_text: str) -> None:
        url = clean_url(url)
        self.playlist_occurrences.append({"url": url, "depth": depth, "source_post": source_post})
        if sum(1 for x in self.playlist_occurrences if x["url"] == url) > 1:
            self.stats.repeated_playlist_urls += 1
        if depth > self.max_playlist_depth:
            self.playlist_events.append({
                "url": url, "depth": depth, "status": "max_depth",
                "records": 0, "source_post": source_post,
            })
            return
        if url in self.active_playlist_chain:
            self.playlist_events.append({
                "url": url, "depth": depth, "status": "cycle",
                "records": 0, "source_post": source_post,
            })
            return
        self.active_playlist_chain.append(url)
        try:
            if len(self.active_playlist_chain) > 1:
                time.sleep(PLAYLIST_DELAY)
            LOG.info("DOWNLOAD PLAYLIST depth=%d: %s", depth, url)
            text, content_type, error, final_url = download_text(
                self.session, url, PLAYLIST_TIMEOUT, MAX_PLAYLIST_BYTES,
            )
            self.stats.playlists_requested += 1
            if error:
                self.stats.playlists_failed += 1
                self.stats.errors += 1
                LOG.error("PLAYLIST ERROR: %s | %s", url, error)
                self.playlist_events.append({
                    "url": url, "final_url": final_url, "depth": depth,
                    "status": "download_error", "records": 0,
                    "source_post": source_post, "error": error,
                })
                return
            if not is_m3u_content(text or "", content_type, final_url):
                self.stats.playlists_not_m3u += 1
                LOG.warning("NOT M3U: %s | ct=%s", url, content_type)
                self.playlist_events.append({
                    "url": url, "final_url": final_url, "depth": depth,
                    "status": "not_m3u", "records": 0,
                    "source_post": source_post, "content_type": content_type,
                })
                return
            self.stats.playlists_ok += 1
            records, nested = parse_m3u(
                text or "", final_url or url, self.page_url or "public",
                source_post, depth, post_text,
            )
            self.stats.playlist_records += len(records)
            self.records.extend(records)
            self.playlist_events.append({
                "url": url, "final_url": final_url, "depth": depth,
                "status": "parsed", "records": len(records), "nested": len(nested),
                "source_post": source_post, "content_type": content_type,
            })
            LOG.info("PLAYLIST PARSED: records=%d nested=%d", len(records), len(nested))
            for nested_url in nested:
                self.stats.nested_playlist_urls += 1
                self.collect_playlist(nested_url, source_post, depth + 1, post_text)
        finally:
            self.active_playlist_chain.pop()

    def process_post(self, post: Post) -> None:
        self.stats.posts_processed += 1
        if looks_like_embedded_m3u(post.text):
            LOG.info("EMBEDDED M3U in post %s", post.post_id)
            records, nested = parse_m3u(
                post.text, post.url, post.page_url or self.page_url,
                post.url, 0, post.text,
            )
            self.stats.playlist_records += len(records)
            self.records.extend(records)
            for nested_url in nested:
                self.stats.nested_playlist_urls += 1
                self.collect_playlist(nested_url, post.url, 1, post.text)

        urls = post_urls(post, self.page_url)
        self.stats.urls_found_in_posts += len(urls)
        for url in urls:
            self.stats.all_links_found += 1
            if looks_like_playlist_url(url):
                self.stats.playlist_urls_found += 1
                self.all_post_links.append({
                    "post_id": post.post_id, "post_url": post.url,
                    "url": url, "kind": "playlist",
                })
                self.collect_playlist(url, post.url, 0, post.text)
                continue
            if looks_like_direct_stream(url):
                self.stats.direct_stream_urls += 1
                name = infer_post_name(post.text, url)
                self.records.append(Record(
                    sequence=0, name=name, url=url,
                    source_type="direct_post_stream",
                    source_page=post.page_url or self.page_url,
                    source_post=post.url, raw_text=post.text,
                ))
                self.all_post_links.append({
                    "post_id": post.post_id, "post_url": post.url,
                    "url": url, "kind": "direct_stream",
                })
            else:
                self.stats.non_stream_links += 1
                self.all_post_links.append({
                    "post_id": post.post_id, "post_url": post.url,
                    "url": url, "kind": "other",
                })

    def crawl_group(self) -> None:
        if not self.page_url:
            LOG.info("No VK URL — skipping crawl")
            return
        LOG.info("=" * 60)
        LOG.info("FULL GROUP CRAWL: %s", self.page_url)
        LOG.info("MAX PAGES: %d", self.max_pages)
        LOG.info("=" * 60)

        queue: deque[tuple[str, str]] = deque()
        queued_pages: set[str] = set()

        def enqueue(url: str, reason: str) -> None:
            url = clean_url(url)
            if not is_http_url(url):
                return
            key = canonical_page_url(url)
            if key in queued_pages:
                return
            queued_pages.add(key)
            queue.append((url, reason))

        enqueue(self.page_url, "initial")
        next_offset = 0
        empty_rounds = 0
        processed_pages = 0

        while queue and processed_pages < self.max_pages:
            current_url, reason = queue.popleft()
            processed_pages += 1
            if processed_pages > 1:
                time.sleep(VK_DELAY)
            LOG.info("PAGE %d/%d | reason=%s | %s", processed_pages, self.max_pages, reason, current_url)
            page = self.fetch_page(current_url)
            if page is None:
                continue

            before_records = len(self.records)
            found_posts = extract_posts(page, current_url, reason)
            new_posts_this_page = 0
            for post in found_posts:
                if post.post_id in self.seen_post_ids:
                    self.stats.repeated_post_ids += 1
                    continue
                self.seen_post_ids.add(post.post_id)
                self.posts.append(post)
                self.stats.posts_found += 1
                new_posts_this_page += 1
                self.process_post(post)

            added = len(self.records) - before_records
            LOG.info(
                "POSTS: detected=%d new=%d total=%d | RECORDS +%d total=%d",
                len(found_posts), new_posts_this_page, len(self.posts),
                added, len(self.records),
            )

            for link in pagination_links(page, current_url):
                enqueue(link, "vk-pagination")
            next_offset += OFFSET_STEP
            for variant in generate_page_variants(self.page_url, next_offset):
                enqueue(variant, f"offset={next_offset}")

            if new_posts_this_page == 0:
                empty_rounds += 1
            else:
                empty_rounds = 0
            if empty_rounds >= EMPTY_PAGE_LIMIT:
                LOG.info("STOP: %d consecutive pages without NEW post_id.", EMPTY_PAGE_LIMIT)
                break

        self.stats.total_records = len(self.records)
        LOG.info(
            "CRAWL FINISHED: pages=%d posts=%d records=%d",
            processed_pages, len(self.posts), len(self.records),
        )

    def load_public_sources(self, sources: list[str]) -> None:
        LOG.info("DISCOVER: loading %d public sources (max RU/CIS)...", len(sources))
        loaded = 0
        for src in sources:
            try:
                text, ct, err, final = download_text(
                    self.session, src, PLAYLIST_TIMEOUT, MAX_PLAYLIST_BYTES,
                )
                if err or not is_m3u_content(text or "", ct, final):
                    LOG.warning("DISCOVER skip %s: %s", src, err or "not m3u")
                    continue
                records, _ = parse_m3u(
                    text or "", final or src, "public", "discover", 0, "",
                )
                filtered = [r for r in records if not is_adult(r.name, r.group_title)]
                self.records.extend(filtered)
                loaded += 1
                LOG.info("DISCOVER %s → %d records", src, len(filtered))
            except Exception as e:
                LOG.error("DISCOVER error %s: %s", src, e)
        self.stats.public_sources_loaded = loaded
        LOG.info("DISCOVER done: %d sources, total records %d", loaded, len(self.records))

    def save_raw(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for i, rec in enumerate(self.records, 1):
            rec.sequence = i

        with (self.output_dir / "combined.m3u").open("w", encoding="utf-8", newline="\n") as f:
            f.write('#EXTM3U x-no-dedup="1" x-top-alts="12"\n')
            for rec in self.records:
                extinf = rec.extinf.strip()
                if not extinf:
                    attrs = []
                    if rec.tvg_id:
                        attrs.append(f'tvg-id="{rec.tvg_id}"')
                    if rec.tvg_name:
                        attrs.append(f'tvg-name="{rec.tvg_name}"')
                    if rec.tvg_logo:
                        attrs.append(f'tvg-logo="{rec.tvg_logo}"')
                    if rec.group_title:
                        attrs.append(f'group-title="{rec.group_title}"')
                    name = rec.name or rec.tvg_name or "Unknown"
                    prefix = "#EXTINF:-1" + (" " + " ".join(attrs) if attrs else "")
                    extinf = f"{prefix},{name}"
                f.write(extinf + "\n")
                f.write(rec.url + "\n")

        with (self.output_dir / "records.jsonl").open("w", encoding="utf-8") as f:
            for rec in self.records:
                f.write(json.dumps(asdict(rec), ensure_ascii=False) + "\n")

        with (self.output_dir / "posts.jsonl").open("w", encoding="utf-8") as f:
            for post in self.posts:
                f.write(json.dumps(asdict(post), ensure_ascii=False) + "\n")

        with (self.output_dir / "posts_urls.txt").open("w", encoding="utf-8", newline="\n") as f:
            for post in self.posts:
                f.write(post.url + "\n")

        with (self.output_dir / "links.jsonl").open("w", encoding="utf-8") as f:
            for item in self.all_post_links:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        with (self.output_dir / "playlists.jsonl").open("w", encoding="utf-8") as f:
            for item in self.playlist_events:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        with (self.output_dir / "playlists_found.jsonl").open("w", encoding="utf-8") as f:
            for item in self.playlist_occurrences:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

        with (self.output_dir / "playlists.txt").open("w", encoding="utf-8", newline="\n") as f:
            for item in self.playlist_occurrences:
                f.write(str(item["url"]) + "\n")

        with (self.output_dir / "streams.txt").open("w", encoding="utf-8", newline="\n") as f:
            for rec in self.records:
                f.write(rec.url + "\n")

        self.stats.total_records = len(self.records)
        stats = asdict(self.stats)
        stats["rules"] = {
            "stream_deduplication": False,
            "channel_deduplication": False,
            "record_deduplication": False,
            "download_playlist_before_parse": True,
            "playlist_url_written_as_stream": False,
            "embedded_m3u_in_post_text": True,
            "max_playlist_depth": self.max_playlist_depth,
            "ultra_checking": True,
            "top_alts_default": 12,
            "public_sources_count": len(PUBLIC_INTERNET_SOURCES),
        }
        stats["source"] = {"url": self.page_url or "discover-only", "public_only": True}
        with (self.output_dir / "stats.json").open("w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)

        with (self.output_dir / "summary.txt").open("w", encoding="utf-8") as f:
            f.write("VK IPTV + ULTRA COLLECTOR v3.1\n")
            f.write("=" * 70 + "\n")
            f.write(f"Source: {self.page_url or 'discover-only'}\n")
            f.write(f"Posts: {len(self.posts)}\n")
            f.write(f"Records: {len(self.records)}\n")
            f.write(f"Public sources loaded: {self.stats.public_sources_loaded}\n")
            f.write(f"Playlist URLs: {self.stats.playlist_urls_found}\n")
            f.write(f"Playlists OK: {self.stats.playlists_ok}\n")
            f.write(f"Direct streams: {self.stats.direct_stream_urls}\n")
            f.write(f"Errors: {self.stats.errors}\n")
            f.write("\nNO DEDUPLICATION: YES\n")
            f.write("EMBEDDED M3U: YES\n")
            f.write("TOP ALTS (reserves): 12\n")
            f.write(f"PUBLIC SOURCES: {len(PUBLIC_INTERNET_SOURCES)}\n")
            f.write("ULTRA CHECK: --check\n")

        LOG.info("RAW SAVED → %s", self.output_dir)

    def build_channels_and_check(
        self,
        fuzzy: float,
        max_alts: int,
        workers: int,
        timeout: float,
        deep: bool,
        user_agent: str,
        ssl_verify: bool,
        top_n: int,
    ) -> None:
        LOG.info("ULTRA STAGE: fuzzy + check (top=%d, max_alts=%d)...", top_n, max_alts)
        groups: dict[str, Channel] = {}
        key_to_display: dict[str, str] = {}
        seen_urls: set[str] = set()

        for rec in self.records:
            if is_adult(rec.name, rec.group_title):
                continue
            url = rec.url.strip()
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            name = rec.name or "Unknown"
            matched = find_best_key(name, key_to_display, fuzzy)
            if matched is None:
                matched = normalize_name(name) or name.lower()
                key_to_display[matched] = name
                groups[matched] = Channel(
                    name=name,
                    group=rec.group_title or "Undefined",
                    logo=rec.tvg_logo or "",
                    tvg_id=rec.tvg_id or "",
                )
            ch = groups[matched]
            if len(ch.streams) < max_alts:
                ch.streams.append(StreamInfo(
                    url=url,
                    source=rec.source_type or rec.playlist_url or "",
                ))

        self.stats.channels_after_fuzzy = len(groups)
        LOG.info("Unique channels after fuzzy: %d", len(groups))

        all_streams = [s for ch in groups.values() for s in ch.streams]
        asyncio.run(process_streams(
            all_streams, workers, timeout, deep, user_agent, ssl_verify,
        ))

        self.stats.streams_checked = len(all_streams)
        self.stats.streams_alive = sum(1 for s in all_streams if s.http_ok)

        out = self.output_dir

        def write_ranked(mode: str, path: Path, top: int = 1) -> int:
            lines = ["#EXTM3U"]
            count = 0
            for ch in sorted(groups.values(), key=lambda c: c.name.lower()):
                alive = [s for s in ch.alive_streams if s.score >= 0]
                if not alive:
                    continue
                if mode == "best":
                    selected = alive[:1]
                elif mode == "stable":
                    selected = [s for s in alive if s.latency_ms < 1500][:1] or alive[:1]
                elif mode == "online":
                    selected = alive
                else:
                    selected = alive[:top]
                for idx, s in enumerate(selected):
                    attrs = []
                    if ch.tvg_id:
                        attrs.append(f'tvg-id="{ch.tvg_id}"')
                    if ch.logo:
                        attrs.append(f'tvg-logo="{ch.logo}"')
                    attrs.append(f'group-title="{ch.group}"')
                    extra = ""
                    if len(selected) > 1:
                        extra = f" [{idx+1}/{len(selected)}]"
                    if s.resolution:
                        extra += f" {s.resolution}"
                    if s.latency_ms < 9000:
                        extra += f" {int(s.latency_ms)}ms"
                    name = f"{ch.name}{extra}"
                    lines.append(f'#EXTINF:-1 {" ".join(attrs)},{name}')
                    lines.append(s.url)
                    count += 1
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return count

        n_best = write_ranked("best", out / "best.m3u")
        n_stable = write_ranked("stable", out / "stable.m3u")
        n_online = write_ranked("online", out / "online.m3u")
        n_alts = write_ranked("all", out / "all_with_alts.m3u", top=top_n)

        LOG.info(
            "Ranked: best=%d stable=%d online=%d alts(up to %d)=%d",
            n_best, n_stable, n_online, top_n, n_alts,
        )

        if HAS_RICH:
            table = Table(title="Ultra Check Summary")
            table.add_column("Metric", style="cyan")
            table.add_column("Value", style="green")
            table.add_row("Channels (fuzzy)", str(len(groups)))
            table.add_row("Channels with live", str(sum(1 for c in groups.values() if c.alive_streams)))
            table.add_row("Streams checked", str(self.stats.streams_checked))
            table.add_row("Working streams", str(self.stats.streams_alive))
            table.add_row("Top alts per channel", str(top_n))
            console.print(table)

        data = {
            k: {
                "name": ch.name,
                "group": ch.group,
                "streams": [asdict(s) for s in ch.streams],
            }
            for k, ch in groups.items()
        }
        (out / "results.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8",
        )

    def run(self, do_check: bool = False, discover: bool = False, **check_kwargs) -> None:
        self.crawl_group()
        if discover:
            self.load_public_sources(PUBLIC_INTERNET_SOURCES)
        self.save_raw()
        if do_check:
            self.build_channels_and_check(**check_kwargs)
        LOG.info("=" * 60)
        LOG.info(
            "FINISHED | posts=%d records=%d alive=%d public=%d",
            len(self.posts), len(self.records),
            self.stats.streams_alive, self.stats.public_sources_loaded,
        )
        LOG.info("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "VK IPTV Collector + Ultra Checker v3.1 — "
            "полный сбор + максимум альтернатив (12) + расширенный RU-поиск"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="VK URL (можно пусто при --discover)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--max-playlist-depth", type=int, default=DEFAULT_MAX_PLAYLIST_DEPTH)
    parser.add_argument("--verbose", action="store_true")

    parser.add_argument("--check", action="store_true", help="HTTP + scoring")
    parser.add_argument("--deep", action="store_true", help="ffprobe")
    parser.add_argument(
        "--discover", action="store_true",
        help="Все публичные источники (iptv-org, smolnp, dearbulut, RU/СНГ)",
    )
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument(
        "--top", type=int, default=DEFAULT_TOP_N,
        help="Резервов потоков на канал (default 12)",
    )
    parser.add_argument("--fuzzy", type=float, default=DEFAULT_FUZZY)
    parser.add_argument("--max-alts", type=int, default=DEFAULT_MAX_ALTS)
    parser.add_argument("--user-agent", default=USER_AGENT)
    parser.add_argument("--no-ssl-verify", action="store_true")

    args = parser.parse_args()

    if args.max_pages < 1 or args.max_playlist_depth < 0:
        print("Invalid max-pages / max-playlist-depth", file=sys.stderr)
        return 2

    if not args.url and not args.discover:
        print("Нужен --url или --discover", file=sys.stderr)
        return 2

    output_dir = Path(args.output)
    setup_logging(output_dir, args.verbose)

    collector = Collector(
        page_url=args.url or "",
        output_dir=output_dir,
        max_pages=args.max_pages,
        max_playlist_depth=args.max_playlist_depth,
    )

    try:
        collector.run(
            do_check=args.check,
            discover=args.discover,
            fuzzy=args.fuzzy,
            max_alts=args.max_alts,
            workers=args.workers,
            timeout=args.timeout,
            deep=args.deep,
            user_agent=args.user_agent,
            ssl_verify=not args.no_ssl_verify,
            top_n=args.top,
        )
        return 0
    except KeyboardInterrupt:
        LOG.warning("Interrupted")
        return 130
    except Exception:
        LOG.exception("FATAL")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())