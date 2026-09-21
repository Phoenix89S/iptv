#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RU IPTV MEGA PARSER
===================
Большой агрегатор публичных M3U/M3U8 + EPG.

Цели:
  * собирать 20 000+ уникальных каналов, если их реально дают источники;
  * отдельный приоритет России/русскоязычных каналов;
  * сохранять ВСЕ уникальные потоки, а не только один URL;
  * для каждого канала стремиться к >= 12 альтернативным потокам;
  * EPG: epg.one -> Teleguide -> EPG из самих M3U/XMLTV источников;
  * проверка потоков с большим количеством workers;
  * SQLite-кэш результатов проверки;
  * готовые M3U, JSON, JSONL, CSV и XMLTV index.

STABLE_Ru_IPTV:
  * отдельная карта состояния stable_state.json;
  * не изменяет логику mega_*.m3u;
  * не использует SQLite-кэш как источник истины для Stable;
  * повторно проверяет сохраненный URL прямо сейчас;
  * при неудаче проверяет следующие потоки только этого же канала;
  * принимает только HTTP 200 + latency < 1500 ms;
  * сохраняет реально проверенный рабочий URL для следующего запуска;
  * не создает новые Stable-каналы, которых нет в stable_state.json.

Важно:
  20 000 каналов и 12 потоков на канал — это ЦЕЛЕВОЙ масштаб.
  Скрипт не создаёт фиктивные URL: итоговое количество зависит от реально
  доступных публичных источников.

Запуск:
  python RU_IPTV_MEGA_PARSER.py

Опционально:
  python RU_IPTV_MEGA_PARSER.py --no-check
  python RU_IPTV_MEGA_PARSER.py --workers 256
  python RU_IPTV_MEGA_PARSER.py --max-channels 0
  python RU_IPTV_MEGA_PARSER.py --sources-file sources.txt

Файл sources.txt: один M3U/M3U8 URL на строку. Можно добавлять свои публичные
плейлисты без изменения кода.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterable, Optional

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

OUT = Path("mega_iptv_output")
DB = OUT / "mega_iptv.db"
LOG = OUT / "mega_parser.log"

# ---------------------------------------------------------------------------
# STRICT STABLE STATE
# ---------------------------------------------------------------------------
# Путь к базе соответствия:
# "Канал -> его рабочая ссылка" для Stable_Ru_IPTV.m3u.
STABLE_STATE_DB = OUT / "stable_state.json"

# Максимальная задержка для попадания ссылки в Stable.
# Условие Stable:
#   HTTP status == 200
#   latency_ms < 1500
STABLE_LATENCY_THRESHOLD_MS = 1500

TARGET_CHANNELS = 20_000
TARGET_RU = 10_000
MIN_ALTERNATIVES = 12
DEFAULT_WORKERS = 256
FETCH_WORKERS = 64
CHECK_WORKERS = 256

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 12
MAX_BYTES = 80 * 1024 * 1024
CACHE_TTL = 6 * 3600

USER_AGENT = "RU-IPTV-Mega-Parser/1.0 (+public-playlist-aggregator)"

# Known public sources. Additional sources can be supplied with --sources-file.
# iptv-org is intentionally expanded dynamically from its PLAYLISTS.md so that
# country/subdivision/city playlists do not have to be hard-coded forever.
BASE_SOURCES = [
    "https://iptv-org.github.io/iptv/countries/ru.m3u",
    "https://naggdd.github.io/iptv/ru.m3u",
    "https://smolnp.github.io/IPTVru/IPTVru.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_russia.m3u8",
    "https://dearbulut.github.io/iptv/playlists/country/ru.m3u",
    "https://raw.githubusercontent.com/substanc1/iptv-russia/main/streams/ru.m3u",
]

IPTV_ORG_PLAYLISTS = "https://raw.githubusercontent.com/iptv-org/iptv/master/PLAYLISTS.md"

EPG_SOURCES = [
    (1, "epg.one", "https://epg.one/epg2.xml.gz"),
    (2, "teleguide", "https://www.teleguide.info/download/new3/xmltv.xml.gz"),
]

# Additional EPG URLs may be placed here. Keep them public XMLTV/XML/XML.GZ.
EXTRA_EPG = []

BAD_NAME_TOKENS = {
    "xxx", "porn", "porno", "pornhub", "adult", "sex", "erotic",
    "18+", "казино", "casino", "bet", "ставки", "букмекер",
}

RU_WORDS = {
    "россия", "российский", "русский", "русская", "москва", "мск",
    "санкт-петербург", "петербург", "питер", "регион", "область",
    "край", "республика", "чувашия", "татарстан", "башкортостан",
    "сибирь", "урал", "кубань", "дон", "сахалин", "калининград",
    "новосибирск", "екатеринбург", "казань", "самара", "омск",
    "томск", "владивосток", "хабаровск", "архангельск", "мурманск",
    "рус", "ru", "cis", "снг", "беларусь", "казахстан", "кыргызстан",
    "узбекистан", "армения", "азербайджан", "молдова",
}

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

OUT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

log = logging.getLogger("mega")

# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------


@dataclass
class Stream:
    url: str
    source: str = ""
    alive: Optional[bool] = None
    latency_ms: Optional[int] = None
    status: Optional[int] = None
    content_type: str = ""
    bitrate: Optional[int] = None
    checked_at: int = 0
    failures: int = 0
    successes: int = 0

    def key(self) -> str:
        return normalize_url(self.url)


@dataclass
class Channel:
    key: str
    name: str
    original_names: list[str] = field(default_factory=list)
    tvg_id: str = ""
    tvg_name: str = ""
    logo: str = ""
    group: str = ""
    country: str = ""
    language: str = ""
    russian_priority: bool = False
    sources: set[str] = field(default_factory=set)
    streams: dict[str, Stream] = field(default_factory=dict)
    epg_source: str = ""
    epg_confidence: float = 0.0
    tvg_shift: str = ""

    def add_stream(self, stream: Stream) -> None:
        k = stream.key()
        if not k:
            return

        old = self.streams.get(k)

        if old is None:
            self.streams[k] = stream
        else:
            # Preserve the richest information from duplicate records.
            if not old.source and stream.source:
                old.source = stream.source

            if stream.logo if False else False:
                pass

    def stream_list(self) -> list[Stream]:
        return list(self.streams.values())


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def request_bytes(
    url: str,
    timeout: tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT),
    max_bytes: int = MAX_BYTES,
) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
        },
    )

    with urllib.request.urlopen(req, timeout=sum(timeout)) as r:
        chunks = []
        total = 0

        while True:
            chunk = r.read(256 * 1024)

            if not chunk:
                break

            total += len(chunk)

            if total > max_bytes:
                raise ValueError(
                    f"response exceeds {max_bytes} bytes: {url}"
                )

            chunks.append(chunk)

        return b"".join(chunks)


def fetch_text(url: str, max_bytes: int = MAX_BYTES) -> str:
    data = request_bytes(url, max_bytes=max_bytes)

    # gzip by extension or magic bytes
    if (
        url.lower().split("?", 1)[0].endswith(".gz")
        or data[:2] == b"\x1f\x8b"
    ):
        data = gzip.decompress(data)

    return data.decode("utf-8", "replace")


# ---------------------------------------------------------------------------
# NORMALIZATION
# ---------------------------------------------------------------------------


def clean_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_name(name: str) -> str:
    s = clean_text(name).lower()

    s = re.sub(
        r"\([^)]*\+\d+[^)]*\)",
        " ",
        s,
    )

    s = re.sub(
        r"\[[^]]*\]",
        " ",
        s,
    )

    s = re.sub(
        r"\b\d{1,4}\s*[.)-]\s*",
        " ",
        s,
    )

    s = re.sub(
        r"\b(uhd|fhd|hd|sd|4k|8k|1080p|720p|576p|480p)\b",
        " ",
        s,
    )

    s = re.sub(
        r"\b(рус|russia|ru)\b",
        " ",
        s,
    )

    s = re.sub(
        r"[^\w\sа-яА-ЯёЁ]",
        " ",
        s,
    )

    return re.sub(r"\s+", " ", s).strip()


def canonical_key(name: str, tvg_id: str = "") -> str:
    n = normalize_name(name)

    if tvg_id:
        tid = clean_text(tvg_id).lower()

        if tid:
            return f"id:{tid}"

    return "name:" + n


def normalize_url(url: str) -> str:
    url = (url or "").strip()

    if not url:
        return ""

    try:
        p = urllib.parse.urlsplit(url)

        # Preserve query because signed streams may depend on it.
        scheme = p.scheme.lower()
        host = p.netloc.lower()
        path = re.sub(r"/{2,}", "/", p.path)

        return urllib.parse.urlunsplit(
            (
                scheme,
                host,
                path,
                p.query,
                "",
            )
        )

    except Exception:
        return url


def is_bad_name(name: str) -> bool:
    low = clean_text(name).lower()

    return any(
        tok in low
        for tok in BAD_NAME_TOKENS
    )


def russian_score(
    name: str,
    group: str,
    country: str,
    language: str,
    source: str,
) -> int:
    text = " ".join(
        [
            name,
            group,
            country,
            language,
            source,
        ]
    ).lower()

    score = 0

    if re.search(r"[а-яё]", text):
        score += 5

    if country.lower() in {
        "ru",
        "russia",
        "rus",
    }:
        score += 10

    if (
        language.lower().startswith("ru")
        or language.lower() in {
            "rus",
            "russian",
        }
    ):
        score += 10

    for w in RU_WORDS:
        if w in text:
            score += 1

    return score


# ---------------------------------------------------------------------------
# M3U PARSER
# ---------------------------------------------------------------------------

_ATTR_RE = re.compile(
    r'''([\w-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s,]+))'''
)


def parse_attrs(line: str) -> dict[str, str]:
    out = {}

    for m in _ATTR_RE.finditer(line):
        out[m.group(1)] = next(
            (
                x
                for x in m.groups()[1:]
                if x is not None
            ),
            "",
        )

    return out


def parse_extinf_name(line: str) -> str:
    if "," in line:
        return line.split(",", 1)[1].strip()

    return ""


def parse_m3u(
    text: str,
    source_url: str,
) -> tuple[list[Channel], list[str]]:
    channels: list[Channel] = []
    epg_urls: list[str] = []
    current: Optional[dict] = None

    lines = text.replace("\r", "").split("\n")

    # Header-level EPG attributes.
    for line in lines[:5]:
        if line.startswith("#EXTM3U"):
            attrs = parse_attrs(line)

            for k in (
                "x-tvg-url",
                "url-tvg",
                "tvg-url",
            ):
                if attrs.get(k):
                    epg_urls.extend(
                        [
                            x.strip()
                            for x in attrs[k].split(",")
                            if x.strip()
                        ]
                    )

    for raw in lines:
        line = raw.strip()

        if not line:
            continue

        if line.startswith("#EXTINF"):
            attrs = parse_attrs(line)

            current = {
                "name": (
                    parse_extinf_name(line)
                    or attrs.get("tvg-name")
                    or "Unknown"
                ),
                "tvg_id": attrs.get("tvg-id", ""),
                "tvg_name": attrs.get("tvg-name", ""),
                "logo": attrs.get("tvg-logo", ""),
                "group": attrs.get("group-title", ""),
                "country": attrs.get("tvg-country", ""),
                "language": attrs.get("tvg-language", ""),
            }

        elif (
            not line.startswith("#")
            and current is not None
            and re.match(r"https?://", line, re.I)
        ):
            name = clean_text(current["name"])

            if not name or is_bad_name(name):
                current = None
                continue

            score = russian_score(
                name,
                current["group"],
                current["country"],
                current["language"],
                source_url,
            )

            ch = Channel(
                key=canonical_key(
                    name,
                    current["tvg_id"],
                ),
                name=name,
                original_names=[name],
                tvg_id=current["tvg_id"],
                tvg_name=current["tvg_name"] or name,
                logo=current["logo"],
                group=current["group"],
                country=current["country"],
                language=current["language"],
                russian_priority=score >= 6,
                sources={source_url},
            )

            ch.add_stream(
                Stream(
                    url=line,
                    source=source_url,
                )
            )

            channels.append(ch)
            current = None

    return channels, epg_urls


# ---------------------------------------------------------------------------
# DISCOVERY
# ---------------------------------------------------------------------------


def discover_iptv_org_playlists() -> list[str]:
    """Extract all public playlist URLs from iptv-org PLAYLISTS.md.

    This automatically discovers Russian subdivisions/cities and all other
    country playlists as the upstream repository changes.
    """

    try:
        text = fetch_text(
            IPTV_ORG_PLAYLISTS,
            max_bytes=15 * 1024 * 1024,
        )

    except Exception as e:
        log.warning(
            "iptv-org playlist index failed: %s",
            e,
        )
        return []

    urls = set(
        re.findall(
            r"https://iptv-org\.github\.io/iptv/[^`\s)]+\.m3u",
            text,
        )
    )

    return sorted(urls)


def load_sources_file(
    path: Optional[str],
) -> list[str]:
    if not path:
        return []

    p = Path(path)

    if not p.exists():
        log.warning(
            "sources file not found: %s",
            path,
        )
        return []

    result = []

    for line in p.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():

        line = line.strip()

        if (
            line
            and not line.startswith("#")
            and re.match(r"https?://", line)
        ):
            result.append(line)

    return result


def build_source_list(
    extra_file: Optional[str],
) -> list[str]:
    urls = list(BASE_SOURCES)

    urls.extend(
        load_sources_file(extra_file)
    )

    urls.extend(
        discover_iptv_org_playlists()
    )

    # de-duplicate while preserving order
    seen = set()
    out = []

    for u in urls:
        k = normalize_url(u)

        if k and k not in seen:
            seen.add(k)
            out.append(u)

    # Russian first, then everything else.
    out.sort(
        key=lambda u: (
            0
            if (
                "/ru" in u.lower()
                or "russia" in u.lower()
            )
            else 1,
            u,
        )
    )

    return out


# ---------------------------------------------------------------------------
# MERGE / CHANNEL MATCHING
# ---------------------------------------------------------------------------


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0

    if a == b:
        return 1.0

    aa = set(a.split())
    bb = set(b.split())

    if not aa or not bb:
        return 0.0

    j = len(aa & bb) / len(aa | bb)

    if a in b or b in a:
        j = max(j, 0.86)

    return j


def find_channel(
    channels: dict[str, Channel],
    incoming: Channel,
) -> Optional[Channel]:

    # Strong ID match.
    if incoming.tvg_id:
        key = canonical_key(
            incoming.name,
            incoming.tvg_id,
        )

        if key in channels:
            return channels[key]

    key = canonical_key(
        incoming.name
    )

    if key in channels:
        return channels[key]

    # Controlled fuzzy merge. Do not compare against all 20k for every item.
    # Index by first two normalized tokens.
    tokens = normalize_name(
        incoming.name
    ).split()

    if not tokens:
        return None

    prefix = " ".join(tokens[:2])

    candidates = [
        c
        for c in channels.values()
        if normalize_name(c.name).startswith(prefix)
    ]

    best = None
    best_score = 0.0

    for c in candidates[:100]:
        s = similarity(
            normalize_name(incoming.name),
            normalize_name(c.name),
        )

        if s > best_score:
            best = c
            best_score = s

    return (
        best
        if best_score >= 0.90
        else None
    )


def merge_channel(
    dst: Channel,
    src: Channel,
) -> None:

    for n in src.original_names:
        if n not in dst.original_names:
            dst.original_names.append(n)

    if not dst.tvg_id and src.tvg_id:
        dst.tvg_id = src.tvg_id

    if not dst.tvg_name and src.tvg_name:
        dst.tvg_name = src.tvg_name

    if not dst.logo and src.logo:
        dst.logo = src.logo

    if not dst.group and src.group:
        dst.group = src.group

    if not dst.country and src.country:
        dst.country = src.country

    if not dst.language and src.language:
        dst.language = src.language

    dst.russian_priority = (
        dst.russian_priority
        or src.russian_priority
    )

    dst.sources.update(src.sources)

    for s in src.streams.values():
        dst.add_stream(s)


def aggregate(
    parsed: Iterable[
        tuple[str, list[Channel], list[str]]
    ],
) -> tuple[dict[str, Channel], list[str]]:

    channels: dict[str, Channel] = {}
    epg_urls: list[str] = []
    index: dict[str, Channel] = {}

    for source_url, items, source_epg in parsed:
        epg_urls.extend(source_epg)

        for incoming in items:

            # Fast exact identity first.
            found = find_channel(
                channels,
                incoming,
            )

            if found is None:
                channels[incoming.key] = incoming
                found = incoming
            else:
                merge_channel(
                    found,
                    incoming,
                )

            # Refresh simple index aliases.
            index[
                canonical_key(found.name)
            ] = found

            if found.tvg_id:
                index[
                    canonical_key(
                        found.name,
                        found.tvg_id,
                    )
                ] = found

    return (
        channels,
        list(dict.fromkeys(epg_urls)),
    )


# ---------------------------------------------------------------------------
# STREAM CHECKING / SQLITE CACHE
# ---------------------------------------------------------------------------


def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(
        DB,
        check_same_thread=False,
    )

    con.execute(
        "PRAGMA journal_mode=WAL"
    )

    con.execute(
        "PRAGMA synchronous=NORMAL"
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_health (
            url TEXT PRIMARY KEY,
            checked INTEGER NOT NULL,
            alive INTEGER NOT NULL,
            status INTEGER,
            latency_ms INTEGER,
            content_type TEXT,
            successes INTEGER NOT NULL DEFAULT 0,
            failures INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    con.commit()

    return con


def cached_health(
    con: sqlite3.Connection,
    url: str,
) -> Optional[dict]:

    row = con.execute(
        """
        SELECT
            url,
            checked,
            alive,
            status,
            latency_ms,
            content_type,
            successes,
            failures
        FROM stream_health
        WHERE url=?
        """,
        (url,),
    ).fetchone()

    if not row:
        return None

    if (
        int(time.time()) - row[1]
        > CACHE_TTL
    ):
        return None

    return dict(
        zip(
            (
                "url",
                "checked",
                "alive",
                "status",
                "latency_ms",
                "content_type",
                "successes",
                "failures",
            ),
            row,
        )
    )


def check_stream(s: Stream) -> Stream:
    start = time.monotonic()

    req = urllib.request.Request(
        s.url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "close",
        },
    )

    try:
        with urllib.request.urlopen(
            req,
            timeout=READ_TIMEOUT,
        ) as r:

            status = getattr(
                r,
                "status",
                200,
            )

            content_type = r.headers.get(
                "Content-Type",
                "",
            )

            # Read only a small prefix. For HLS this confirms that the endpoint
            # actually returns data without downloading the media.
            prefix = r.read(4096)

            if not prefix:
                raise IOError(
                    "empty response"
                )

            s.alive = (
                200 <= status < 400
            )

            s.status = status
            s.content_type = content_type

            s.latency_ms = int(
                (
                    time.monotonic()
                    - start
                ) * 1000
            )

            s.checked_at = int(
                time.time()
            )

            if s.alive:
                s.successes += 1
            else:
                s.failures += 1

    except Exception:
        s.alive = False
        s.status = None

        s.latency_ms = int(
            (
                time.monotonic()
                - start
            ) * 1000
        )

        s.checked_at = int(
            time.time()
        )

        s.failures += 1

    return s


def stream_score(
    s: Stream,
) -> float:

    if s.alive is False:
        return -1000.0

    score = 0.0

    if s.alive is True:
        score += 100

    if s.latency_ms is not None:
        score += max(
            0,
            40 - s.latency_ms / 100,
        )

    if (
        s.content_type
        and (
            "mpegurl"
            in s.content_type.lower()
            or "m3u8"
            in s.content_type.lower()
        )
    ):
        score += 10

    score += min(
        s.successes * 2,
        20,
    )

    score -= min(
        s.failures * 5,
        30,
    )

    return score


def validate_streams(
    channels: dict[str, Channel],
    workers: int,
    enabled: bool,
) -> None:

    if not enabled:
        log.info(
            "STREAM CHECK: disabled"
        )
        return

    con = init_db()

    all_streams: list[Stream] = []

    for ch in channels.values():
        for s in ch.streams.values():
            all_streams.append(s)

    log.info(
        "STREAM CHECK: %d unique URLs, workers=%d",
        len(all_streams),
        workers,
    )

    todo = []

    for s in all_streams:
        c = cached_health(
            con,
            s.key(),
        )

        if c:
            s.alive = bool(
                c["alive"]
            )

            s.status = c["status"]
            s.latency_ms = c["latency_ms"]
            s.content_type = (
                c["content_type"]
                or ""
            )
            s.checked_at = c["checked"]
            s.successes = c["successes"]
            s.failures = c["failures"]

        else:
            todo.append(s)

    log.info(
        "STREAM CHECK: cache hit=%d network=%d",
        len(all_streams) - len(todo),
        len(todo),
    )

    if todo:
        with cf.ThreadPoolExecutor(
            max_workers=workers
        ) as ex:

            for i, s in enumerate(
                ex.map(
                    check_stream,
                    todo,
                ),
                1,
            ):
                if i % 1000 == 0:
                    log.info(
                        "STREAM CHECK: %d/%d",
                        i,
                        len(todo),
                    )

    rows = []

    for s in all_streams:
        rows.append(
            (
                s.key(),
                int(
                    s.checked_at
                    or time.time()
                ),
                int(bool(s.alive)),
                s.status,
                s.latency_ms,
                s.content_type,
                s.successes,
                s.failures,
            )
        )

    con.executemany(
        """
        INSERT OR REPLACE INTO stream_health(
            url,
            checked,
            alive,
            status,
            latency_ms,
            content_type,
            successes,
            failures
        )
        VALUES(?,?,?,?,?,?,?,?)
        """,
        rows,
    )

    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# STRICT STABLE STATE
# ---------------------------------------------------------------------------


def load_stable_state() -> dict:
    """
    Загружает карту:
        channel_key -> последний успешный URL.

    Если файла нет, возвращается пустая карта.

    Если JSON поврежден, карта сбрасывается,
    но основной парсер продолжает работу.
    """

    if STABLE_STATE_DB.exists():
        try:
            data = json.loads(
                STABLE_STATE_DB.read_text(
                    encoding="utf-8"
                )
            )

            if isinstance(data, dict):
                return data

            log.warning(
                "Stable state DB has invalid root type, resetting"
            )
            return {}

        except Exception as e:
            log.warning(
                "Stable state DB corrupted, resetting: %s",
                e,
            )
            return {}

    return {}


def save_stable_state(
    mapping: dict,
):
    """
    Сохраняет только непустые соответствия:
        channel_key -> рабочий URL.
    """

    clean_mapping = {
        k: v
        for k, v in mapping.items()
        if v
    }

    try:
        STABLE_STATE_DB.write_text(
            json.dumps(
                clean_mapping,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    except Exception as e:
        log.error(
            "Failed to write stable state: %s",
            e,
        )


def select_and_validate_stable_stream(
    ch: Channel,
    state_map: dict,
) -> Optional[Stream]:
    """
    Изолированная проверка для Stable-плейлиста.

    ВАЖНО:
      * не использует глобальный пул alive-ссылок как источник истины;
      * не использует SQLite-кэш для принятия решения;
      * каждый кандидат проверяется реальным check_stream()
        непосредственно в момент генерации Stable;
      * сначала проверяется сохраненная ссылка;
      * затем проверяются остальные ссылки только этого же канала;
      * успешной считается только ссылка:
            status == 200
            latency_ms < 1500
      * при успехе именно этот URL записывается в state_map.

    Если все кандидаты не прошли:
      state_map[ch.key] = ""
      возвращается None.
    """

    ch_key = ch.key
    last_url = state_map.get(ch_key)

    candidate_streams: list[Stream] = []

    # -----------------------------------------------------------------------
    # 1. Эталонная ссылка из предыдущего запуска.
    # -----------------------------------------------------------------------

    if last_url:
        norm_last = normalize_url(
            last_url
        )

        for s in ch.stream_list():
            if (
                normalize_url(s.url)
                == norm_last
            ):
                candidate_streams.insert(
                    0,
                    s,
                )
                break

    # -----------------------------------------------------------------------
    # 2. Остальные потоки ТОЛЬКО ЭТОГО ЖЕ КАНАЛА.
    # -----------------------------------------------------------------------

    others = [
        s
        for s in ch.stream_list()
        if normalize_url(s.url)
        != normalize_url(last_url)
    ]

    # Используем текущий score только для порядка кандидатов.
    # Сам результат score НЕ является доказательством работоспособности.
    candidate_streams.extend(
        sorted(
            others,
            key=stream_score,
            reverse=True,
        )
    )

    # -----------------------------------------------------------------------
    # 3. Изолированная реальная проверка.
    # -----------------------------------------------------------------------

    for stream in candidate_streams:

        log.debug(
            "STABLE CHECK: Testing %s for channel '%s'",
            stream.url,
            ch.name,
        )

        # ВАЖНО:
        # создаем новый Stream, чтобы Stable не наследовал alive/status/
        # latency из глобальной проверки или SQLite-кэша.
        test_result = check_stream(
            Stream(
                url=stream.url
            )
        )

        is_status_ok = (
            test_result.alive is True
            and test_result.status == 200
        )

        is_latency_ok = (
            test_result.latency_ms is not None
            and test_result.latency_ms
            < STABLE_LATENCY_THRESHOLD_MS
        )

        if (
            is_status_ok
            and is_latency_ok
        ):
            log.info(
                "STABLE CHECK: SUCCESS %s | channel='%s' | status=%s | latency=%sms",
                stream.url,
                ch.name,
                test_result.status,
                test_result.latency_ms,
            )

            # Сохраняем именно тот URL, который реально проверили.
            state_map[ch_key] = stream.url

            return test_result

        reason = (
            f"Status {getattr(test_result, 'status', 'N/A')}"
        )

        if (
            test_result.latency_ms
            is not None
        ):
            reason += (
                f", Latency "
                f"{test_result.latency_ms}ms"
            )

        log.info(
            "STABLE CHECK: Failed %s (%s)",
            stream.url,
            reason,
        )

    # -----------------------------------------------------------------------
    # 4. Ни одна ссылка канала не прошла.
    # -----------------------------------------------------------------------

    state_map[ch_key] = ""

    log.warning(
        "STABLE CHECK: NO VALID STREAM for channel '%s' [%s]",
        ch.name,
        ch_key,
    )

    return None


# ---------------------------------------------------------------------------
# EPG
# ---------------------------------------------------------------------------


def load_xmltv(
    url: str,
) -> dict[str, dict]:

    log.info(
        "EPG FETCH: %s",
        url,
    )

    try:
        data = request_bytes(
            url,
            max_bytes=150 * 1024 * 1024,
        )

        if (
            url.lower()
            .split("?", 1)[0]
            .endswith(".gz")
            or data[:2] == b"\x1f\x8b"
        ):
            data = gzip.decompress(data)

        root = ET.fromstring(data)

    except Exception as e:
        log.warning(
            "EPG failed %s: %s",
            url,
            e,
        )
        return {}

    out = {}

    for ch in root.findall("channel"):
        cid = ch.attrib.get(
            "id",
            "",
        ).strip()

        if not cid:
            continue

        names = [
            clean_text(
                x.text or ""
            )
            for x in ch.findall(
                "display-name"
            )
            if (
                x.text or ""
            ).strip()
        ]

        icon = ch.find("icon")

        logo = (
            icon.attrib.get(
                "src",
                "",
            )
            if icon is not None
            else ""
        )

        out[cid] = {
            "id": cid,
            "names": names,
            "logo": logo,
        }

    return out


def epg_match(
    channels: dict[str, Channel],
    epg_sets: list[
        tuple[str, dict[str, dict]]
    ],
) -> None:

    # Priority is represented by list order: epg.one first.
    for ch in channels.values():

        best = None
        best_score = 0.0

        # Existing tvg-id is a strong exact candidate.
        for source_name, epg in epg_sets:

            if (
                ch.tvg_id
                and ch.tvg_id in epg
            ):
                best = (
                    source_name,
                    epg[ch.tvg_id],
                    1.0,
                )
                break

            target = normalize_name(
                ch.name
            )

            if not target:
                continue

            for item in epg.values():
                for n in item["names"]:

                    s = similarity(
                        target,
                        normalize_name(n),
                    )

                    if s > best_score:
                        best_score = s
                        best = (
                            source_name,
                            item,
                            s,
                        )

                if best_score >= 0.98:
                    break

            if best_score >= 0.98:
                break

        if (
            best
            and best[2] >= 0.82
        ):
            src, item, score = best

            ch.tvg_id = item["id"]
            ch.epg_source = src
            ch.epg_confidence = score

            if (
                not ch.logo
                and item.get("logo")
            ):
                ch.logo = item["logo"]


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------


def xml_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def m3u_attr(s: str) -> str:
    return clean_text(s).replace(
        '"',
        "'",
    )


def write_m3u(
    channels: list[Channel],
    path: Path,
    all_streams: bool,
    min_streams: int = 0,
) -> int:

    lines = ["#EXTM3U"]
    count = 0

    for ch in channels:

        streams = sorted(
            ch.stream_list(),
            key=lambda x: stream_score(x),
            reverse=True,
        )

        if not all_streams:
            streams = streams[:1]

        if (
            min_streams
            and len(streams) < min_streams
        ):
            continue

        for rank, s in enumerate(
            streams,
            1,
        ):

            suffix = (
                f" [ALT {rank}]"
                if rank > 1
                else ""
            )

            attrs = [
                (
                    f'tvg-id="{m3u_attr(ch.tvg_id)}"'
                    if ch.tvg_id
                    else ""
                ),
                (
                    f'tvg-name="{m3u_attr(ch.tvg_name or ch.name)}"'
                ),
                (
                    f'tvg-logo="{m3u_attr(ch.logo)}"'
                    if ch.logo
                    else ""
                ),
                (
                    f'group-title="{m3u_attr(ch.group or ("Россия" if ch.russian_priority else "IPTV"))}"'
                ),
                f'stream-rank="{rank}"',
                (
                    f'backup-count="{max(0, len(streams)-1)}"'
                ),
            ]

            attrs = " ".join(
                x
                for x in attrs
                if x
            )

            lines.append(
                f'#EXTINF:-1 {attrs},{m3u_attr(ch.name)}{suffix}'
            )

            lines.append(
                s.url
            )

            count += 1

    path.write_text(
        "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )

    return count


# ---------------------------------------------------------------------------
# STRICT STABLE PLAYLIST WRITER
# ---------------------------------------------------------------------------


def write_strictly_stable_playlist(
    channels: list[Channel],
    path: Path,
    state_map: dict,
) -> int:
    """
    Создает Stable_Ru_IPTV.m3u строго на основании state_map.

    ВАЖНО:
      * новые каналы автоматически не добавляются;
      * используются только ключи, уже существующие в stable_state.json;
      * канал ищется в текущем пуле по ch.key;
      * ссылка проверяется отдельно через select_and_validate_stable_stream();
      * в итог попадает максимум одна ссылка на канал;
      * если канал исчез из текущего пула, он пропускается;
      * если все ссылки канала мертвы/медленные, канал не записывается.
    """

    lines = ["#EXTM3U"]
    count = 0

    # Индекс текущих каналов для быстрого поиска по ключу из JSON.
    channels_index = {
        c.key: c
        for c in channels
    }

    processed_keys = set()

    # Проходим только по тем каналам,
    # которые были сохранены в state_map.
    for ch_key, saved_url in state_map.items():

        if (
            not saved_url
            or ch_key in processed_keys
        ):
            continue

        ch = channels_index.get(
            ch_key
        )

        if not ch:
            log.warning(
                "STABLE: Channel from state map not found in current pool: %s",
                ch_key,
            )
            continue

        selected_stream = (
            select_and_validate_stable_stream(
                ch,
                state_map,
            )
        )

        if selected_stream:

            attrs = [
                (
                    f'tvg-id="{m3u_attr(ch.tvg_id)}"'
                    if ch.tvg_id
                    else ""
                ),
                (
                    f'tvg-name="{m3u_attr(ch.tvg_name or ch.name)}"'
                ),
                (
                    f'tvg-logo="{m3u_attr(ch.logo)}"'
                    if ch.logo
                    else ""
                ),
                (
                    f'group-title="{m3u_attr(ch.group or ("Россия" if ch.russian_priority else "IPTV"))}"'
                ),
                'tvg-shift="0"',
            ]

            attrs = " ".join(
                x
                for x in attrs
                if x
            )

            lines.append(
                f'#EXTINF:-1 {attrs},{m3u_attr(ch.name)}'
            )

            lines.append(
                selected_stream.url
            )

            count += 1

        processed_keys.add(
            ch_key
        )

    path.write_text(
        "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )

    return count


def write_json(
    channels: list[Channel],
    path: Path,
) -> None:

    payload = []

    for c in channels:

        d = {
            "key": c.key,
            "name": c.name,
            "original_names": c.original_names,
            "tvg_id": c.tvg_id,
            "tvg_name": c.tvg_name,
            "logo": c.logo,
            "group": c.group,
            "country": c.country,
            "language": c.language,
            "russian_priority": c.russian_priority,
            "epg_source": c.epg_source,
            "epg_confidence": c.epg_confidence,
            "stream_count": len(c.streams),
            "alive_stream_count": sum(
                1
                for s in c.streams.values()
                if s.alive
            ),
            "streams": [
                asdict(s)
                for s in sorted(
                    c.streams.values(),
                    key=stream_score,
                    reverse=True,
                )
            ],
            "sources": sorted(
                c.sources
            ),
        }

        payload.append(d)

    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def write_jsonl(
    channels: list[Channel],
    path: Path,
) -> None:

    with path.open(
        "w",
        encoding="utf-8",
    ) as f:

        for c in channels:

            f.write(
                json.dumps(
                    {
                        "key": c.key,
                        "name": c.name,
                        "tvg_id": c.tvg_id,
                        "logo": c.logo,
                        "group": c.group,
                        "russian": c.russian_priority,
                        "epg_source": c.epg_source,
                        "epg_confidence": c.epg_confidence,
                        "streams": [
                            asdict(s)
                            for s in sorted(
                                c.streams.values(),
                                key=stream_score,
                                reverse=True,
                            )
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def write_txt(
    channels: list[Channel],
    path: Path,
) -> None:

    with path.open(
        "w",
        encoding="utf-8",
    ) as f:

        for c in channels:

            streams = sorted(
                c.streams.values(),
                key=stream_score,
                reverse=True,
            )

            f.write(
                f"{c.name} | "
                f"EPG={c.tvg_id or '-'} | "
                f"streams={len(streams)} | "
                f"alive={sum(1 for s in streams if s.alive)}\n"
            )

            for i, s in enumerate(
                streams,
                1,
            ):
                f.write(
                    f"  {i:03d}. {s.url}\n"
                )


def write_stats(
    channels: list[Channel],
    source_count: int,
    epg_count: int,
    path: Path,
) -> dict:

    ru = [
        c
        for c in channels
        if c.russian_priority
    ]

    stream_total = sum(
        len(c.streams)
        for c in channels
    )

    alive_total = sum(
        sum(
            1
            for s in c.streams.values()
            if s.alive
        )
        for c in channels
    )

    with12 = sum(
        1
        for c in channels
        if sum(
            1
            for s in c.streams.values()
            if s.alive is not False
        ) >= MIN_ALTERNATIVES
    )

    with12_alive = sum(
        1
        for c in channels
        if sum(
            1
            for s in c.streams.values()
            if s.alive is True
        ) >= MIN_ALTERNATIVES
    )

    stats = {
        "channels": len(channels),
        "russian_channels": len(ru),
        "target_channels": TARGET_CHANNELS,
        "target_russian_channels": TARGET_RU,
        "stream_urls": stream_total,
        "alive_stream_urls": alive_total,
        "channels_with_12plus_pool": with12,
        "channels_with_12plus_alive": with12_alive,
        "sources": source_count,
        "epg_sources": epg_count,
        "channels_with_epg": sum(
            1
            for c in channels
            if c.tvg_id
        ),
        "epg_matched_by_epg_one": sum(
            1
            for c in channels
            if c.epg_source == "epg.one"
        ),
        "epg_matched_by_teleguide": sum(
            1
            for c in channels
            if c.epg_source == "teleguide"
        ),
        "generated_at": int(
            time.time()
        ),
    }

    path.write_text(
        json.dumps(
            stats,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return stats


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RU IPTV Mega Parser"
    )

    p.add_argument(
        "--sources-file",
        default=None,
    )

    p.add_argument(
        "--workers",
        type=int,
        default=CHECK_WORKERS,
    )

    p.add_argument(
        "--fetch-workers",
        type=int,
        default=FETCH_WORKERS,
    )

    p.add_argument(
        "--max-channels",
        type=int,
        default=0,
        help="0 = unlimited",
    )

    p.add_argument(
        "--no-check",
        action="store_true",
    )

    p.add_argument(
        "--no-iptv-org-expand",
        action="store_true",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    log.info("=" * 72)
    log.info(
        "RU IPTV MEGA PARSER"
    )
    log.info(
        "TARGET: >= %d channels / >= %d Russian",
        TARGET_CHANNELS,
        TARGET_RU,
    )
    log.info(
        "ALTERNATIVES TARGET: >= %d per channel, no upper limit",
        MIN_ALTERNATIVES,
    )
    log.info("=" * 72)

    sources = list(
        BASE_SOURCES
    )

    sources.extend(
        load_sources_file(
            args.sources_file
        )
    )

    if not args.no_iptv_org_expand:
        sources.extend(
            discover_iptv_org_playlists()
        )

    sources = list(
        dict.fromkeys(
            normalize_url(x)
            for x in sources
            if x
        )
    )

    log.info(
        "SOURCES: %d",
        len(sources),
    )

    parsed: list[
        tuple[
            str,
            list[Channel],
            list[str],
        ]
    ] = []

    source_epg_urls: list[str] = []

    def fetch_parse(url: str):
        try:
            text = fetch_text(url)

            items, epgs = parse_m3u(
                text,
                url,
            )

            return (
                url,
                items,
                epgs,
                None,
            )

        except Exception as e:
            return (
                url,
                [],
                [],
                repr(e),
            )

    with cf.ThreadPoolExecutor(
        max_workers=max(
            1,
            args.fetch_workers,
        )
    ) as ex:

        futures = [
            ex.submit(
                fetch_parse,
                u,
            )
            for u in sources
        ]

        for i, fut in enumerate(
            cf.as_completed(futures),
            1,
        ):

            (
                url,
                items,
                epgs,
                err,
            ) = fut.result()

            if err:
                log.warning(
                    "SOURCE FAIL [%d/%d] %s :: %s",
                    i,
                    len(futures),
                    url,
                    err,
                )
                continue

            parsed.append(
                (
                    url,
                    items,
                    epgs,
                )
            )

            source_epg_urls.extend(
                epgs
            )

            log.info(
                "SOURCE %d/%d: %s -> records=%d epg=%d",
                i,
                len(futures),
                url,
                len(items),
                len(epgs),
            )

    channels, m3u_epgs = aggregate(
        parsed
    )

    source_epg_urls.extend(
        m3u_epgs
    )

    log.info(
        "CHANNELS AFTER MERGE: %d",
        len(channels),
    )

    # If max-channels is set, keep Russian channels first, then the rest.
    if (
        args.max_channels
        and len(channels)
        > args.max_channels
    ):

        ordered = sorted(
            channels.values(),
            key=lambda c: (
                not c.russian_priority,
                -len(c.streams),
                c.name,
            ),
        )[
            :args.max_channels
        ]

        channels = {
            c.key: c
            for c in ordered
        }

    # EPG priority:
    # epg.one, teleguide, then URLs embedded in M3U.
    epg_urls = (
        [
            u
            for _, _, u in EPG_SOURCES
        ]
        + EXTRA_EPG
        + source_epg_urls
    )

    epg_urls = list(
        dict.fromkeys(
            u
            for u in epg_urls
            if re.match(
                r"https?://",
                u,
                re.I,
            )
        )
    )

    epg_sets = []

    for priority, name, url in EPG_SOURCES:
        epg = load_xmltv(url)

        if epg:
            epg_sets.append(
                (
                    name,
                    epg,
                )
            )

    # Limit embedded EPG downloads to avoid a runaway number of giant XML files.
    embedded = [
        u
        for u in epg_urls
        if u
        not in {
            x[2]
            for x in EPG_SOURCES
        }
    ][:30]

    for url in embedded:

        if any(
            url == x[2]
            for x in EPG_SOURCES
        ):
            continue

        epg = load_xmltv(url)

        if epg:
            epg_sets.append(
                (
                    url,
                    epg,
                )
            )

    epg_match(
        channels,
        epg_sets,
    )

    log.info(
        "EPG: loaded_sets=%d",
        len(epg_sets),
    )

    # -----------------------------------------------------------------------
    # ОБЩАЯ ПРОВЕРКА ПОТОКОВ
    # -----------------------------------------------------------------------
    #
    # Эта проверка остается полностью независимой от Stable.
    # Она продолжает обслуживать обычные mega_*.m3u и статистику.
    #
    validate_streams(
        channels,
        max(
            1,
            args.workers,
        ),
        not args.no_check,
    )

    # -----------------------------------------------------------------------
    # SORT CHANNELS
    # -----------------------------------------------------------------------

    # Sort Russian channels first and by stream richness.
    channel_list = sorted(
        channels.values(),
        key=lambda c: (
            not c.russian_priority,
            -len(c.streams),
            -sum(
                1
                for s in c.streams.values()
                if s.alive
            ),
            normalize_name(c.name),
        ),
    )

    # -----------------------------------------------------------------------
    # EXISTING MEGA OUTPUT
    # -----------------------------------------------------------------------

    best = (
        OUT
        / "mega_best.m3u"
    )

    all_streams = (
        OUT
        / "mega_all_streams.m3u"
    )

    twelve = (
        OUT
        / "mega_12plus.m3u"
    )

    ru = (
        OUT
        / "mega_russia.m3u"
    )

    ru12 = (
        OUT
        / "mega_russia_12plus.m3u"
    )

    write_m3u(
        channel_list,
        best,
        all_streams=False,
    )

    write_m3u(
        channel_list,
        all_streams,
        all_streams=True,
    )

    write_m3u(
        channel_list,
        twelve,
        all_streams=True,
        min_streams=MIN_ALTERNATIVES,
    )

    ru_channels = [
        c
        for c in channel_list
        if c.russian_priority
    ]

    write_m3u(
        ru_channels,
        ru,
        all_streams=False,
    )

    write_m3u(
        ru_channels,
        ru12,
        all_streams=True,
        min_streams=MIN_ALTERNATIVES,
    )

    # -----------------------------------------------------------------------
    # STRICTLY STABLE PLAYLIST
    # -----------------------------------------------------------------------
    #
    # ВАЖНО:
    #
    # Этот блок НЕ заменяет validate_streams().
    #
    # validate_streams() уже отработал выше и обслуживает основной пул.
    #
    # Stable здесь запускает отдельную точечную проверку:
    #
    #   stable_state.json
    #          |
    #          v
    #   только сохраненные каналы
    #          |
    #          v
    #   сохраненный URL
    #          |
    #          v
    #   check_stream() прямо сейчас
    #          |
    #       200 + <1500ms
    #          |
    #       YES -> Stable
    #       NO  -> следующий URL
    #                только того же канала
    #
    # SQLite-кэш здесь НЕ является источником истины.
    # -----------------------------------------------------------------------

    log.info(
        "GENERATING STRICTLY STABLE PLAYLIST..."
    )

    # Загружаем карту соответствий
    # предыдущего успешного запуска.
    stable_map = load_stable_state()

    log.info(
        "STABLE STATE: %d saved channel mappings loaded.",
        sum(
            1
            for v in stable_map.values()
            if v
        ),
    )

    # Запускаем точечную проверку только тех каналов,
    # что есть в карте состояния.
    strict_stable_count = (
        write_strictly_stable_playlist(
            channel_list,
            OUT / "Stable_Ru_IPTV.m3u",
            stable_map,
        )
    )

    # Сохраняем обновленные рабочие ссылки
    # для следующего запуска.
    save_stable_state(
        stable_map
    )

    log.info(
        "STRICTLY STABLE PLAYLIST: %d verified entries written.",
        strict_stable_count,
    )

    log.info(
        "STRICTLY STABLE STATE: %d active mappings saved.",
        sum(
            1
            for v in stable_map.values()
            if v
        ),
    )

    # -----------------------------------------------------------------------
    # EXISTING JSON / JSONL / TXT / STATISTICS
    # -----------------------------------------------------------------------

    write_json(
        channel_list,
        OUT / "mega_channels.json",
    )

    write_jsonl(
        channel_list,
        OUT / "mega_channels.jsonl",
    )

    write_txt(
        channel_list,
        OUT / "mega_channels.txt",
    )

    stats = write_stats(
        channel_list,
        len(sources),
        len(epg_sets),
        OUT / "statistics.json",
    )

    report = (
        OUT
        / "statistics.txt"
    )

    report.write_text(
        "\n".join(
            [
                "RU IPTV MEGA PARSER",
                "=" * 60,
                f"Channels: {stats['channels']}",
                f"Russian/CIS priority: {stats['russian_channels']}",
                f"Stream URLs: {stats['stream_urls']}",
                f"Alive stream URLs: {stats['alive_stream_urls']}",
                f"Channels with >=12 pool: {stats['channels_with_12plus_pool']}",
                f"Channels with >=12 alive: {stats['channels_with_12plus_alive']}",
                f"Channels with EPG: {stats['channels_with_epg']}",
                f"EPG.one matches: {stats['epg_matched_by_epg_one']}",
                f"Teleguide matches: {stats['epg_matched_by_teleguide']}",
                f"Sources: {stats['sources']}",
                f"EPG sets: {stats['epg_sources']}",
                "",
                (
                    f"Target 20k reached: "
                    f"{'YES' if stats['channels'] >= TARGET_CHANNELS else 'NO'}"
                ),
                (
                    f"Russian target 10k reached: "
                    f"{'YES' if stats['russian_channels'] >= TARGET_RU else 'NO'}"
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # -----------------------------------------------------------------------
    # FINAL LOG
    # -----------------------------------------------------------------------

    log.info("=" * 72)
    log.info(
        "FINISHED"
    )

    log.info(
        "CHANNELS: %d | RU: %d | STREAMS: %d | ALIVE: %d",
        stats["channels"],
        stats["russian_channels"],
        stats["stream_urls"],
        stats["alive_stream_urls"],
    )

    log.info(
        ">=12 pool: %d | >=12 alive: %d",
        stats["channels_with_12plus_pool"],
        stats["channels_with_12plus_alive"],
    )

    log.info(
        "STABLE: %d verified entries",
        strict_stable_count,
    )

    log.info(
        "STABLE STATE: %d active mappings",
        sum(
            1
            for v in stable_map.values()
            if v
        ),
    )

    log.info(
        "OUTPUT: %s",
        OUT.resolve(),
    )

    log.info("=" * 72)

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )