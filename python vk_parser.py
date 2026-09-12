import asyncio
import html
import logging
import random
import re
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)


# ============================================================
# VK IPTV PARSER
# ============================================================
#
# Логика:
#
# VK wall
#    ↓
# полученный HTML
#    ↓
# баннер НЕ считается отсутствием страницы
#    ↓
# анализ HTML + DOM + href
#    ↓
# поиск IPTV / M3U / M3U8 / TXT / PLS / TS / UDP
#    ↓
# дедупликация
#    ↓
# итоговый M3U
#
# CAPTCHA / обязательную авторизацию этот скрипт НЕ обходит.
# Если VK отдаёт публичное содержимое вместе с баннером —
# содержимое всё равно анализируется.
# ============================================================


# ============================================================
# НАСТРОЙКИ
# ============================================================

GROUP_ID = "228871429"

BASE_URL = f"https://m.vk.ru/wall-{GROUP_ID}"

OUTPUT_FILE = "tv_ip_tv_playlist.m3u"
LOG_FILE = "vk_parser.log"

# Исходные HTML сохраняются сюда
HTML_DIR = Path("vk_html")

# Максимальное количество найденных объектов постов
MAX_POSTS_TO_CHECK = 100

# Максимальное количество страниц стены
MAX_PAGES = 10

# Шаг offset
PAGE_OFFSET = 20

# Таймаут перехода
PAGE_TIMEOUT = 30000

# Таймаут ожидания DOM
SELECTOR_TIMEOUT = 8000

# Паузы
MIN_DELAY = 1.5
MAX_DELAY = 3.5

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/126.0.0.0 Safari/537.36"
)


# ============================================================
# ЛОГ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(
            LOG_FILE,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)

log = logging.getLogger("VK-IPTV")


# ============================================================
# REGEX
# ============================================================

# URL с классическими расширениями
EXTENSION_URL_REGEX = re.compile(
    r"""https?://[^\s"'<>]+?\.(?:m3u8?|pls|txt|ts|mp4|flv|avi)(?:\?[^\s"'<>]*)?""",
    re.IGNORECASE,
)

# UDP
UDP_REGEX = re.compile(
    r"""udp://[^\s"'<>]+""",
    re.IGNORECASE,
)

# Любой HTTP(S) URL с query string.
#
# Нужен для:
#
# get.php?username=...&password=...&type=m3u
# playlist.php?...
# player_api.php?...
#
PARAMETER_URL_REGEX = re.compile(
    r"""https?://[^\s"'<>]+\?[^\s"'<>]+""",
    re.IGNORECASE,
)

# URL, встречающиеся внутри HTML/JSON
GENERIC_URL_REGEX = re.compile(
    r"""https?://[^\s"'<>\\]+""",
    re.IGNORECASE,
)


# ============================================================
# НОРМАЛИЗАЦИЯ
# ============================================================

def normalize_url(url: str) -> str:
    """
    Минимальная безопасная нормализация URL.
    IPTV query-параметры не удаляются.
    """

    if not url:
        return ""

    url = html.unescape(url)

    # JSON escaped slash
    url = url.replace("\\/", "/")

    # HTML escaped &
    url = url.replace("&amp;", "&")

    url = url.strip()

    # Убираем окружающие кавычки/скобки
    url = url.strip("\"'<>[]{}")

    return url


# ============================================================
# VK AWAY.PHP
# ============================================================

def unwrap_vk_away_url(url: str) -> str:
    """
    Если VK завернул внешний URL через away.php,
    пытаемся извлечь оригинальный URL.

    Пример:

    https://vk.com/away.php?to=https%3A%2F%2Fexample.com%2Fa.m3u8

    → https://example.com/a.m3u8
    """

    if not url:
        return ""

    try:
        parsed = urlparse(url)

        if "away.php" not in parsed.path.lower():
            return url

        query = parse_qs(parsed.query)

        for key in ("to", "url", "u"):

            values = query.get(key)

            if values:

                real_url = unquote(values[0])

                real_url = normalize_url(
                    real_url
                )

                if real_url:
                    return real_url

    except Exception as exc:

        log.debug(
            "Ошибка unwrap VK URL: %s",
            exc,
        )

    return url


# ============================================================
# ПРОВЕРКА IPTV URL
# ============================================================

def is_probably_iptv_url(url: str) -> bool:
    """
    Определяем, является ли URL потенциальным IPTV,
    плейлистом или видеопотоком.
    """

    if not url:
        return False

    url = normalize_url(url)

    lower = url.lower()

    # UDP
    if lower.startswith("udp://"):
        return True

    # Явные расширения
    extensions = (
        ".m3u",
        ".m3u8",
        ".pls",
        ".txt",
        ".ts",
        ".mp4",
        ".flv",
        ".avi",
    )

    if any(
        extension in lower
        for extension in extensions
    ):
        return True

    # Типичные IPTV endpoints
    markers = (
        "get.php?",
        "playlist.php",
        "playlist?",
        "player_api.php",
        "type=m3u",
        "type=m3u_plus",
        "output=m3u",
        "output=m3u8",
        "m3u_plus",
        "/hls/",
        "/live/",
        "/stream/",
        "/video/",
    )

    if any(
        marker in lower
        for marker in markers
    ):
        return True

    return False


# ============================================================
# ФИЛЬТР VK
# ============================================================

def is_vk_internal_url(url: str) -> bool:
    """
    Проверяет, является ли URL внутренним VK.
    """

    try:

        parsed = urlparse(url)

        host = parsed.netloc.lower()

        if not host:
            return False

        vk_hosts = (
            "vk.com",
            "vk.ru",
            "m.vk.ru",
            "m.vk.com",
        )

        return any(
            host == item
            or host.endswith("." + item)
            for item in vk_hosts
        )

    except Exception:
        return False


# ============================================================
# DEDUP
# ============================================================

def deduplicate_preserve_order(items):
    """
    Удаление дублей с сохранением первоначального порядка.
    """

    result = []
    seen = set()

    for item in items:

        item = normalize_url(item)

        if not item:
            continue

        if item in seen:
            continue

        seen.add(item)

        result.append(item)

    return result


# ============================================================
# URL ИЗ ТЕКСТА
# ============================================================

def extract_urls_from_text(text: str):
    """
    Ищет URL в тексте.
    """

    if not text:
        return []

    found = []

    # M3U/M3U8/TXT/PLS/TS/MP4...
    found.extend(
        EXTENSION_URL_REGEX.findall(text)
    )

    # UDP
    found.extend(
        UDP_REGEX.findall(text)
    )

    # URL с параметрами
    found.extend(
        PARAMETER_URL_REGEX.findall(text)
    )

    return deduplicate_preserve_order(
        found
    )


# ============================================================
# URL ИЗ HTML
# ============================================================

def extract_urls_from_html(raw_html: str):
    """
    Извлечение URL непосредственно из HTML.

    Это принципиально важно:
    ссылка может быть не видна в тексте,
    но находиться в href/data-* или JSON.
    """

    if not raw_html:
        return []

    found = []

    decoded = html.unescape(
        raw_html
    )

    decoded = decoded.replace(
        "\\/",
        "/",
    )

    # --------------------------------------------------------
    # href="..."
    # --------------------------------------------------------

    href_regex = re.compile(
        r"""href\s*=\s*["']([^"']+)["']""",
        re.IGNORECASE,
    )

    for href in href_regex.findall(
        decoded
    ):

        href = normalize_url(href)

        if not href:
            continue

        if href.startswith("//"):
            href = "https:" + href

        elif href.startswith("/"):
            href = urljoin(
                "https://m.vk.ru",
                href,
            )

        if href.startswith(
            (
                "http://",
                "https://",
                "udp://",
            )
        ):
            found.append(href)

    # --------------------------------------------------------
    # data-url / data-href / data-link
    # --------------------------------------------------------

    data_regex = re.compile(
        r"""data-(?:url|href|link)\s*=\s*["']([^"']+)["']""",
        re.IGNORECASE,
    )

    for value in data_regex.findall(
        decoded
    ):

        value = normalize_url(value)

        if value.startswith(
            (
                "http://",
                "https://",
                "udp://",
            )
        ):
            found.append(value)

    # --------------------------------------------------------
    # URL с расширениями
    # --------------------------------------------------------

    found.extend(
        EXTENSION_URL_REGEX.findall(
            decoded
        )
    )

    # --------------------------------------------------------
    # URL с query
    # --------------------------------------------------------

    found.extend(
        PARAMETER_URL_REGEX.findall(
            decoded
        )
    )

    # --------------------------------------------------------
    # UDP
    # --------------------------------------------------------

    found.extend(
        UDP_REGEX.findall(
            decoded
        )
    )

    # --------------------------------------------------------
    # Общие URL
    # --------------------------------------------------------

    for url in GENERIC_URL_REGEX.findall(
        decoded
    ):

        url = normalize_url(url)

        if is_probably_iptv_url(url):
            found.append(url)

    return deduplicate_preserve_order(
        found
    )


# ============================================================
# СОХРАНЕНИЕ HTML
# ============================================================

async def save_html(
    page,
    filename: str,
):
    """
    Сохраняет фактически полученный DOM/HTML.
    """

    HTML_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = HTML_DIR / filename

    try:

        content = await page.content()

        path.write_text(
            content,
            encoding="utf-8",
        )

        log.info(
            "HTML сохранён: %s (%d байт)",
            path,
            len(
                content.encode(
                    "utf-8"
                )
            ),
        )

        return content

    except Exception as exc:

        log.exception(
            "Не удалось сохранить HTML: %s",
            exc,
        )

        return ""


# ============================================================
# ДИАГНОСТИКА СТРАНИЦЫ
# ============================================================

def detect_page_type(
    raw_html: str,
):
    """
    Определяет характер полученной страницы.

    Баннер не считается автоматически
    признаком отсутствия стены.
    """

    if not raw_html:
        return "EMPTY"

    lower = raw_html.lower()

    wall_markers = (
        "wall_post",
        "wall_posts",
        "post_id",
        "wall_reply",
        "wall_text",
        "post_author",
        "post_link",
    )

    auth_markers = (
        "авторизац",
        "войти",
        "login",
        "sign in",
        "password",
        "phone",
    )

    wall_score = sum(
        marker in lower
        for marker in wall_markers
    )

    auth_score = sum(
        marker in lower
        for marker in auth_markers
    )

    if (
        wall_score >= 2
        and auth_score >= 1
    ):
        return "WALL_WITH_BANNER"

    if wall_score >= 1:
        return "WALL"

    if auth_score >= 2:
        return "SERVICE_OR_AUTH"

    return "UNKNOWN"


# ============================================================
# DOM POST EXTRACTION
# ============================================================

async def extract_posts_from_dom(
    page,
):
    """
    Ищет посты различными селекторами.
    """

    selectors = [
        ".pm_post",
        ".wall_post",
        "[data-post-id]",
        "[id^='post-']",
        "[class*='wall_post']",
        "[class*='post']",
    ]

    elements = []

    for selector in selectors:

        try:

            found = await page.query_selector_all(
                selector
            )

            if found:

                log.info(
                    "DOM selector '%s': %d элементов",
                    selector,
                    len(found),
                )

                elements.extend(
                    found
                )

                if len(elements) >= MAX_POSTS_TO_CHECK:
                    break

        except Exception as exc:

            log.debug(
                "Ошибка selector '%s': %s",
                selector,
                exc,
            )

    posts = []

    signatures = set()

    for element in elements:

        try:

            text = await element.inner_text()

            inner_html = await element.inner_html()

            signature = (
                text[:1500],
                inner_html[:1500],
            )

            if signature in signatures:
                continue

            signatures.add(
                signature
            )

            links = []

            # ------------------------------------------------
            # Все A
            # ------------------------------------------------

            anchors = (
                await element.query_selector_all(
                    "a"
                )
            )

            for anchor in anchors:

                href = await anchor.get_attribute(
                    "href"
                )

                if not href:
                    continue

                href = normalize_url(
                    href
                )

                if href.startswith("/"):
                    href = urljoin(
                        "https://m.vk.ru",
                        href,
                    )

                if href.startswith(
                    (
                        "http://",
                        "https://",
                        "udp://",
                    )
                ):
                    links.append(
                        href
                    )

            # ------------------------------------------------
            # Текст
            # ------------------------------------------------

            links.extend(
                extract_urls_from_text(
                    text
                )
            )

            # ------------------------------------------------
            # HTML поста
            # ------------------------------------------------

            links.extend(
                extract_urls_from_html(
                    inner_html
                )
            )

            links = deduplicate_preserve_order(
                links
            )

            posts.append(
                {
                    "text": text,
                    "links": links,
                    "source": "DOM",
                }
            )

            if len(posts) >= MAX_POSTS_TO_CHECK:
                break

        except Exception as exc:

            log.debug(
                "Ошибка обработки DOM post: %s",
                exc,
            )

    return posts


# ============================================================
# DOCUMENT LINKS
# ============================================================

async def extract_document_links(
    page,
):
    """
    Ищет ссылки на VK documents.
    """

    result = []

    selectors = [
        "a[href*='doc']",
        "a[href*='/docs']",
        "a[href*='document']",
    ]

    for selector in selectors:

        try:

            elements = (
                await page.query_selector_all(
                    selector
                )
            )

            for element in elements:

                href = await element.get_attribute(
                    "href"
                )

                if not href:
                    continue

                href = normalize_url(
                    href
                )

                if href.startswith("/"):
                    href = urljoin(
                        "https://m.vk.ru",
                        href,
                    )

                result.append(
                    href
                )

        except Exception as exc:

            log.debug(
                "Ошибка документов: %s",
                exc,
            )

    return deduplicate_preserve_order(
        result
    )


# ============================================================
# ОДНА СТРАНИЦА
# ============================================================

async def scan_page(
    page,
    page_number: int,
):
    """
    Загружает и анализирует одну страницу стены.
    """

    offset = (
        page_number * PAGE_OFFSET
    )

    url = (
        f"{BASE_URL}"
        f"?offset={offset}"
    )

    log.info(
        "=" * 70
    )

    log.info(
        "СТРАНИЦА %d",
        page_number + 1,
    )

    log.info(
        "URL: %s",
        url,
    )

    response = None

    try:

        response = await page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=PAGE_TIMEOUT,
        )

    except PlaywrightTimeoutError:

        log.warning(
            "Timeout загрузки. "
            "Продолжаем анализ уже полученного DOM."
        )

    except Exception as exc:

        log.error(
            "Ошибка page.goto(): %s",
            exc,
        )

    if response:

        try:

            log.info(
                "HTTP status: %s",
                response.status,
            )

        except Exception:
            pass

    await asyncio.sleep(
        random.uniform(
            MIN_DELAY,
            MAX_DELAY,
        )
    )

    # --------------------------------------------------------
    # Полученный HTML
    # --------------------------------------------------------

    raw_html = await save_html(
        page,
        f"page_{page_number + 1:03d}.html",
    )

    page_type = detect_page_type(
        raw_html
    )

    log.info(
        "Тип страницы: %s",
        page_type,
    )

    # --------------------------------------------------------
    # 1. URL непосредственно из HTML
    # --------------------------------------------------------

    html_urls = extract_urls_from_html(
        raw_html
    )

    useful_html_urls = []

    for raw_url in html_urls:

        url = normalize_url(
            raw_url
        )

        url = unwrap_vk_away_url(
            url
        )

        if not url:
            continue

        if is_vk_internal_url(url):
            continue

        if is_probably_iptv_url(url):

            if url not in useful_html_urls:

                useful_html_urls.append(
                    url
                )

    log.info(
        "IPTV URL непосредственно в HTML: %d",
        len(useful_html_urls),
    )

    # --------------------------------------------------------
    # 2. Посты из DOM
    # --------------------------------------------------------

    posts = await extract_posts_from_dom(
        page
    )

    log.info(
        "Постов/DOM объектов: %d",
        len(posts),
    )

    # --------------------------------------------------------
    # 3. Документы
    # --------------------------------------------------------

    documents = (
        await extract_document_links(
            page
        )
    )

    log.info(
        "VK document links: %d",
        len(documents),
    )

    return {
        "page_type": page_type,
        "html_urls": useful_html_urls,
        "posts": posts,
        "documents": documents,
    }


# ============================================================
# ОБРАБОТКА URL
# ============================================================

def process_candidate_url(
    raw_url: str,
):
    """
    Приводит найденный URL к конечному виду
    и отбрасывает внутренние VK ссылки.
    """

    if not raw_url:
        return None

    url = normalize_url(
        raw_url
    )

    url = unwrap_vk_away_url(
        url
    )

    if not url:
        return None

    if is_vk_internal_url(url):
        return None

    if not is_probably_iptv_url(
        url
    ):
        return None

    return url


# ============================================================
# M3U
# ============================================================

def write_m3u(
    urls,
):
    """
    Формирует итоговый M3U.
    """

    urls = deduplicate_preserve_order(
        urls
    )

    with open(
        OUTPUT_FILE,
        "w",
        encoding="utf-8",
        newline="\n",
    ) as f:

        f.write(
            "#EXTM3U\n"
        )

        for index, url in enumerate(
            urls,
            1,
        ):

            lower = url.lower()

            if (
                ".m3u8" in lower
                or ".m3u" in lower
                or "type=m3u" in lower
                or "output=m3u" in lower
            ):

                name = (
                    f"Playlist {index}"
                )

            elif lower.startswith(
                "udp://"
            ):

                name = (
                    f"UDP Stream {index}"
                )

            elif (
                ".ts" in lower
                or "/hls/" in lower
                or "/live/" in lower
                or "/stream/" in lower
            ):

                name = (
                    f"IPTV Stream {index}"
                )

            else:

                name = (
                    f"Direct Stream {index}"
                )

            f.write(
                f"#EXTINF:-1,{name}\n"
            )

            f.write(
                f"{url}\n"
            )

    log.info(
        "Итоговый M3U записан: %s",
        OUTPUT_FILE,
    )


# ============================================================
# MAIN
# ============================================================

async def run():

    collected_urls = []

    log.info(
        "=" * 70
    )

    log.info(
        "VK IPTV PARSER START"
    )

    log.info(
        "GROUP_ID = %s",
        GROUP_ID,
    )

    log.info(
        "BASE_URL = %s",
        BASE_URL,
    )

    log.info(
        "=" * 70
    )

    async with async_playwright() as p:

        # ----------------------------------------------------
        # Chromium
        # ----------------------------------------------------

        browser = await p.chromium.launch(
            headless=True,
        )

        # ----------------------------------------------------
        # Browser Context
        #
        # User-Agent задаётся здесь.
        # page.set_user_agent() НЕ используется.
        # ----------------------------------------------------

        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="ru-RU",
            extra_http_headers={
                "Accept-Language":
                    "ru-RU,ru;q=0.9,en;q=0.8",
            },
        )

        page = await context.new_page()

        # ----------------------------------------------------
        # Основной цикл страниц
        # ----------------------------------------------------

        for page_number in range(
            MAX_PAGES
        ):

            if len(collected_urls) >= 10000:
                break

            try:

                result = await scan_page(
                    page,
                    page_number,
                )

            except Exception as exc:

                log.exception(
                    "Ошибка обработки страницы %d: %s",
                    page_number + 1,
                    exc,
                )

                continue

            # ------------------------------------------------
            # HTML URL
            # ------------------------------------------------

            for raw_url in result[
                "html_urls"
            ]:

                url = process_candidate_url(
                    raw_url
                )

                if (
                    url
                    and url not in collected_urls
                ):

                    collected_urls.append(
                        url
                    )

                    log.info(
                        "НАЙДЕНО [HTML]: %s",
                        url,
                    )

            # ------------------------------------------------
            # DOM posts
            # ------------------------------------------------

            for post in result[
                "posts"
            ]:

                for raw_url in post[
                    "links"
                ]:

                    url = process_candidate_url(
                        raw_url
                    )

                    if (
                        url
                        and url not in collected_urls
                    ):

                        collected_urls.append(
                            url
                        )

                        log.info(
                            "НАЙДЕНО [POST]: %s",
                            url,
                        )

            # ------------------------------------------------
            # Documents
            # ------------------------------------------------

            for raw_url in result[
                "documents"
            ]:

                url = process_candidate_url(
                    raw_url
                )

                if (
                    url
                    and url not in collected_urls
                ):

                    collected_urls.append(
                        url
                    )

                    log.info(
                        "НАЙДЕНО [DOCUMENT]: %s",
                        url,
                    )

            log.info(
                "После страницы %d найдено "
                "уникальных IPTV URL: %d",
                page_number + 1,
                len(collected_urls),
            )

            # ------------------------------------------------
            # Пауза перед следующей страницей
            # ------------------------------------------------

            await asyncio.sleep(
                random.uniform(
                    MIN_DELAY,
                    MAX_DELAY,
                )
            )

        # ----------------------------------------------------
        # Закрытие
        # ----------------------------------------------------

        await context.close()
        await browser.close()

    # ========================================================
    # FINAL DEDUP
    # ========================================================

    collected_urls = (
        deduplicate_preserve_order(
            collected_urls
        )
    )

    # ========================================================
    # РЕЗУЛЬТАТ
    # ========================================================

    log.info(
        "=" * 70
    )

    log.info(
        "СКАНИРОВАНИЕ ЗАВЕРШЕНО"
    )

    log.info(
        "УНИКАЛЬНЫХ IPTV URL: %d",
        len(collected_urls),
    )

    log.info(
        "=" * 70
    )

    if collected_urls:

        write_m3u(
            collected_urls
        )

        print()
        print(
            f"Готово. Найдено: "
            f"{len(collected_urls)}"
        )

        print(
            f"M3U: {OUTPUT_FILE}"
        )

        print(
            f"LOG: {LOG_FILE}"
        )

        print(
            f"HTML: {HTML_DIR}/"
        )

    else:

        log.warning(
            "IPTV URL не найдены."
        )

        log.warning(
            "Это НЕ означает автоматически, "
            "что группа/стена пустая."
        )

        log.warning(
            "Проверьте сохранённые HTML:"
        )

        log.warning(
            "%s",
            HTML_DIR,
        )

        log.warning(
            "И журнал:"
        )

        log.warning(
            "%s",
            LOG_FILE,
        )

        # Создаём пустой валидный M3U,
        # чтобы результат всегда существовал.
        write_m3u([])

        print()
        print(
            "IPTV-ссылки не найдены."
        )

        print(
            "Исходные HTML сохранены в:",
            HTML_DIR,
        )


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(
            run()
        )

    except KeyboardInterrupt:

        log.warning(
            "Остановлено пользователем."
        )

    except Exception as exc:

        log.exception(
            "КРИТИЧЕСКАЯ ОШИБКА: %s",
            exc,
        )