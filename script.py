#!/usr/bin/env python3
"""
Scrape Dataset 9 file list from DOJ website and download PDFs in batches.
Uses Playwright to handle age verification and pagination.
"""
 
import asyncio
import importlib.util
import json
import random
import re
import sys
from pathlib import Path

 
BASE_URL = "https://www.justice.gov/epstein/doj-disclosures/data-set-9-files"
OUTPUT_DIR = Path(r"D:\Epstein Files\Dataset9")
INDEX_FILE = OUTPUT_DIR / "dataset9_index.json"
STATE_FILE = OUTPUT_DIR / "dataset9_state.json"
BATCH_SIZE = None  # None means no per-batch limit; scrape/download everything found
HEADLESS = False
SLOW_MO_MS = 50
SCRAPE_RETRIES = 5
DOWNLOAD_RETRIES = 5
PAGE_TIMEOUT_MS = 60000
DOWNLOAD_TIMEOUT_MS = 120000
LINK_RE = re.compile(r'href="([^"]*/epstein/files/[^"]+\.pdf)"', re.IGNORECASE)
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
EXTRA_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
}
DOWNLOAD_CHUNK_SIZE = 1_048_576  # 1MB chunks to avoid large memory use
async_playwright = None
aiohttp = None


def _playwright_available():
    global async_playwright
    if async_playwright is not None:
        return True
    if importlib.util.find_spec("playwright") is None:
        return False
    from playwright.async_api import async_playwright as playwright_async
    async_playwright = playwright_async
    return True


def _ensure_aiohttp():
    global aiohttp
    if aiohttp is not None:
        return
    if importlib.util.find_spec("aiohttp") is None:
        print(
            "Missing dependency: aiohttp.\n"
            "Install it with:\n"
            "  pip install aiohttp"
        )
        raise SystemExit(1)
    import aiohttp as aiohttp_module
    aiohttp = aiohttp_module


def _load_json(path, default):
    if path.exists():
        with open(path, "r") as f:
            return json.load(f)
    return default


def _save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _load_index():
    return _load_json(INDEX_FILE, [])


def _save_index(all_files):
    _save_json(INDEX_FILE, all_files)


def _load_state():
    return _load_json(STATE_FILE, {"next_page": 0, "max_page": None})


def _save_state(state):
    _save_json(STATE_FILE, state)


def _existing_file_names():
    if not OUTPUT_DIR.exists():
        return set()
    return {p.name for p in OUTPUT_DIR.glob("*") if p.is_file()}


def _cookie_dict_from_list(cookie_list):
    return {c["name"]: c["value"] for c in cookie_list}
 
def _age_cookies():
    return [
        {
            "name": "justiceGovAgeVerified",
            "value": "true",
            "domain": "www.justice.gov",
            "path": "/"
        },
        {
            "name": "justiceGovAgeVerified",
            "value": "true",
            "domain": ".justice.gov",
            "path": "/"
        }
    ]


async def _add_age_cookies(context):
    await context.add_cookies(_age_cookies())


def _extract_links_from_html(html):
    matches = LINK_RE.findall(html or "")
    seen = set()
    links = []
    for href in matches:
        if href not in seen:
            seen.add(href)
            links.append(href)
    return links


def _save_debug_html(html, page_num):
    if not html:
        return
    debug_path = OUTPUT_DIR / f"debug_page_{page_num}.html"
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)


def _is_access_denied(html):
    if not html:
        return False
    lowered = html.lower()
    # Be strict to avoid false positives from the age-gate hidden error block.
    if "you don't have permission to access" in lowered:
        return True
    if "errors.edgesuite.net" in lowered:
        return True
    if "reference #" in lowered and "access denied" in lowered:
        return True
    return False


async def _maybe_accept_age_gate(page):
    gate_text = page.locator("text=Are you 18 years of age or older?")
    try:
        if await gate_text.is_visible():
            yes_button = page.get_by_role("button", name="Yes")
            if await yes_button.count() > 0:
                await yes_button.first.click()
            else:
                for selector in [
                    'button:has-text("Yes")',
                    'input[value="Yes"]',
                    'a:has-text("Yes")'
                ]:
                    btn = page.locator(selector)
                    if await btn.count() > 0:
                        await btn.first.click()
                        break
            await page.wait_for_load_state("networkidle")
            return True
    except Exception:
        return False
    return False


async def _ensure_age_verified(page):
    try:
        await page.goto(BASE_URL, timeout=PAGE_TIMEOUT_MS)
        await page.wait_for_load_state("domcontentloaded")
        await _maybe_accept_age_gate(page)
    except Exception:
        pass


async def _load_dataset_page(page, page_num):
    await page.goto(f"{BASE_URL}?page={page_num}", timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
    await page.wait_for_load_state("domcontentloaded")
    accepted = await _maybe_accept_age_gate(page)
    if accepted:
        await page.goto(f"{BASE_URL}?page={page_num}", timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
        await page.wait_for_load_state("domcontentloaded")


async def _page_is_access_denied(page):
    try:
        html = await page.content()
        return _is_access_denied(html)
    except Exception:
        return False


async def _collect_links_from_page(page):
    links = []
    link_handles = await page.query_selector_all('a[href*="/epstein/files/"]')
    for handle in link_handles:
        href = await handle.get_attribute("href")
        if href:
            links.append(href)
    if links:
        return links
    html = await page.content()
    return _extract_links_from_html(html)


async def _get_max_page(page):
    print("Finding total pages...")
    await _load_dataset_page(page, 0)

    last_link = await page.query_selector('a[aria-label="Last page"]')
    if last_link:
        href = await last_link.get_attribute("href")
        max_page = int(href.split("page=")[1])
        print(f"Found {max_page + 1} pages (0 to {max_page})")
        return max_page

    print("Could not find last page link, using manual max...")
    return 20500  # Dataset 9 has ~20,450 pages


async def _scrape_pages_for_batch(batch_size, all_files, file_set, state):
    """Scrape pages until we collect batch_size new files or reach the end."""
    new_files = []
    existing_files = _existing_file_names()
    if not _playwright_available():
        print(
            "Playwright not available; falling back to HTTP-only scraping. "
            "Install Playwright + Chromium for more reliable results."
        )
        return await _scrape_pages_for_batch_http(
            batch_size, all_files, file_set, state, existing_files
        )

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
        context = await browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers=EXTRA_HEADERS
        )

        await _add_age_cookies(context)

        page = await context.new_page()
        await _ensure_age_verified(page)

        # Navigate sequentially via UI to keep Akamai session happy.
        await page.goto(f"{BASE_URL}?page=0", timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
        await _maybe_accept_age_gate(page)

        # Advance to the resume page using the "Next page" button to preserve cookies/tokens.
        target_page = state.get("next_page", 0)
        current_page = 0
        while current_page < target_page:
            next_btn = page.get_by_role("link", name="Next page")
            if await next_btn.count() == 0:
                next_btn = page.locator('a[aria-label="Next page"]')
            if await next_btn.count() == 0:
                break
            await next_btn.first.click()
            await page.wait_for_load_state("domcontentloaded")
            current_page += 1

        # Main loop: click Next for each subsequent page
        while batch_size is None or len(new_files) < batch_size:
            print(f"Scraping page {current_page}...")

            if await _page_is_access_denied(page):
                html = await page.content()
                _save_debug_html(html, current_page)
                print("  Access denied detected. Stopping for resume.")
                break

            links = await _collect_links_from_page(page)
            if not links:
                html = await page.content()
                _save_debug_html(html, current_page)
                print(f"No files found on page {current_page}, stopping.")
                break

            page_new_files = []
            for href in links:
                filename = href.split("/")[-1]
                if filename not in file_set:
                    file_set.add(filename)
                    record = {
                        "filename": filename,
                        "url": f"https://www.justice.gov{href}" if href.startswith("/") else href,
                        "downloaded": filename in existing_files
                    }
                    all_files.append(record)
                    new_files.append(record)
                    page_new_files.append(record)
                    if batch_size is not None and len(new_files) >= batch_size:
                        break

            print(f"  Found {len(links)} links, total unique files: {len(all_files)}")

            if page_new_files:
                print(f"  Downloading {len(page_new_files)} new files from page {current_page}...")
                downloaded, skipped, failed = await _download_batch(page_new_files, all_files)
                _save_index(all_files)
                if failed > 0:
                    print("  Some downloads failed on this page; will resume later.")

            current_page += 1
            state["next_page"] = current_page
            _save_state(state)
            _save_index(all_files)

            # Move to next page via UI. If no next button, stop.
            next_btn = page.get_by_role("link", name="Next page")
            if await next_btn.count() == 0:
                next_btn = page.locator('a[aria-label="Next page"]')
            if await next_btn.count() == 0:
                print("No Next page button; stopping.")
                break
            await next_btn.first.click()
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(800 + random.randint(0, 800))

        await browser.close()

    return new_files
 
 
async def _scrape_pages_for_batch_http(batch_size, all_files, file_set, state, existing_files):
    """HTTP-only scrape when Playwright is unavailable."""
    new_files = []
    _ensure_aiohttp()

    timeout = aiohttp.ClientTimeout(total=PAGE_TIMEOUT_MS / 1000 + 30)
    headers = {"User-Agent": USER_AGENT, **EXTRA_HEADERS}
    cookie_jar = aiohttp.CookieJar()
    cookie_jar.update_cookies(
        _cookie_dict_from_list(_age_cookies()),
        response_url="https://www.justice.gov/"
    )

    current_page = state.get("next_page", 0)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, cookie_jar=cookie_jar) as session:
        while batch_size is None or len(new_files) < batch_size:
            print(f"Scraping page {current_page} (HTTP fallback)...")
            url = f"{BASE_URL}?page={current_page}"
            try:
                async with session.get(url, allow_redirects=True) as resp:
                    if resp.status != 200:
                        print(f"  HTTP {resp.status} for page {current_page}. Stopping.")
                        break
                    html = await resp.text(errors="ignore")
            except Exception as exc:
                print(f"  Request failed for page {current_page}: {exc}")
                break

            if _is_access_denied(html):
                _save_debug_html(html, current_page)
                print("  Access denied detected. Stopping for resume.")
                break

            links = _extract_links_from_html(html)
            if not links:
                _save_debug_html(html, current_page)
                print(f"No files found on page {current_page}, stopping.")
                break

            page_new_files = []
            for href in links:
                filename = href.split("/")[-1]
                if filename not in file_set:
                    file_set.add(filename)
                    record = {
                        "filename": filename,
                        "url": f"https://www.justice.gov{href}" if href.startswith("/") else href,
                        "downloaded": filename in existing_files
                    }
                    all_files.append(record)
                    new_files.append(record)
                    page_new_files.append(record)
                    if batch_size is not None and len(new_files) >= batch_size:
                        break

            print(f"  Found {len(links)} links, total unique files: {len(all_files)}")

            if page_new_files:
                print(f"  Downloading {len(page_new_files)} new files from page {current_page}...")
                downloaded, skipped, failed = await _download_batch(page_new_files, all_files)
                _save_index(all_files)
                if failed > 0:
                    print("  Some downloads failed on this page; will resume later.")

            current_page += 1
            state["next_page"] = current_page
            _save_state(state)
            _save_index(all_files)

            await asyncio.sleep(0.5)

    return new_files


async def _download_batch(batch, all_files):
    """Download a batch of file records."""
    if not batch:
        return 0, 0, 0
    _ensure_aiohttp()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    existing_files = _existing_file_names()

    # Create an HTTP session for streaming large files without loading into memory
    timeout = aiohttp.ClientTimeout(total=DOWNLOAD_TIMEOUT_MS / 1000 + 60)
    headers = {"User-Agent": USER_AGENT, **EXTRA_HEADERS}

    cookie_jar = aiohttp.CookieJar()
    if _playwright_available():
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO_MS)
            context = await browser.new_context(
                user_agent=USER_AGENT,
                locale="en-US",
                timezone_id="America/New_York",
                extra_http_headers=EXTRA_HEADERS
            )
            await _add_age_cookies(context)
            page = await context.new_page()
            await _ensure_age_verified(page)
            context_cookies = await context.cookies()
            await browser.close()
        cookie_jar.update_cookies(_cookie_dict_from_list(context_cookies))
    else:
        cookie_jar.update_cookies(
            _cookie_dict_from_list(_age_cookies()),
            response_url="https://www.justice.gov/"
        )
    async with aiohttp.ClientSession(headers=headers, timeout=timeout, cookie_jar=cookie_jar) as session:

        downloaded = 0
        skipped = 0
        failed = 0

        for i, file_info in enumerate(batch, start=1):
            filename = file_info["filename"]
            url = file_info["url"]
            output_path = OUTPUT_DIR / filename

            if output_path.exists() or filename in existing_files:
                file_info["downloaded"] = True
                skipped += 1
                _save_index(all_files)
                continue

            print(f"[{i}/{len(batch)}] Downloading {filename}...")

            success = False
            for attempt in range(1, DOWNLOAD_RETRIES + 1):
                try:
                    async with session.get(url, allow_redirects=True) as resp:
                        if resp.status == 404:
                            print("  HTTP 404: skipping (marked missing)")
                            file_info["downloaded"] = True
                            file_info["missing"] = True
                            skipped += 1
                            success = True
                            break
                        if resp.status != 200:
                            raise RuntimeError(f"HTTP {resp.status}")
                        content_type = resp.headers.get("content-type", "").lower()
                        if "text/html" in content_type:
                            # Likely age gate or error page
                            text_sample = await resp.text(errors="ignore")
                            raise RuntimeError(f"HTML response: {content_type}")
                        with open(output_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(DOWNLOAD_CHUNK_SIZE):
                                if chunk:
                                    f.write(chunk)
                    file_info["downloaded"] = True
                    downloaded += 1
                    success = True
                    break
                except Exception as e:
                    print(f"  Download attempt {attempt} failed: {e}")
                    await asyncio.sleep(2 * attempt)

            if not success:
                failed += 1

            _save_index(all_files)
            await asyncio.sleep(0.3)

    return downloaded, skipped, failed


async def download_files(start_from=0):
    """Download PDFs from the index."""
    if not INDEX_FILE.exists():
        print("Index file not found. Run scrape first.")
        return

    all_files = _load_index()
    batch = all_files[start_from:]
    downloaded, skipped, failed = await _download_batch(batch, all_files)

    _save_index(all_files)
    print(f"\nDone! Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")


async def auto_scrape_and_download(batch_size=BATCH_SIZE):
    """Scrape in batches of N files, then download each batch before continuing."""
    all_files = _load_index()
    file_set = {f["filename"] for f in all_files}
    state = _load_state()

    while True:
        pending = [f for f in all_files if not f.get("downloaded")]
        if pending:
            batch = pending if batch_size is None else pending[:batch_size]
            print(f"Downloading existing pending batch: {len(batch)} files")
            downloaded, skipped, failed = await _download_batch(batch, all_files)
            _save_index(all_files)
            print(f"Batch done. Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")
            if failed > 0:
                print("Some downloads failed. You can rerun to retry.")
                break
            continue

        new_files = await _scrape_pages_for_batch(batch_size, all_files, file_set, state)
        if not new_files:
            print("No new files found to scrape. All done.")
            break

        print(f"Scraped {len(new_files)} new files. Downloading batch...")
        downloaded, skipped, failed = await _download_batch(new_files, all_files)
        _save_index(all_files)
        print(f"Batch done. Downloaded: {downloaded}, Skipped: {skipped}, Failed: {failed}")
        if failed > 0:
            print("Some downloads failed. You can rerun to retry.")
            break
 
 
async def main():
    import sys

    # Optional args:
    #   auto [batch_size]
    #   download [start_index]
    if len(sys.argv) < 2:
        cmd = "auto"
        args = []
    else:
        cmd = sys.argv[1].lower()
        args = sys.argv[2:]

    if cmd == "auto":
        if len(args) > 0 and args[0].lower() not in ("none", "all"):
            batch_size = int(args[0])
        else:
            batch_size = None
        await auto_scrape_and_download(batch_size)
    elif cmd == "scrape":
        print("Scrape-only mode is deprecated in this script. Use auto mode.")
        if len(args) > 0 and args[0].lower() not in ("none", "all"):
            batch_size = int(args[0])
        else:
            batch_size = None
        await auto_scrape_and_download(batch_size)
    elif cmd == "download":
        start = int(args[0]) if len(args) > 0 else 0
        await download_files(start)
    else:
        print("Usage:")
        print("  python script.py auto [batch_size|all]   - Scrape/download in batches (default: all)")
        print("  python script.py download [start]        - Download from index starting at file #start")
 
 
if __name__ == '__main__':
    asyncio.run(main())
