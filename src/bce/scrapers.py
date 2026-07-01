"""Scrapers CBSO, Stapor, eJustice, KBO + helpers navigateur/Tor."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from bce import config
from bce.utils import normalize_bce, tva_from_bce

try:
    from stem import Signal
    from stem.control import Controller
    _HAS_STEM = True
except ImportError:
    _HAS_STEM = False


# --- Tor / HTTP ---


def _socks_url() -> str:
    return f"socks5h://{config.TOR_SOCKS_HOST}:{config.TOR_SOCKS_PORT}"


def requests_proxies() -> dict[str, str]:
    url = _socks_url()
    return {"http": url, "https": url}


def playwright_proxy() -> dict[str, str] | None:
    return {"server": _socks_url()} if config.TOR_SOCKS_HOST else None


def renew_tor_identity(wait_seconds: float = 5.0) -> bool:
    if not _HAS_STEM:
        return False
    try:
        with Controller.from_port(address=config.TOR_CONTROL_HOST, port=config.TOR_CONTROL_PORT) as c:
            if config.TOR_CONTROL_PASSWORD:
                c.authenticate(password=config.TOR_CONTROL_PASSWORD)
            else:
                c.authenticate()
            c.signal(Signal.NEWNYM)
        if wait_seconds:
            time.sleep(wait_seconds)
        return True
    except Exception:
        return False


def _http_session(use_tor: bool) -> requests.Session:
    s = requests.Session()
    if use_tor:
        s.proxies.update(requests_proxies())
    s.headers.update({"User-Agent": config.USER_AGENT})
    return s


def _launch_browser(playwright, proxy: dict | None = None):
    kwargs = {"headless": True, "args": ["--disable-blink-features=AutomationControlled"]}
    return playwright.chromium.launch(**kwargs)


def _browser_context(browser, proxy: dict | None = None):
    ctx_kwargs = {"user_agent": config.USER_AGENT, "locale": "fr-BE", "viewport": {"width": 1440, "height": 900}}
    if proxy:
        ctx_kwargs["proxy"] = proxy
    ctx = browser.new_context(**ctx_kwargs)
    ctx.add_init_script('Object.defineProperty(navigator, "webdriver", {get: () => undefined})')
    return ctx


# --- CBSO / NBB ---

CBSO_BASE = "https://consult.cbso.nbb.be"


def is_consolidated(dep: dict) -> bool:
    model = (dep.get("modelId") or dep.get("modelName") or "").lower()
    return model.startswith("mc-") or "consolid" in model


def select_deposits(deposits: list[dict]) -> dict[int, dict]:
    by_year: dict[int, tuple[tuple, dict]] = {}
    for dep in deposits:
        if is_consolidated(dep):
            continue
        year = dep.get("periodEndDateYear") or int(str(dep.get("periodEndDate", ""))[:4] or 0)
        if not year:
            continue
        lang = (dep.get("language") or "FR").upper()[:2]
        score = (0 if lang == "FR" else 1 if lang == "NL" else 2, dep.get("depositDate") or "")
        if year not in by_year or score < by_year[year][0]:
            by_year[year] = (score, dep)
    return {y: d for y, (_, d) in sorted(by_year.items())}


def is_valid_csv_bytes(raw: bytes) -> bool:
    if raw.lstrip().startswith(b"<!") or raw.lstrip().startswith(b"<html"):
        return False
    return b"Reference number" in raw or b'"70"' in raw


@dataclass
class CBSODepositArtifact:
    bce_number: str
    year: int
    deposit_id: str
    artifact_type: str
    model: str | None = None
    language: str | None = None
    deposit_date: str | None = None


class CBSOScraper:
    def __init__(self, use_tor: bool = False):
        self._proxy = playwright_proxy() if use_tor else None

    def _session(self):
        from playwright.sync_api import sync_playwright
        pw = sync_playwright().start()
        browser = _launch_browser(pw, self._proxy)
        return pw, browser, _browser_context(browser, self._proxy)

    def list_deposits(self, bce_number: str) -> list[dict]:
        n = normalize_bce(bce_number)
        pw, browser, ctx = self._session()
        try:
            page = ctx.new_page()
            page.goto(f"{CBSO_BASE}/consult-enterprise/{n}", wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(3000)
            api, deposits, page_idx, size = ctx.request, [], 0, 100
            while True:
                url = f"{CBSO_BASE}/api/rs-consult/published-deposits?page={page_idx}&size={size}&enterpriseNumber={n}&sort=periodEndDate,desc&sort=depositDate,desc"
                data = api.get(url).json()
                batch = data.get("content") or []
                deposits.extend(batch)
                if data.get("last") or len(batch) < size:
                    break
                page_idx += 1
            return deposits
        finally:
            browser.close()
            pw.stop()

    def discover_artifacts(self, bce_number: str) -> list[CBSODepositArtifact]:
        n = normalize_bce(bce_number)
        out: list[CBSODepositArtifact] = []
        for year, dep in select_deposits(self.list_deposits(n)).items():
            dep_id = dep["id"]
            base = dict(bce_number=n, year=year, deposit_id=dep_id, model=dep.get("modelId") or dep.get("modelName"),
                        language=dep.get("language"), deposit_date=(dep.get("depositDate") or "")[:10] or None)
            out.append(CBSODepositArtifact(**base, artifact_type="pdf"))
            if year >= 2021:
                out.append(CBSODepositArtifact(**base, artifact_type="csv"))
        return out

    def download_deposit_file(self, bce_number: str, deposit_id: str, artifact_type: str) -> bytes:
        n = normalize_bce(bce_number)
        pw, browser, ctx = self._session()
        try:
            page = ctx.new_page()
            page.goto(f"{CBSO_BASE}/consult-enterprise/{n}", wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(2000)
            if artifact_type == "pdf":
                url = f"{CBSO_BASE}/api/external/broker/public/deposits/pdf/{deposit_id}"
            elif artifact_type == "csv":
                url = f"{CBSO_BASE}/api/external/broker/public/deposits/consult/csv/{deposit_id}"
            else:
                raise ValueError(f"Unknown artifact_type {artifact_type}")
            body = ctx.request.get(url).body()
            if artifact_type == "csv" and not is_valid_csv_bytes(body):
                raise ValueError("Invalid CSV response (HTML or missing headers)")
            return body
        finally:
            browser.close()
            pw.stop()


# --- Stapor ---

STAPOR_BASE = "https://statuts.notaire.be/stapor_v1"


@dataclass
class StaporArtifact:
    bce_number: str
    document_id: str
    title: str
    date: str | None
    download_url: str


class StaporScraper:
    def __init__(self, use_tor: bool = False, max_retries: int = 3):
        self._proxy = playwright_proxy() if use_tor else None
        self.max_retries = max_retries

    def discover_statutes(self, bce_number: str) -> list[StaporArtifact]:
        n = normalize_bce(bce_number)
        blocks = None
        for _ in range(self.max_retries):
            blocks = self._fetch_blocks(n)
            if blocks is not None:
                break
        if not blocks:
            return []
        out, seen = [], set()
        for block in blocks:
            for s in block.get("statutes") or []:
                key = s.get("statutesHistoryId") or s.get("documentId")
                if key in seen:
                    continue
                seen.add(key)
                doc_id = s.get("documentId")
                if not doc_id:
                    continue
                out.append(StaporArtifact(
                    bce_number=n, document_id=str(doc_id), title=s.get("documentTitle") or "Statuts",
                    date=(s.get("deedDate") or s.get("lastModificationDate") or "")[:10] or None,
                    download_url=f"{STAPOR_BASE}/api/documents/{doc_id}/content",
                ))
        return out

    def _fetch_blocks(self, n: str) -> list | None:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = _launch_browser(p, self._proxy)
            ctx = _browser_context(browser, self._proxy)
            page = ctx.new_page()
            page.goto(f"{STAPOR_BASE}/enterprise/{n}/statutes", wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(3000)
            api, blocks, offset, limit, total = ctx.request, [], 0, 5, None
            while total is None or offset < total:
                resp = api.get(f"{STAPOR_BASE}/api/enterprises/{n}/statutes?offset={offset}&limit={limit}")
                if resp.status != 200 or "json" not in resp.headers.get("content-type", ""):
                    browser.close()
                    return None
                data = resp.json()
                blocks.append(data)
                if total is None:
                    total = data.get("totalItems", 0)
                offset += len(data.get("statutes") or [])
                if offset >= total:
                    break
            browser.close()
            return blocks

    def download_document(self, bce_number: str, document_id: str) -> bytes:
        from playwright.sync_api import sync_playwright
        n = normalize_bce(bce_number)
        with sync_playwright() as p:
            browser = _launch_browser(p, self._proxy)
            ctx = _browser_context(browser, self._proxy)
            page = ctx.new_page()
            page.goto(f"{STAPOR_BASE}/enterprise/{n}/statutes", wait_until="domcontentloaded", timeout=120000)
            page.wait_for_timeout(2000)
            body = ctx.request.get(f"{STAPOR_BASE}/api/documents/{document_id}/content").body()
            browser.close()
            return body


# --- eJustice ---

@dataclass
class EJusticePublication:
    bce_number: str
    numac: str
    date: str | None
    pub_type: str | None
    pdf_url: str


class EJusticeScraper:
    def __init__(self, use_tor: bool = False):
        self.use_tor = use_tor
        self.session = _http_session(use_tor)
        if not use_tor:
            self.session.headers.update({"Accept-Language": "fr-BE,fr;q=0.9"})

    def discover_publications(self, moniteur_list_url: str, bce_number: str) -> list[EJusticePublication]:
        if not moniteur_list_url:
            return []
        base = "https://www.ejustice.just.fgov.be"
        parsed = urlparse(moniteur_list_url.split("#")[0])
        qs = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        pubs, page = [], 1
        while True:
            qs["page"] = str(page)
            page_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(qs)}"
            r = self.session.get(page_url, timeout=60, proxies=requests_proxies() if self.use_tor else None)
            r.encoding = "iso-8859-1"
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            items = soup.select("div.list-item")
            if not items:
                break
            for item in items:
                title = item.select_one(".list-item--title")
                title_txt = title.get_text(" ", strip=True) if title else item.get_text(" ", strip=True)
                dm = re.search(r"(\d{4}-\d{2}-\d{2})\s*/\s*(\d+)", title_txt)
                tm = re.search(r"\d{3}\.\d{3}\.\d{3}\s+(.+?)\s+\d{4}-\d{2}-\d{2}\s*/", title_txt)
                pdf = item.find("a", href=re.compile(r"tsv_pdf|\.pdf", re.I))
                if not pdf or not dm:
                    continue
                pubs.append(EJusticePublication(
                    bce_number=bce_number, numac=dm.group(2), date=dm.group(1),
                    pub_type=tm.group(1).strip() if tm else None, pdf_url=urljoin(base, pdf["href"]),
                ))
            pages = {int(p) for p in re.findall(r"page=(\d+)", r.text)}
            if page >= max(pages, default=page):
                break
            page += 1
        return pubs

    def download_pdf(self, pdf_url: str) -> bytes:
        r = self.session.get(pdf_url, timeout=120, proxies=requests_proxies() if self.use_tor else None)
        r.raise_for_status()
        return r.content


# --- KBO web ---

KBO_BASE = "https://kbopub.economie.fgov.be/kbopub"


class KBOWebScraper:
    def __init__(self, use_tor: bool = False):
        self.use_tor = use_tor
        self._proxy = playwright_proxy() if use_tor else None

    def fetch_external_links(self, bce_number: str) -> dict[str, str]:
        # kbopub bloque les requêtes non-navigateur (503) → passage par Playwright
        from playwright.sync_api import sync_playwright

        n = normalize_bce(bce_number)
        with sync_playwright() as p:
            browser = _launch_browser(p, self._proxy)
            ctx = _browser_context(browser, self._proxy)
            page = ctx.new_page()
            page.goto(
                f"{KBO_BASE}/toonondernemingps.html?lang=fr&ondernemingsnummer={n}",
                wait_until="domcontentloaded",
                timeout=120000,
            )
            page.wait_for_timeout(1500)
            html = page.content()
            browser.close()
        soup = BeautifulSoup(html, "lxml")
        return {a.get_text(" ", strip=True): a["href"] for a in soup.select("div#table a.external")}

    def get_moniteur_url(self, bce_number: str) -> str | None:
        return self.fetch_external_links(bce_number).get("Publications au Moniteur belge")

    def get_tva(self, bce_number: str) -> str:
        return tva_from_bce(bce_number)
