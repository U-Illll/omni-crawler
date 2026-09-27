#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_search — 搜索引擎适配器（360/搜狗/Bing）+ 降级链（v0.1）

统一接口：search(fetcher, query) -> {"engine": str, "results": [ {title, url, raw} ], "err": str|None}
- 结果 url：尽量解包成真实 URL（360/搜狗为跳转包装）
- 解包失败则保留 raw 跳转链接（后续抓页时再解）
"""
import html as html_mod
import re
import urllib.parse

from sm_common import audit, sha1, log

TAG_RE = re.compile(r"<[^>]+>")


def _clean(s):
    s = TAG_RE.sub("", s or "")
    s = html_mod.unescape(s)
    return re.sub(r"\s+", " ", s).strip()


def _unescape_bing(u):
    return html_mod.unescape(u)


# ---------------- 360 搜索 ----------------
def search_360(fetcher, query, page=1):
    url = "https://www.so.com/s?" + urllib.parse.urlencode({"q": query, "pn": str(page)})
    status, text, err = fetcher.get(url, referer="https://www.so.com/")
    if err:
        return {"engine": "so360", "results": [], "err": err}
    results = []
    # <h3 class="res-title"><a href="...">title</a>
    for m in re.finditer(
        r'<h3[^>]*class="[^"]*res-title[^"]*"[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
        text, re.S,
    ):
        href, title = m.group(1), _clean(m.group(2))
        if not href or href.startswith("javascript"):
            continue
        results.append({"title": title, "raw": href})
    return {"engine": "so360", "results": results, "err": None}


# ---------------- 搜狗搜索 ----------------
def search_sogou(fetcher, query, page=1):
    url = "https://www.sogou.com/web?" + urllib.parse.urlencode({"query": query, "page": str(page)})
    status, text, err = fetcher.get(url, referer="https://www.sogou.com/")
    if err:
        return {"engine": "sogou", "results": [], "err": err}
    results = []
    for m in re.finditer(r'<h3[^>]*>\s*<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>', text, re.S):
        href, title = m.group(1), _clean(m.group(2))
        if not href or href.startswith("javascript"):
            continue
        if href.startswith("/link?"):
            href = "https://www.sogou.com" + href
        results.append({"title": title, "raw": href})
    return {"engine": "sogou", "results": results, "err": None}


# ---------------- Bing ----------------
def search_bing(fetcher, query, page=1):
    url = "https://cn.bing.com/search?" + urllib.parse.urlencode(
        {"q": query, "first": str((page - 1) * 10 + 1)})
    status, text, err = fetcher.get(url, referer="https://cn.bing.com/")
    if err:
        return {"engine": "bing", "results": [], "err": err}
    results = []
    for m in re.finditer(r'<h2[^>]*><a[^>]*href="([^"]+)"[^>]*>(.*?)</a></h2>', text, re.S):
        href, title = _unescape_bing(m.group(1)), _clean(m.group(2))
        if not href.startswith("http"):
            continue
        results.append({"title": title, "raw": href})
    return {"engine": "bing", "results": results, "err": None}


ENGINES = [("so360", search_360), ("sogou", search_sogou), ("bing", search_bing)]


def search_with_failover(fetcher, query, prefer=None):
    """降级链：按顺序尝试引擎，成功即返回（R4′-2）"""
    order = ENGINES
    if prefer:
        order = sorted(ENGINES, key=lambda e: 0 if e[0] == prefer else 1)
    for name, fn in order:
        try:
            r = fn(fetcher, query)
        except Exception as e:  # noqa: BLE001
            r = {"engine": name, "results": [], "err": f"{type(e).__name__}: {e}"}
        audit({"kind": "search", "engine": r["engine"], "query": query,
               "n": len(r["results"]), "err": r.get("err")})
        if r["results"]:
            return r
        log(f"[search] {name} 空/失败({r.get('err')})，尝试下一引擎")
    return {"engine": None, "results": [], "err": "ALL_ENGINES_FAILED"}


def uncloak(fetcher, url, max_hops=4):
    """解包跳转链（支持 302 + JS window.location + meta refresh）；失败返回原 URL"""
    if not url:
        return url
    import urllib.request
    import urllib.error
    import html as _html

    u = url
    for _ in range(max_hops):
        if not any(k in u for k in ("so.com/link", "sogou.com/link", "baidu.com/link")):
            return u
        try:
            if fetcher is not None:
                fetcher.limiter.wait(urllib.parse.urlparse(u).netloc)  # 复用限速器（防密集）
            class NoRedirect(urllib.request.HTTPRedirectHandler):
                def redirect_request(self, *a, **k):
                    return None
            op = urllib.request.build_opener(NoRedirect)
            req = urllib.request.Request(u, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36",
                "Accept": "*/*",
            })
            loc, body = None, None
            try:
                r = op.open(req, timeout=12)
                loc = r.headers.get("Location")
                if not loc:
                    body = r.read(8192).decode("utf-8", "ignore")
            except urllib.error.HTTPError as e:
                loc = e.headers.get("Location")
                if not loc:
                    try:
                        body = e.read(8192).decode("utf-8", "ignore")
                    except Exception:  # noqa: BLE001
                        body = None
            if loc:
                u = urllib.parse.urljoin(u, loc)
                continue
            if body:
                m = (re.search(r'window\.location\.replace\(\s*["\']([^"\']+)["\']', body)
                     or re.search(r'location\.href\s*=\s*["\']([^"\']+)["\']', body)
                     or re.search(r'(?:URL=|url=)["\']?(https?://[^"\'>\s]+)', body))
                if m:
                    u = _html.unescape(m.group(1))
                    continue
            return u
        except Exception:  # noqa: BLE001
            return u
    return u
