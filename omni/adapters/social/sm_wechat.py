#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_wechat — 搜狗微信通道（D2）：搜索文章 → 解链 → 抓正文（v0.1）

流程（实证 2026-09-23）：
  1. 预热首页（拿 SNUID cookie）
  2. 文章搜索 weixin.sogou.com/weixin?type=2
  3. 解 /link?url= → JS 拼接（url += '...'）→ mp.weixin.qq.com/s?src=11&...
  4. 抓 mp 文章 → 标题 + 正文文本（js_content 起 150KB 窗口）

风控：搜狗侧 ≥4s/req；跳 antispider 即熔断冷却。
"""
import html as html_mod
import http.cookiejar
import re
import time
import urllib.parse
import urllib.request

from sm_common import audit, log

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
TAG_RE = re.compile(r"<[^>]+>")


def _plain(s):
    return re.sub(r"\s+", " ", html_mod.unescape(TAG_RE.sub(" ", s or ""))).strip()


class WechatChannel:
    def __init__(self, interval=4.5):
        self.interval = interval
        self.last_req = 0.0
        self.cj = http.cookiejar.CookieJar()
        self.op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))
        self.op.addheaders = [("User-Agent", UA), ("Accept-Language", "zh-CN,zh;q=0.9")]
        self.warmed = False
        self.blocked_until = 0.0

    def _wait(self):
        gap = time.time() - self.last_req
        if gap < self.interval:
            time.sleep(self.interval - gap + (time.time() % 1.0))
        self.last_req = time.time()

    def _get(self, url, referer=None, timeout=25, read=2560 * 1024):
        if time.time() < self.blocked_until:
            raise RuntimeError(f"wechat channel cooling {int(self.blocked_until - time.time())}s")
        self._wait()
        h = {}
        if referer:
            h["Referer"] = referer
        req = urllib.request.Request(url, headers=h)
        r = self.op.open(req, timeout=timeout)
        body = r.read(read).decode("utf-8", "ignore")
        final = r.geturl()
        if "antispider" in final:
            self.blocked_until = time.time() + 300
            audit({"kind": "wechat_block", "url": url})
            log("[风控] 搜狗微信 antispider 命中 → 冷却 300s")
            raise RuntimeError("antispider hit")
        return final, body

    def warm(self):
        if self.warmed:
            return
        try:
            self._get("https://weixin.sogou.com/")
            self.warmed = True
            names = [c.name for c in self.cj]
            audit({"kind": "wechat_warm", "cookies": names})
        except Exception as e:  # noqa: BLE001
            log(f"[wechat] 预热失败: {e}")

    def search(self, query, page=1):
        self.warm()
        url = "https://weixin.sogou.com/weixin?" + urllib.parse.urlencode(
            {"type": "2", "query": query, "ie": "utf8", "page": str(page)})
        final, body = self._get(url, referer="https://weixin.sogou.com/")
        articles = []
        for m in re.finditer(
            r'<h3>\s*<a[^>]*href="(/link\?url=[^"]+)"[^>]*>(.*?)</a>\s*</h3>(.*?)(?=<h3>|</ul>)',
            body, re.S,
        ):
            href, title, tail = m.group(1), _plain(m.group(2)), m.group(3)
            acct = re.search(r'class="account"[^>]*>(.*?)</a>', tail)
            date = re.search(r"document\.write\(timeConvert\('(\d+)'\)\)", tail)
            digest = re.search(r'<p class="txt-info"[^>]*>(.*?)</p>', tail, re.S)
            articles.append({
                "href": re.sub(r"\s+", "", href).replace("&amp;", "&"),
                "title": title,
                "account": _plain(acct.group(1)) if acct else "",
                "ts": int(date.group(1)) if date else None,
                "digest": _plain(digest.group(1)) if digest else "",
                "query": query,
            })
        audit({"kind": "wechat_search", "query": query, "n": len(articles)})
        return articles

    def resolve(self, href):
        """解链 → mp.weixin.qq.com URL"""
        full = "https://weixin.sogou.com" + href if href.startswith("/") else href
        final, body = self._get(full, referer="https://weixin.sogou.com/weixin?type=2")
        frags = re.findall(r"url\s*\+=\s*'([^']*)'", body)
        mp_url = "".join(frags)
        if mp_url.startswith("https://mp.weixin.qq.com"):
            return mp_url
        # 兜底：直接从 body 找完整 URL
        m = re.search(r"(https://mp\.weixin\.qq\.com/s\?[^\"'\s<>]+)", body)
        return m.group(1) if m else None

    def article(self, mp_url):
        """抓文章 → 标题 + 正文文本 + 原始 HTML"""
        final, art = self._get(mp_url, referer="https://weixin.sogou.com/")
        title = ""
        for pat in (
            r'<h1[^>]*id="activity-name"[^>]*>(.*?)</h1>',
            r'<meta property="og:title" content="([^"]+)"',
            r"var msg_title = ['\"]([^'\"]+)['\"]",
        ):
            m = re.search(pat, art, re.S)
            if m:
                title = _plain(m.group(1))
                break
        pos = art.find('id="js_content"')
        text = ""
        if pos >= 0:
            text = _plain(art[pos: pos + 400000])
        else:
            text = _plain(art[:400000])
        audit({"kind": "wechat_article", "url": mp_url[:200], "title": title[:80], "len": len(text)})
        return {"url": mp_url, "title": title, "text": text, "html": art[:2500000]}


if __name__ == "__main__":
    import sys
    ch = WechatChannel()
    q = sys.argv[1] if len(sys.argv) > 1 else "南方科技大学 留学生 群"
    arts = ch.search(q)
    print(f"搜索 {q}: {len(arts)} 篇")
    for a in arts[:5]:
        print(" -", a["title"][:70], "|", a["account"][:20])
    if arts:
        mp = ch.resolve(arts[0]["href"])
        print("解链:", (mp or "FAIL")[:120])
        if mp:
            art = ch.article(mp)
            print("标题:", art["title"])
            print("正文长:", len(art["text"]))
            nums = re.findall(r"群[号]?[^\d]{0,8}(\d{5,11})", art["text"])
            print("群号:", nums[:10])
