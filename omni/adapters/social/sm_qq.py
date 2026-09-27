#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_qq — QQ 加群链接（k 链接）解析：qm.qq.com → 群号

实证（2026-09-23）：curl 直取 qm.qq.com/cgi-bin/qm/qr?k=xxx 页面
- 页面含 var rawuin = <群号>;
- 且含 hex 编码 JSON: param=7B2267726F757055696E223A... → {"groupUin":<群号>,...}
双重提取，任一命中即可。
"""
import json
import re
import urllib.request

from sm_common import audit, log

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def resolve_qm(k_url, timeout=20):
    """解析 qm.qq.com 加群链接 → dict(group_uin, rawuin, extra) 或 None"""
    if not k_url:
        return None
    if k_url.startswith("//"):
        k_url = "https:" + k_url
    try:
        req = urllib.request.Request(k_url, headers={"User-Agent": UA, "Accept-Language": "zh-CN,zh;q=0.9"})
        html = urllib.request.urlopen(req, timeout=timeout).read().decode("utf-8", "ignore")
    except Exception as e:  # noqa: BLE001
        audit({"kind": "qm_resolve_err", "url": k_url[:200], "err": str(e)[:120]})
        return None

    out = {"url": k_url, "group_uin": None, "rawuin": None, "k": None}
    m = re.search(r"k=([A-Za-z0-9]+)", k_url)
    if m:
        out["k"] = m.group(1)
    m = re.search(r"var\s+rawuin\s*=\s*(\d{5,12})", html)
    if m:
        out["rawuin"] = m.group(1)
    # hex param → JSON
    for m in re.finditer(r"param\s*=\s*[\"']([0-9A-Fa-f]{20,})[\"']", html):
        try:
            decoded = bytes.fromhex(m.group(1)).decode("utf-8", "ignore")
            j = json.loads(decoded)
            gu = j.get("groupUin") or j.get("group_uin")
            if gu:
                out["group_uin"] = str(gu)
                out["extra"] = {k2: j.get(k2) for k2 in ("timeStamp", "subcmd") if k2 in j}
                break
        except Exception:  # noqa: BLE001
            continue
    if not out["group_uin"] and out["rawuin"]:
        out["group_uin"] = out["rawuin"]
    audit({"kind": "qm_resolve", "k": out["k"], "group": out["group_uin"]})
    return out if out["group_uin"] else None


def resolve_jq(jq_url, timeout=20):
    """解析 jq.qq.com 加群链接 → 跟随 302 → group_code / qm 页解析"""
    if not jq_url:
        return None
    try:
        class NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None
        op = urllib.request.build_opener(NoRedirect)
        req = urllib.request.Request(jq_url, headers={"User-Agent": UA})
        loc = None
        try:
            r = op.open(req, timeout=timeout)
            loc = r.headers.get("Location")
        except urllib.error.HTTPError as e:
            loc = e.headers.get("Location")
        if not loc:
            return None
        # Location 里可能有 group_code
        m = re.search(r"group_code=(\d{5,12})", loc)
        if m:
            k = re.search(r"k=([A-Za-z0-9_-]+)", loc)
            audit({"kind": "jq_resolve", "group": m.group(1), "k": k.group(1) if k else None})
            return {"url": jq_url, "group_uin": m.group(1), "k": k.group(1) if k else None,
                    "via": "redirect"}
        # 否则从 qm 页解析
        r2 = resolve_qm(loc, timeout=timeout)
        if r2:
            r2["url"] = jq_url
            r2["via"] = "qm"
        return r2
    except Exception as e:  # noqa: BLE001
        audit({"kind": "jq_resolve_err", "url": jq_url[:200], "err": str(e)[:120]})
        return None


def resolve_any(url, timeout=20):
    """统一入口：qm.qq.com / jq.qq.com / qun.qq.com 链接 → 群号"""
    if not url:
        return None
    if "jq.qq.com" in url:
        return resolve_jq(url, timeout)
    if "qm.qq.com" in url:
        return resolve_qm(url, timeout)
    return None


if __name__ == "__main__":
    import sys
    for u in sys.argv[1:]:
        r = resolve_qm(u)
        print(r)
