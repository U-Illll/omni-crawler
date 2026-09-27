#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_qr — 二维码提取与解码（微信群/QQ群二维码 = 群的入口）

流程：页面 HTML → 提取图片 URL → 下载 → cv2 解码 → 二维码内容
输出内容可能是：群邀请链接 / 微信号 / 文本（如"群满加xx"）
"""
import os
import re
import urllib.request

from sm_common import RUNS, audit, log, sha1

IMG_DIR = os.path.join(RUNS, "images")
os.makedirs(IMG_DIR, exist_ok=True)

# 微信文章图片（mmbiz.qpic.cn）+ 通用图片
IMG_PATTERNS = [
    re.compile(r'data-src="(https?://mmbiz\.qpic\.cn/[^"]{10,600})"'),
    re.compile(r'<img[^>]+src="(https?://mmbiz\.qpic\.cn/[^"]{10,600})"'),
    re.compile(r'data-src="(https?://[^"]+\.(?:jpg|jpeg|png|gif)[^"]{0,300})"'),
]

QR_DIR = os.path.join(RUNS, "qr")
os.makedirs(QR_DIR, exist_ok=True)


def extract_images(html, limit=40):
    urls = []
    for pat in IMG_PATTERNS:
        for m in pat.finditer(html):
            u = m.group(1).replace("&amp;", "&")
            if u not in urls:
                urls.append(u)
    return urls[:limit]


def download_image(url, timeout=25):
    """下载图片到本地，返回路径（失败返回 None）"""
    ext = ".png" if ".png" in url.lower() else (".gif" if ".gif" in url.lower() else ".jpg")
    path = os.path.join(IMG_DIR, sha1(url)[:16] + ext)
    if os.path.exists(path):
        return path
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/131.0.0.0 Safari/537.36",
            "Referer": "https://mp.weixin.qq.com/",
        })
        data = urllib.request.urlopen(req, timeout=timeout).read()
        if len(data) < 100:
            return None
        with open(path, "wb") as f:
            f.write(data)
        audit({"kind": "img_download", "url": url[:200], "bytes": len(data)})
        return path
    except Exception as e:  # noqa: BLE001
        audit({"kind": "img_download_err", "url": url[:200], "err": str(e)[:100]})
        return None


def decode_qr(path):
    """解码单张图片中的二维码（返回内容列表）"""
    try:
        import cv2
    except ImportError:
        return []
    img = cv2.imread(path)
    if img is None:
        return []
    out = []
    try:
        det = cv2.QRCodeDetector()
        ok, decoded, points, _ = det.detectAndDecodeMulti(img)
        if ok and decoded is not None:
            out = [d for d in decoded if d]
    except Exception:  # noqa: BLE001
        pass
    if not out:
        try:
            det = cv2.QRCodeDetector()
            d, pts, _ = det.detectAndDecode(img)
            if d:
                out = [d]
        except Exception:  # noqa: BLE001
            pass
    return out


def scan_html_for_qr(html, max_images=20):
    """对页面 HTML 找图片→下载→解码，返回 [(img_url, [qr_contents])]"""
    results = []
    for u in extract_images(html, limit=max_images):
        p = download_image(u)
        if not p:
            continue
        codes = decode_qr(p)
        if codes:
            results.append((u, p, codes))
            log(f"[qr] 解码成功: {u[:80]} → {codes[:2]}")
            audit({"kind": "qr_decoded", "img": u[:200], "codes": codes[:5]})
    return results


if __name__ == "__main__":
    import sys
    # 测试：直接对一张图片路径/URL 解码
    arg = sys.argv[1]
    if arg.startswith("http"):
        p = download_image(arg)
    else:
        p = arg
    print("文件:", p)
    if p:
        print("解码:", decode_qr(p))
