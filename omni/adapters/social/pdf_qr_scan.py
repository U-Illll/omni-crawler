#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""提取图片型 PDF 的页面图片并扫描二维码"""
import io
import sys

import cv2
import numpy as np
from pypdf import PdfReader

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sm-recon/recon/ia_fj_723648c5"
r = PdfReader(path)
print(f"共 {len(r.pages)} 页")
detector = cv2.QRCodeDetector()
total = 0
for pi, page in enumerate(r.pages):
    try:
        imgs = list(page.images)
    except Exception as e:  # noqa: BLE001
        print(f"p{pi}: images 失败 {str(e)[:60]}")
        continue
    for ii, img in enumerate(imgs):
        try:
            data = img.data
            arr = cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
            if arr is None or arr.size < 10000:
                continue
            ok, decoded, pts, _ = detector.detectAndDecodeMulti(arr)
            codes = [d for d in (decoded or []) if d]
            if codes:
                for c in codes:
                    print(f"p{pi}#{ii} ({arr.shape}) → {c[:160]}")
                    total += 1
        except Exception as e:  # noqa: BLE001
            pass
print(f"QR 合计: {total}")
