#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""PDF 图片 QR 扫描 v2（PIL 兜底解码 + 多尺度）"""
import io
import sys

import cv2
import numpy as np
from PIL import Image
from pypdf import PdfReader

path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/sm-recon/recon/ia_fj_723648c5"
r = PdfReader(path)
detector = cv2.QRCodeDetector()
total = 0
for pi, page in enumerate(r.pages):
    try:
        imgs = list(page.images)
    except Exception:
        continue
    for ii, img in enumerate(imgs):
        try:
            pil = Image.open(io.BytesIO(img.data)).convert("RGB")
        except Exception as e:  # noqa: BLE001
            print(f"p{pi}: PIL 失败 {str(e)[:50]}")
            continue
        arr = cv2.cvtColor(np.array(pil), cv2.COLOR_RGB2BGR)
        print(f"p{pi}: {arr.shape}")
        for scale in (1.0, 0.6, 1.4):
            a = arr if scale == 1.0 else cv2.resize(arr, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
            try:
                ok, decoded, pts, _ = detector.detectAndDecodeMulti(a)
                for d in (decoded or []):
                    if d:
                        print(f"   p{pi}#s{scale} → {d[:160]}")
                        total += 1
            except Exception:
                pass
print(f"QR 合计: {total}")
