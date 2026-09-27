#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.fetch.escalate — 执行体升级链（L0→L1→L2→L3 决策）。

根据 L0/L1 结果与页面特征给出下一步建议（纯决策，无网络副作用）：
- ok                成功
- retry_later       限流/暂时性 → 等冷却/退避后重试
- switch_egress     疑似出口问题（channel 失败）→ 换出口
- escalate_browser  空壳页 / 挑战页 → 升级 L1 浏览器执行体
- needs_reverse     签名壁垒 → 生成逆向任务单（L3，模式二逆向桥）
- give_up           确定性失败（fatal）→ 放弃该目标

fetch_smart()：L0→L1 的自动升级便利函数（L2/L3 由 agent 工作流接管）。
"""
import json
import os
import time

from ..anticrawl.detect import like_empty_shell
from ..core.log import audit, runs_dir


def plan_from_result(result, page_empty=None, signature_error=False):
    """输入 FetchResult，输出 {"action":..., "reason":...}。"""
    cls = result.cls
    if cls == "ok":
        if signature_error:
            return {"action": "needs_reverse", "reason": "signature_error_marker"}
        if page_empty is None and result.text is not None:
            page_empty = like_empty_shell(result.text)
        if page_empty:
            return {"action": "escalate_browser", "reason": "empty_shell"}
        return {"action": "ok", "reason": "success"}
    if cls == "channel":
        return {"action": "switch_egress", "reason": "channel_unavailable"}
    if cls == "circuit_open":
        return {"action": "retry_later", "reason": "circuit_cooldown"}
    if cls == "blocked":
        err = (result.err or "")
        if "CHALLENGE" in err:
            return {"action": "escalate_browser", "reason": "challenge_page"}
        return {"action": "retry_later", "reason": "block_sign_cooldown"}
    if cls == "limited":
        return {"action": "retry_later", "reason": "rate_limited"}
    if cls == "transient":
        return {"action": "retry_later", "reason": "transient_exhausted"}
    if cls == "fatal":
        # 403 是模糊地带：可能是认证失败，也可能是反爬 WAF 拒绝 → 浏览器升级试一次
        if result.status == 403:
            return {"action": "escalate_browser", "reason": "forbidden_waf_suspect"}
        return {"action": "give_up", "reason": "fatal_status"}
    return {"action": "retry_later", "reason": f"unknown_cls:{cls}"}


def write_reverse_ticket(url, reason, evidence, sample=None):
    """生成逆向任务单（L3 入口）。落盘 runs/reverse-tickets/，返回路径。
    任务单结构对齐模式二"任务书"风格（目标/样本/期望/证据）。"""
    d = os.path.join(runs_dir(), "reverse-tickets")
    os.makedirs(d, exist_ok=True)
    ts = time.strftime("%Y%m%dT%H%M%S")
    path = os.path.join(d, f"{ts}-{abs(hash(url)) % 10**8}.md")
    lines = [
        f"# 逆向任务单（{ts}）",
        "",
        f"- 目标 URL：{url}",
        f"- 触发原因：{reason}",
        f"- 证据：{evidence}",
        "",
        "## 样本（请求/响应摘要）",
        "```",
        json.dumps(sample or {}, ensure_ascii=False, indent=1)[:4000],
        "```",
        "",
        "## 期望产出",
        "1. param-blueprint.md（算法/密钥/调用链/边界）",
        "2. signers/<target>/ 纯本地复现插件（Node/Python）",
        "3. 回归验证读数（重放真实请求对照）",
        "",
        "> 流程见 docs/runbook.md『逆向桥』；工具链：frx-director 44 工具。",
    ]
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    audit({"kind": "reverse_ticket", "url": url, "reason": reason, "path": path})
    return path


def fetch_smart(http, browser, url, env="AUTO", allow_browser=True, fallback_url=None,
                impersonate="chrome"):
    """L0 → L0+（协议指纹）→ （按需）L1 自动升级。返回 (FetchResult, stage)。
    stage ∈ {"http", "http-impersonated", "browser-escalated", "browser-fallback-http"}。
    L0+（2026-09-27 加入）：TLS/协议指纹重试（curl_cffi）——对 blocked/fatal-403
    类先过一道协议层增强，再决定是否升级浏览器。"""
    r = http.get(url, env=env)
    plan = plan_from_result(r)
    if (plan["action"] in ("retry_later", "escalate_browser")
            and plan.get("reason") != "empty_shell"   # 空壳=JS 渲染问题，协议层无用，直接上浏览器
            and impersonate and hasattr(http, "get_impersonated")):
        r2 = http.get_impersonated(url, impersonate=impersonate, env=env)
        if r2.ok() and r2.text:
            return r2, "http-impersonated"
        if len(r2.text or "") > len(r.text or ""):
            r = r2
        plan = plan_from_result(r)
    if plan["action"] == "escalate_browser" and allow_browser and browser is not None:
        audit({"kind": "escalate", "url": url, "from": "http", "to": "browser",
               "reason": plan["reason"]})
        rb = browser.fetch(fallback_url or url, env=env)
        if rb.ok():
            return rb, "browser-escalated"
        # 浏览器也失败：返回原始结果与浏览器结果中信息更多者
        return (rb if rb.text else r), "browser-fallback-http"
    return r, "http"
