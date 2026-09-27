#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.llm.client — LLM 多通道客户端（降级链）。

从 sm-recon sm_llm.py 移植（R4′-1 降级链）：
  1. oc-bridge 127.0.0.1:8791（本机专用，opencode go 经 Windows 桥）
  2. opencode go 直连
  3. Agnes（agnes-2.5-flash）
  4. NVIDIA NIM（短超时快速失败位）
  5. DeepSeek 官方
  6. opencode-dsv4
- 失败熔断：某通道连续失败 → 本轮跳过 120s
- 全部失败 → 抛 LLMUnavailable（调用方降级为规则逻辑）
- 审计每调用一行
"""
import json
import os
import socket
import time
import urllib.error
import urllib.request

from ..core.log import audit, log

ENV_PATHS = [
    "/root/.opencode-keys.env",
    "/root/.gateway.env",
    os.path.expanduser("~/.cache/gateway/.env"),
    os.path.expanduser("~/go/opencode-keys.env"),
    os.path.expanduser("~/.opencode-keys.env"),
]

_session_id = f"omni-{int(time.time())}"


def load_env():
    env = dict(os.environ)
    for p in ENV_PATHS:
        try:
            if not os.path.exists(p):
                continue
            with open(p, encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    if k and v and k not in env:
                        env[k] = v
        except Exception:  # noqa: BLE001
            continue
    return env


def _port_open(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def default_channels():
    env = load_env()
    chans = []
    if _port_open("127.0.0.1", 8791):
        chans.append({
            "name": "oc-bridge", "url": "http://127.0.0.1:8791/v1/chat/completions",
            "key": "local-dev-key", "model": "deepseek-v4.1-flash", "headers": {},
        })
    if env.get("OPENCODE_GO_API_KEY"):
        chans.append({
            "name": "opencode-go", "url": "https://opencode.ai/zen/go/v1/chat/completions",
            "key": env["OPENCODE_GO_API_KEY"], "model": "deepseek-v4.1-flash",
            "headers": {"x-opencode-session": _session_id},
        })
    if env.get("PROVIDER_KEY_AGNES"):
        chans.append({
            "name": "agnes", "url": "https://apihub.agnes-ai.com/v1/chat/completions",
            "key": env["PROVIDER_KEY_AGNES"], "model": "agnes-2.5-flash", "headers": {},
        })
    if env.get("PROVIDER_KEY_NVIDIA"):
        chans.append({
            "name": "nvidia", "url": "https://integrate.api.nvidia.com/v1/chat/completions",
            "key": env["PROVIDER_KEY_NVIDIA"], "model": "deepseek-ai/deepseek-v4.1-flash",
            "headers": {}, "timeout": 25,
        })
    if env.get("PROVIDER_KEY_DEEPSEEK"):
        chans.append({
            "name": "deepseek-official", "url": "https://api.deepseek.com/v1/chat/completions",
            "key": env["PROVIDER_KEY_DEEPSEEK"], "model": "deepseek-v4-pro", "headers": {},
        })
    if env.get("OPENCODE_DSV4_API_KEY"):
        chans.append({
            "name": "opencode-dsv4", "url": "https://opencode.ai/zen/go/v1/chat/completions",
            "key": env["OPENCODE_DSV4_API_KEY"], "model": "deepseek-v4.1-flash",
            "headers": {"x-opencode-session": _session_id},
        })
    return chans


class LLMUnavailable(Exception):
    pass


class LLMClient:
    def __init__(self, channels=None, timeout=60):
        self.channels = channels if channels is not None else default_channels()
        self.timeout = timeout
        self.fail_until = {}

    def chat(self, messages, max_tokens=1600, temperature=0.2, purpose="generic"):
        tried = []
        for ch in self.channels:
            name = ch["name"]
            if time.time() < self.fail_until.get(name, 0):
                continue
            try:
                text = self._call(ch, messages, max_tokens, temperature)
                if text:
                    audit({"kind": "llm", "channel": name, "purpose": purpose,
                           "ok": True, "len": len(text)})
                    return text, name
                raise RuntimeError("empty response")
            except Exception as e:  # noqa: BLE001
                tried.append(f"{name}: {type(e).__name__}")
                self.fail_until[name] = time.time() + 120
                audit({"kind": "llm", "channel": name, "purpose": purpose,
                       "ok": False, "err": f"{type(e).__name__}: {e}"[:200]})
                log(f"[llm] 通道 {name} 失败({type(e).__name__})，尝试下一通道")
        raise LLMUnavailable("ALL_CHANNELS_FAILED: " + " | ".join(tried))

    def _call(self, ch, messages, max_tokens, temperature):
        body = json.dumps({
            "model": ch["model"], "messages": messages,
            "max_tokens": max_tokens, "temperature": temperature,
        }).encode("utf-8")
        h = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {ch['key']}",
            # 上游（opencode go / CF 层）会拦截 Python-urllib UA → 403（sm-recon 实测）
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        }
        h.update(ch.get("headers", {}))
        req = urllib.request.Request(ch["url"], data=body, headers=h, method="POST")
        try:
            r = urllib.request.urlopen(req, timeout=ch.get("timeout") or self.timeout)
            data = json.loads(r.read().decode("utf-8", "ignore"))
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"HTTP {e.code}: {e.read()[:160].decode('utf-8', 'ignore')}")
        msg = (data.get("choices") or [{}])[0].get("message", {})
        content = (msg.get("content") or "").strip()
        if not content:
            rc = (msg.get("reasoning_content") or "").strip()
            if rc:
                content = rc
        return content
