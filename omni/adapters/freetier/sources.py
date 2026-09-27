#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.adapters.freetier.sources — 目标站点清单（2026-09-27 定稿 19 站 + 补 9 站 = 28 站）。

字段：
- id        站点键（store/meta/快照目录名；全局唯一）
- vendor    平台
- area      产品/域
- title     人类可读标题（报告用）
- url       目标页面（https）
- handshake 可选 True：需 signin 两步握手（仅 ai.google.dev 两页，2026-09-27 实测）

「不重复」纪律：本清单只覆盖官方公开页面的**原文采集**；
与 library/social/tis/shuake 采集域零重叠；与 Token 渠道卷宗的分工见 README
（卷宗=人工调研结论快照；本 adapter=官方原文基线+变化监控，不产出人工结论）。
"""

SITES = [
    # ---------- Google（ai.google.dev 两页需 signin 两步握手） ----------
    {"id": "google-gemini-pricing", "vendor": "Google", "area": "Gemini API",
     "title": "Gemini API Pricing（含免费层条款）",
     "url": "https://ai.google.dev/gemini-api/docs/pricing",
     "handshake": True},
    {"id": "google-gemini-rate-limits", "vendor": "Google", "area": "Gemini API",
     "title": "Gemini API Rate Limits（免费层 RPM/RPD/TPM）",
     "url": "https://ai.google.dev/gemini-api/docs/rate-limits",
     "handshake": True},
    {"id": "google-gemini-code-assist", "vendor": "Google", "area": "Gemini Code Assist",
     "title": "Gemini Code Assist 概览（个人免费档）",
     "url": "https://developers.google.com/gemini-code-assist/docs/overview"},
    {"id": "google-gemini-blog", "vendor": "Google", "area": "Gemini",
     "title": "Google Gemini 官方博客（活动/促销动态）",
     "url": "https://blog.google/products/gemini/"},
    # ---------- OpenAI ----------
    {"id": "openai-chatgpt-pricing", "vendor": "OpenAI", "area": "ChatGPT",
     "title": "ChatGPT Pricing（Free/Plus 权益）",
     "url": "https://openai.com/chatgpt/pricing/"},
    {"id": "openai-api-pricing", "vendor": "OpenAI", "area": "OpenAI API",
     "title": "OpenAI API Pricing",
     "url": "https://openai.com/api/pricing/"},
    {"id": "openai-news", "vendor": "OpenAI", "area": "OpenAI",
     "title": "OpenAI News（新政策/活动）",
     "url": "https://openai.com/news/"},
    # ---------- GitHub ----------
    {"id": "github-copilot-plans", "vendor": "GitHub", "area": "Copilot",
     "title": "GitHub Copilot Plans（Free 档细则）",
     "url": "https://docs.github.com/en/copilot/get-started/plans"},
    {"id": "github-copilot-features", "vendor": "GitHub", "area": "Copilot",
     "title": "GitHub Copilot 功能页",
     "url": "https://github.com/features/copilot"},
    {"id": "github-pricing", "vendor": "GitHub", "area": "GitHub",
     "title": "GitHub Pricing",
     "url": "https://github.com/pricing"},
    {"id": "github-education-pack", "vendor": "GitHub", "area": "GitHub Education",
     "title": "GitHub Student Developer Pack",
     "url": "https://education.github.com/pack"},
    {"id": "github-models-about", "vendor": "GitHub", "area": "GitHub Models",
     "title": "GitHub Models 说明（含免费额度）",
     "url": "https://docs.github.com/en/github-models/about-github-models"},
    # ---------- 扩展（主流 LLM 平台官方页） ----------
    {"id": "anthropic-pricing", "vendor": "Anthropic", "area": "Claude",
     "title": "Anthropic Pricing",
     "url": "https://www.anthropic.com/pricing"},
    {"id": "mistral-pricing", "vendor": "Mistral", "area": "La Plateforme",
     "title": "Mistral Pricing（免费档）",
     "url": "https://mistral.ai/pricing"},
    {"id": "cohere-pricing", "vendor": "Cohere", "area": "Cohere",
     "title": "Cohere Pricing（Trial key 条款）",
     "url": "https://cohere.com/pricing"},
    {"id": "groq-pricing", "vendor": "Groq", "area": "GroqCloud",
     "title": "Groq Pricing（免费档限制）",
     "url": "https://groq.com/pricing"},
    {"id": "deepseek-pricing", "vendor": "DeepSeek", "area": "DeepSeek API",
     "title": "DeepSeek API Pricing",
     "url": "https://api-docs.deepseek.com/quick_start/pricing"},
    {"id": "cloudflare-workers-ai-pricing", "vendor": "Cloudflare", "area": "Workers AI",
     "title": "Workers AI Pricing（每日免费额度）",
     "url": "https://developers.cloudflare.com/workers-ai/platform/pricing/"},
    {"id": "xai-api", "vendor": "xAI", "area": "Grok API",
     "title": "xAI API（免费额度说明）",
     "url": "https://x.ai/api"},
    # ---------- 可反代 IDE/平台（2026-09-27 补） ----------
    # 探活（curl -L -x 127.0.0.1:7890，BROWSER_HEADERS 同源）：9/10 候选可用；
    # perplexity.ai 全变体 403/JS 空壳（25 字符）→ 未收录。
    # 注：notion-pricing 首跑 meta=failed（[blocked] CHALLENGE）系检测器假阳性——
    # omni/anticrawl/detect.py 的关键词 'waf' 命中 Notion 页面里 Next.js 随机 chunk
    # 文件名（/_next/static/chunks/2ydmx5jsxqwaf.css）；正文 22340 字符为真实定价页，
    # direct / mihomo 两出口读数一致。待检测词表收紧后 refresh 复核即可转为 ok。
    {"id": "google-antigravity", "vendor": "Google", "area": "Antigravity",
     "title": "Google Antigravity 官方站（Agent-first IDE/CLI）",
     "url": "https://antigravity.google/"},
    {"id": "cursor-pricing", "vendor": "Cursor", "area": "Cursor",
     "title": "Cursor Pricing（Hobby 免费档/学生优惠）",
     "url": "https://cursor.com/pricing"},
    {"id": "windsurf-pricing", "vendor": "Windsurf", "area": "Windsurf",
     "title": "Windsurf Pricing（现 301 至 Devin/Cognition 定价页）",
     "url": "https://windsurf.com/pricing"},
    {"id": "zed-pricing", "vendor": "Zed", "area": "Zed",
     "title": "Zed Pricing（Free 档/试用额度）",
     "url": "https://zed.dev/pricing"},
    {"id": "kiro-pricing", "vendor": "AWS", "area": "Kiro",
     "title": "Kiro Pricing（KIRO FREE 档/学生优惠）",
     "url": "https://kiro.dev/pricing/"},
    {"id": "amazonq-developer", "vendor": "AWS", "area": "Amazon Q Developer",
     "title": "Amazon Q Developer Pricing（免费层条款）",
     "url": "https://aws.amazon.com/q/developer/pricing/"},
    {"id": "trae-pricing", "vendor": "ByteDance", "area": "TRAE",
     "title": "TRAE Pricing（Free $0 档）",
     "url": "https://www.trae.ai/pricing"},
    {"id": "notion-pricing", "vendor": "Notion", "area": "Notion",
     "title": "Notion Pricing（Free/Plus 档，含学生优惠）",
     "url": "https://www.notion.com/pricing"},
    {"id": "m365-copilot", "vendor": "Microsoft", "area": "Microsoft 365 Copilot",
     "title": "Microsoft 365 Copilot 计划与定价（Free/Trial 条款）",
     "url": "https://www.microsoft.com/en-us/microsoft-365/copilot/pricing"},
]

# 报告关键词监控表（快照文本命中计数；大小写不敏感）
WATCH_WORDS = ["free tier", "free plan", "free trial", "free credits",
               "promotion", "promo", "bonus", "check-in", "student"]
