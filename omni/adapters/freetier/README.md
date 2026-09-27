# freetier — 主流平台官方「免费机制」基线采集与变化监控

> 2026-09-27 建立。任务来源：Token 渠道情报的"补充爬取"需求（谷歌/OpenAI/GitHub 等
> 主流官方平台免费机制），要求不与既有内容重复。

## 目标与边界（「不重复」落地清单）

1. **采集域零重叠**：library（书目）/ social（社媒群号）/ tis / shuake 之外的新域——
   28 个官方页面（首跑 19 + 可反代 IDE/平台补 9，定价/免费层/活动）。
2. **与 Token 渠道卷宗的分工**：卷宗 = 人工调研**结论**（一次性快照，
   `Desktop/Token渠道-20260924/`）；本 adapter = 官方页面**原文基线 + 变化监控**
   （机器读数）——不产出人工结论、不复制卷宗条目、不与其重复。
3. **幂等纪律**：baseline 重复跑跳过已完成站点；refresh 未变化不产出记录、不动快照。
4. **store 幂等**：change 记录 key 含版本号（`<site>#v<N>`），按 (kind,key) 唯一。
5. **快照版本化**：仅首次/变化时新增 `v<N>.txt`（版本号单调，不覆盖历史）。

## 用法

```bash
cd ~/go/omni-crawler
python3 -m omni.adapters.freetier.main baseline   # 首跑建基线（幂等）
python3 -m omni.adapters.freetier.main refresh    # 复核变化（幂等）
python3 -m omni.adapters.freetier.main status     # 站点状态表
python3 -m omni.adapters.freetier.main report     # 生成 runs/report.md
```

## 数据位置

| 路径 | 内容 |
|---|---|
| `state/snapshots/<site_id>/v<N>.txt` | 快照（textified 原文，含 sha256 头） |
| `state/snapshots/<site_id>/meta.json` | 站点状态（status/last_sha/versions/last_change_ts…） |
| `state/changes.jsonl` | 变化账本（追加；含 diff 预览） |
| `runs/report.md` | 最新报告（状态表+变化+失败+关键词命中） |
| `runs/ logs/` | 框架 Store / 审计（`audit.jsonl` 含每请求读数） |

## 已实测的特殊处理（2026-09-27）

- **ai.google.dev（2 页）**：直连 302（signin OAuth 跳转）→ **两步握手**：首个请求
  `allow_redirects=False` 捕获 signin cookie → 第二次请求 200（257KB 完整页）。
  代码：`adapter._fetch_site` 的 `handshake` 分支 + `fetch/http.py` 的 `allow_redirects` 参数。
- **openai.com**：CF 校验**头部一致性**（UA 与 sec-ch-ua 版本必须同源 + Sec-Fetch-* 全套）。
  出口无关（普通 7890 / 住宅 7894/7895 均可）。代码：`BROWSER_HEADERS`。
- **登录页防误存**：handshake 站点若正文是 Google 登录页（`_looks_like_login`）→ 记
  failed（防把 OAuth 登录页当内容存快照）。

## 已知限制（2026-09-27 实测登记）

1. **A/B 变体抖动**：营销页（如 openai.com/chatgpt/pricing）可能在不同请求间返回
   文案 A/B 变体——refresh 会如实记录为"变化"（diff 预览中表现为几行文案往返）。
   人工看 `changes.jsonl` 的 diff 可秒判；低频运行下噪声可接受。未来可选优化=
   双读确认或历史版本识别（当前不做，保持简单透明）。
2. **JS 渲染页**：groq.com/pricing 为前端渲染页，L0（HTTP）仅拿到框架文本
   （922 字符）；需完整渲染时走 L1 浏览器执行体（升级链）——当前登记不阻塞。
3. **瞬时偏差自愈**：首抓若遇边缘瞬时重定向（如 deepseek 一次拿到相邻页），
   下一轮 refresh 会以正确内容建立新版本（实测已自愈）；diff 记录在案。
4. **检测器假阳性（notion-pricing，2026-09-27 已修复）**：`omni/anticrawl/detect.py`
   的 CHALLENGE 关键词 `'waf'` 曾命中 Notion 页面里 Next.js 随机 chunk 文件名
   （`/_next/static/chunks/2ydmx5jsxqwaf.css`）→ 首抓被判 `[blocked] CHALLENGE`。
   **修复**：框架移除 `'waf'` 宽词（真 WAF 页由 "just a moment"/"checking your browser"
   等信号覆盖）；修复后 refresh → notion-pricing ok（28/28 全绿，实测）。
5. **轻量 L0 文本（2026-09-27 补录）**：trae-pricing（1415 字符）、
   google-antigravity（4477 字符）以框架文本为主；需抓全定价细节时走 L1 升级链。

## 验证

- 离线：`python3 tests/smoke_freetier.py`（协议/握手/变化检测/失败路径/幂等；无网络）
- 真实网络：`baseline` 首跑 → `runs/report.md`（28 站状态）

## 坐标

- 框架：`~/go/omni-crawler/`（omni.core.engine 驱动；见 docs/runbook.md）
- 站点清单：`sources.py`（28 站 = 定稿 19 + 补 9「可反代 IDE/平台」；2026-09-27）
- 任务档案：`Desktop/校园网/03-omni-crawler实施报告-20260927.md`（追加节）+ 本 README
