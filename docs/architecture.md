# 全能爬虫 · 架构设计草案 v0.1（2026-09-27）

> **目标（用户原话）**：盘点所有爬虫后，按《爬虫5轮优化工程交接文档-20260918》标准，接入标准模式二的各种逆向工具以突破反爬，制作一个能够在南科大内网、国内公网和外网通用的全能爬虫，以后爬虫类任务均交给它。
>
> **输入**：《爬虫资产盘点报告-20260927》（同目录 01 号文档）。
> **状态**：草案，待拍板（见 §10）。

---

## 1. 设计目标与标准

### 1.1 功能目标（4 条）

1. **统一采集框架**：一个核心引擎 + 适配器插件；新爬虫任务 = 写一个 adapter，不再从零造。
2. **三环境通用**：同一任务代码在「南科大内网 / 国内公网 / 外网」下自动选路（声明式通道），双端部署（本机 WSL + 阿里云）按需分配。
3. **反爬对抗体系化**：运行态自动对抗（限速/指纹/出口/升级执行体）+ 工程态逆向桥（模式二浏览器域 44 工具 → 签名还原 → 插件回注）。
4. **长程自动化**：无人值守长跑（keeper / 断点 / 自愈 / 审计），即"以后把任务丢给它就不管了"。

### 1.2 标准映射（《爬虫5轮优化工程》硬指标体系 → 框架设计）

| 原指标 | 框架落点 |
|---|---|
| A1 连续 72h 无人值守 | keeper + 心跳 + 审计（框架级默认） |
| A2 进程被杀 ≤180s 恢复 | keeper 继承 sm-recon 16s 实测 / R3 keeper-v3 锁族 |
| A3 断电恢复一致性 | 断点事务（原子写 + reconcile + repair） |
| B1 稳态吞吐（adapter 级） | 自适应限速器（每 adapter 定标） |
| B3 限流红线 30min 零 429 | 限速器 + 熔断 + 审计事件 |
| C1 请求级重试≥3 + 退避 | retry 族继承（R3 抽取） |
| C2 块级兜底 | reconcile / rebuild（R3 抽取） |
| C3 进程级 keeper | keeper（继承） |
| D1 多通道 fallback ≤30s | **通道层**（新设计，核心增量） |
| D2 错误四分类 100% 可审计 | fetch 层统一出口 + 分类器 + 审计 |
| D3 熔断 | circuit（含 DR 教训修正） |
| E1-E2 数据一致性/覆盖 | records 事务 + 快照复核 |

### 1.3 DR 终审教训 → 设计对策（**必读，防重蹈**）

| DR 教训（终审裁决） | 本设计对策 |
|---|---|
| 熔断"空转事件"（404×30 写 circuit_open 但状态不变） | 熔断器状态机断言：审计事件必须伴随可验证状态变化（closed→open→half-open→closed 每步带读数） |
| 负速率 / 负 ETA（计数回退） | 监控计数重置语义：重置→null+原因（不钳零伪装） |
| 限速器参数可放宽红线（min_interval=0.05） | 限速器唯一入口 + 硬上限编译期常量（不可经参数放宽） |
| 验收判据可伪造（全桩目录 ⇒ exit 0） | fail-closed 判据 + **run 身份绑定**（独立控制方记录 run_id/pid/证据摘要，写者不可改） |
| 修复语义冲突（fatal 4xx 是否熔断） | 目标卡显式定义：fatal（认证/不存在）与 transient（限流/抖动）分离，熔断只对 transient |
| "登记即消项" | 未达成项保持 FAIL/缺证状态传播，不豁免 |

**框架自检目标**：以上 6 条对策均在框架默认行为中体现，且红队套件可验证。

---

## 2. 总体架构

```
┌────────────────────────────────────────────────────────────────┐
│ adapters/（每个爬虫任务一个插件）                                │
│   library │ social │ tis │ shuake │ generic-web │ …（未来任务）  │
│   接口：seeds / fetch_one / parse / verify / acceptance         │
├────────────────────────────────────────────────────────────────┤
│ core/ 引擎（长程/自愈/一致性/审计）                              │
│   engine 调度 │ limiter 限速 │ retry 重试 │ circuit 熔断        │
│   checkpoint 断点 │ store 幂等存储 │ audit 审计 │ heartbeat 心跳│
│   memguard 内存守卫                                             │
├────────────────────────────────────────────────────────────────┤
│ channel/ 通道层（三环境选路）                                    │
│   声明(LAN/CN/INTL) → 出口选择 → 健康探测 → fallback ≤30s → 审计 │
│   出口：本机直连 │ mihomo 代理链 │ 阿里云直连 │（备）云隧道      │
├────────────────────────────────────────────────────────────────┤
│ fetch/ 执行体层（升级链四档）                                    │
│   L0 HTTP(TLS/UA/会话) → L1 反检测浏览器(Camoufox/CloakBrowser) │
│   → L2 CDP 复用真实浏览器 → L3 逆向桥(frx→签名插件)             │
│   escalate.py：拦截检测 → 档位升级决策 → 审计                    │
├────────────────────────────────────────────────────────────────┤
│ anticrawl/ 反爬对抗                                              │
│   detect 拦截特征 │ captcha 验证码 │ signers 签名插件（逆向产出）│
├────────────────────────────────────────────────────────────────┤
│ llm/ LLM 辅助（分类/抽取/判定；oc-bridge→agnes→nvidia 降级链）    │
├────────────────────────────────────────────────────────────────┤
│ deploy/ keeper.sh │ deploy.sh(双端同步) │ acceptance │ redteam  │
└────────────────────────────────────────────────────────────────┘
```

**双端部署**：同一 codebase，`deploy.sh` 同步至阿里云；任务按"资源归属"分配（见 §4.4 部署矩阵）。

---

## 3. 核心引擎（core/）

### 3.1 部件与来源（不重写，合并成熟件）

| 模块 | 来源 | 要点 |
|---|---|---|
| `engine.py` | sm_main 泛化 | run/loop/once 三模式；任务循环 + 休整 + report；状态机可恢复 |
| `limiter.py` | R3 RateLimiter（scrape.py:1090+）+ sm_fetch.RateLimiter 融合 | 自适应（按响应反馈）+ 静态双模式；**硬上限编译期常量**（DR 对策）；ms 级精度 |
| `retry.py` | R3 retry 族（1752-1990） | 错误分类（fatal/transient/blocked/unknown）→ 重试计划（指数退避+抖动）；fatal 与限速器解耦 |
| `circuit.py` | R3 circuit 修正 + sm_fetch BlockedError | 真状态机；事件带读数；只对 transient |
| `checkpoint.py` | R3 断点事务族（549-1070） | save_progress 原子写 / repair_records_tail / reconcile / rebuild_from_records |
| `store.py` | sm_store + records 语义融合 | 幂等 upsert（按指纹）+ 原子写 + seen 集合；records.jsonl 兼容 |
| `audit.py` | R3 audit.py（r3-audit-v2 契约 v2.1） | writer 分组 / canonical 事件 / 校验器防伪 7 条 |
| `heartbeat.py` | R3 heartbeat.py + sm_common 心跳 | runs/.heartbeat；长操作主动打点 |
| `memguard.py` | R3 memguard.py | 内存 90% 暂停语义 |

### 3.2 一致性纪律（继承 E1-E2）

- records 追加为**事务型**（失败重写、撕裂修复、坏行对账）；
- progress 与 records 相互校验（reconcile）；
- 交付生成幂等（从 records 全量重放）。

---

## 4. 通道层（channel/）——三环境通用性核心

### 4.1 环境定义

| 代号 | 含义 | 典型目标 | 出口要求 |
|---|---|---|---|
| `LAN` | 南科大内网 | TIS(172.18.23.218) / 图书馆内网入口(124.251.9.18) / infoadmin / CAS / BB | 校园网内或可达内网的出口 |
| `CN` | 国内公网 | QQ / 微信 / B站 / 搜狗 / 学校官网 | 国内 IP 出口 |
| `INTL` | 外网 | Telegram / Brave / Google / GitHub | 代理出口（mihomo） |
| `AUTO` | 自动 | 公开无要求目标 | 默认优先级 |

### 4.2 出口资源（现状盘点）

| 出口 | 位置 | 覆盖 | 状态 |
|---|---|---|---|
| 本机直连 | WSL（校园网） | LAN/CN 部分 | 现成 |
| mihomo 代理链 | 本机 7890-7895（机场/自建 EDGE/ISP 三组） | INTL + CN | 现成，有 health API |
| 阿里云直连 | YOUR_SERVER_IP | CN + LAN（实测达 TIS/图书馆内网入口） | 现成 |
| （备）云隧道 | 本机 ↔ 阿里云 | 组合 | 未来可选 |

### 4.3 通道管理器设计

- **声明式选路**：adapter 每个请求声明 `channel`；manager 按环境→出口映射表选择具体出口，支持同环境多出口优先级 + 轮换。
- **健康探测**：出入口定期探针（真实目标的特征 URL），带缓存与熔断标记。
- **fallback**：出口失败 → 依次降级（≤30s 切换完成，继承 D1）；切换事件写审计（含发现/切换/业务恢复三段读数，DR-B1 教训）。
- **双端协同**：`INTL` 任务默认本机执行；`CN/LAN` 任务可本机或云执行；adapter 可声明 `prefer_host`。

### 4.4 部署矩阵（任务 × 环境 → 执行端）

| 任务 | LAN | CN | INTL | 备注 |
|---|---|---|---|---|
| library | 本机/云（双入口） | 云 | — | 云已实证双入口 |
| social | — | 云（QQ/微信） | 本机（Brave/Telegram） | 现行分工，保持 |
| tis | 本机/云 | 云 | — | 双端均可 |
| shuake | — | 本机/云 | — | — |
| generic | 视目标 | 视目标 | 视目标 | 按声明 |

---

## 5. 执行体层（fetch/）与升级链

### 5.1 四档执行体

| 档 | 实现 | 用途 | 现状 |
|---|---|---|---|
| L0 | `http.py`：requests/httpx + UA 池 + 会话 + LegacyTLS | 绝大多数 API/页面 | 现成（R3+sm_fetch 抽取） |
| L1 | `browser.py`：Playwright + **Camoufox**（默认反检测）/ Chromium | JS 渲染 / 中等反爬 | 部件现成（本机 camoufox-venv；云侧需装） |
| L2 | `cdp.py`：CDP 复用真实浏览器（9222/Edge） | 高信任会话（登录态复用） | 现成（tis-bridge/web-scrape.js 模式） |
| L3 | `anticrawl/signers`：**逆向桥**（见 §6） | 签名/加密参数 | 新设计 |

### 5.2 升级链状态机（escalate.py）

```
请求(L0) → 响应分类
  ├─ 正常 → 记录（含性能读数）
  ├─ 限流(429/挑战) → 限速退避 / 熔断冷却 / 出口切换（channel）
  ├─ 封禁(403/WAF) → 出口切换 → 仍封 → 执行体升级 L1（指纹级对抗）
  ├─ 空壳/SPA → 执行体升级 L1/L2（渲染）
  ├─ 签名拒绝(缺失/错误 sign 参数) → 标记"需逆向" → 生成逆向任务单（L3）
  └─ 未知 → 审计 + 保守重试 + 上报
```

每级升级/失败/恢复均写审计；升级链行为可用红队验证（模拟拦截场景）。

---

## 6. 模式二逆向工具接入（anticrawl/ 与逆向桥）

> 用户点名：**接入标准模式二的各种逆向工具以突破反爬**。

### 6.1 接入矩阵

| 模式二资产 | 域 | 爬虫用途 | 接入方式 | 前提 |
|---|---|---|---|---|
| **frx-director（44 工具）** | 浏览器域 | 签名/加密参数还原、JSVMP 反混淆、闭包密钥读取、请求取证 | **逆向桥工作流**（§6.2） | 用户启动 Firefox Reverse 浏览器（一次性） |
| **Camoufox** | 反检测执行体 | 指纹随机化（navigator/WebGL/canvas…） | `fetch/browser.py` L1 默认引擎 | 已具备（本机 venv；云侧待装） |
| **CloakBrowser** | 反检测执行体 | Chromium 侧高防护（WebDriver 检测/指纹比对） | L1 备选引擎（CDP 目标） | 已具备（F 盘） |
| codegraph / serena | 静态分析 | 抓回 JS 的符号级分析（区域划分/交叉引用） | agent 工作流 MCP | 已通 |
| wx-worker（Frida） | Windows 域 | （非爬虫核心；桌面端数据采集的未来扩展） | 待建 | — |
| WMPFDebugger | 小程序调试 | （未来：小程序类采集） | 备选 | 已具备（F 盘） |
| 散件：aliyun_waf_bypass.py / tis-captcha | 反爬对抗 | WAF 挑战 / 验证码处置 | 并入 `anticrawl/` 工具箱 | 现成 |

### 6.2 逆向桥工作流（一次逆向 → 长期复用的标准管道）

```
[运行态] fetch 检测到"签名壁垒"（如缺失 sign= 请求被拒）
   ↓ 自动
① 生成《逆向任务单》（结构化：目标域名、请求样本、拒因读证、期望参数）
   ↓ agent（人参与，标准模式二流程）
② frx-director 会话（浏览器域）：page_eval 侦查 → net_capture 定位 → signer_trace/closure_read
   → jsvmp_trace/js vmp_disassemble（如需）→ 提取参数蓝图
   ↓ 产出物
③ 《param-blueprint.md》（算法/密钥/调用链/边界值）
   ↓ 实现
④ 签名插件（Node/Python 纯本地复现，不依赖浏览器）→ 放入 anticrawl/signers/<target>/
   ↓ 回注
⑤ adapter 声明该目标使用 signer 插件 → L0 HTTP 层自动附加签名
   ↓ 验证
⑥ 回归验证（重放真实请求 + 对照读数）→ 记账（run 血缘）
```

**边界与纪律**：
- 逆向是**一次性工程**（agent 深度参与），产出是**纯代码插件**（运行时零 agent 开销）；
- 逆向过程产物（蓝图/证据）归档可审计（防下次重踩，继承 ACS/qwen 战例纪律）；
- 浏览器域工具的"零注入优先"原则继承（引擎层旁观 > page_eval > hook_inject）。

### 6.3 待用户操作项（阻塞点）

1. **启动 Firefox Reverse**（marionette 3928，profile 路径见模式二文档）→ frx-director 才能直用；
2. （一次性）浏览器 Agent 设置里配置 worker key。

---

## 7. 适配器规范（adapters/）

### 7.1 接口（草案）

```python
class Adapter(Protocol):
    name: str                  # 唯一名（library/social/tis/...）
    channel: str               # 默认通道声明 lan/cn/intl/auto
    prefer_host: str           # local/cloud/any

    def seeds(self) -> SeedSet: ...           # 任务定义（目标/范围/查询矩阵/种子URL）
    def fetch_one(self, ctx, item) -> Raw: ... # 经由框架 fetch 栈（自动限速/重试/升级）
    def parse(self, raw) -> list[Record]: ... # 标准化记录（统一 schema）
    def verify(self) -> VerifyResult: ...     # 验收判据（机器判定）

    # 目录内附带：acceptance.sh / redteam_*.py / 目标卡.md
```

### 7.2 统一记录 schema（草案）

```json
{"key": "fingerprint", "kind": "book|lead|course|generic", "value": {...},
 "evidence": {"src": "url", "ts": "...", "method": "http|browser"}, "run_id": "..."}
```

### 7.3 存量爬虫 → adapter 迁移映射

| 现有资产 | 迁移动作 | 优先级 |
|---|---|---|
| sm-recon | 骨架直接泛化（src→adapters/social + core 抽取）；保持云上运行，切换期零停机 | P2 |
| 图书馆 R3 v2.1 | scrape.py 拆解：遗留 10 项必修择要继承（验收防伪/熔断语义/通道）；records 事务并入 core | P2 |
| TIS | 抽认证/重登部件入 core（会话层）；采集入口做成 adapter | P4 |
| shuake | 低优先级样板（表单会话型） | P4 |
| sustech-cli | 不迁移（公众封装）；library-search 逻辑参考 | — |
| 探针脚本群 | 不迁移；按需抽部件入 anticrawl/ 工具箱 | — |

---

## 8. 部署与运维

- **目录**：`~/go/omni-crawler/`（建议），结构 = §2 布局；`deploy/` 内含 keeper.sh / deploy.sh / sync-persist.sh / acceptance.sh / redteam（继承 sm-recon 全套并升级）。
- **双端**：本机 WSL（开发 + INTL/校园网任务）+ 阿里云（CN/LAN 长跑）；`deploy.sh` 排除 `*.pid/runs/logs/venv/recon`（sm-recon 教训）。
- **看护**：keeper 按任务实例配置（MAIN_CMD / STALE_LIMIT 纪律：阈值 > 最长休整）。
- **与既有守护共存**：tri-agent-boot 链、sm-recon 现行 keeper——迁移期并行（sm-recon 切走后由其 keeper 管理同一实例，不双拉）。
- **验收套件**：框架自检（`acceptance/framework.sh`：kv 断点/重试/熔断/通道fallback 四组红队）+ 每 adapter acceptance。

---

## 9. 实施路线（建议）

| 阶段 | 内容 | 产出 |
|---|---|---|
| **P0** | 拍板（本文档 §10） | 决策记录 |
| **P1** | 核心抽取：core/（来自 R3+sm-recon）+ channel/ + fetch/（L0/L1）+ 框架自检红队 | 可运行框架 + 自检全绿 |
| **P2** | 样板迁移：social adapter（sm-recon 迁移）+ library adapter（R3 v2.1 迁移，继承必修择要） | 两个真实任务跑在框架上 |
| **P3** | 逆向接入：anticrawl 升级链 + frx 逆向桥标准化（含一处实战：如遇真实签名壁垒则实做，否则以演练目标验证管道） | 升级链红队 + 逆向桥 runbook |
| **P4** | 其余迁移：tis / generic（/ shuake） | 全量收敛 |
| **P5** | 收口：三环境端到端验收 + 文档 + 记忆归档 + 存量现场退役计划 | 交付包 |

**建设方式建议**：P1-P3 采用「直接构建 + 每阶段红队/独立验收」；如用户希望按模式三五轮制（批评者池/多轮修订），可在 P1 后插入评审轮——**待拍板**。

---

## 10. 待拍板项

1. **基座选择**（§3 建议：sm-recon 骨架 + 图书馆深设施融合，新建 repo，不推倒重写）
2. **存量迁移策略**（全量迁移 / 增量 / 样板优先）
3. **本轮实施深度**（P1+P2 / 最小 P1 / 全量 P1-P4）
4. **命名与位置**（默认 `~/go/omni-crawler`；中文显示名"全能爬虫"）

---

## 附：与安全红线的关系

- 本项目涉及授权范围内的学术/校园数据采集（图书馆公开目录、选课统计、公开社媒信息），沿用既有任务的合规边界；对 TIS 等敏感数据沿用已披露路径与低频纪律。
- 逆向工具使用限定于"自有账号访问自有可见数据"的签名复现，产出物不含他人凭据；敏感内容三通道规则（REASONIX.md）在涉及凭据时不进会话。
