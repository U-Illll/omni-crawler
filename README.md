# omni-crawler

**全能爬虫**（2026-09-27 起建）——南科大内网 / 国内公网 / 外网三环境通用的统一采集框架。

## 定位

- 一个核心引擎（限速/重试/熔断/断点/存储/审计/看护）+ 通道层（LAN/CN/INTL 选路）+ 执行体升级链（HTTP → 反检测浏览器 → CDP → 逆向桥）+ 适配器插件（每个爬虫任务一个 adapter）。
- 以后爬虫类任务（数据采集）统一在此框架内实现。
- 建设标准：《爬虫5轮优化工程交接文档-20260918》硬指标体系（长程自动化/速度/自愈/降级兜底/数据一致性）+ DR 轮终审教训对策（见 docs/architecture.md §1.3）。
- 逆向对抗：接入标准模式二工具链（frx-director 44 工具 / Camoufox / CloakBrowser / codegraph+serena）。

## 来源（融合两个成熟样板）

- **sm-recon**（社媒爬虫，模块分层 + 双端部署 + 红队套件）→ `omni/` 骨架
- **图书馆 R3 v2.1**（断点事务 / 审计契约 / keeper 教训 / 演练台思想）→ `core/` 深设施

## 结构

```
omni/core/      引擎：limiter retry circuit store checkpoint audit log engine
omni/channel/   通道层：LAN/CN/INTL 声明式选路 + 健康探测 + fallback
omni/fetch/     执行体：L0 HTTP / L0+ curl_cffi协议指纹 / L1 browser(Chromium/Camoufox) / 升级链
omni/anticrawl/ 反爬对抗：拦截检测 / 签名插件（逆向产出落地处）
omni/llm/       LLM 辅助（oc-bridge → agnes → nvidia 降级链）
omni/adapters/  适配器插件：library / social / freetier / ...
deploy/         keeper.sh / deploy.sh / acceptance
tests/          框架自检红队
docs/           目标卡 / 架构 / runbook
```

## 快速开始

```bash
# 框架自检（红队）
bash tests/run_acceptance.sh

# 运行某个 adapter（示例）
python3 -m omni.adapters.social.main run --rounds 1,2

# freetier：官方免费机制基线+变化监控（谷歌/OpenAI/GitHub/IDE 平台等 28 站）
python3 -m omni.adapters.freetier.main baseline   # 建基线（幂等）
python3 -m omni.adapters.freetier.main refresh    # 复核变化
```
