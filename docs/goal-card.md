# 目标卡 — omni-crawler（全能爬虫）

> 建立：2026-09-27 ｜ 拍板：用户四项全选推荐（基座=sm-recon 骨架+图书馆设施融合；范围=P1 核心+P2 双样板；方式=直接构建+红队验收；边界=只做数据采集）

## 1. 目标

1. **统一采集框架**：一个核心引擎 + 适配器插件；新爬虫任务 = 写 adapter。
2. **三环境通用**：南科大内网（LAN）/ 国内公网（CN）/ 外网（INTL）声明式选路；双端部署（本机 + 云）。
3. **反爬对抗体系化**：运行态自动（限速/熔断/出口/浏览器升级）+ 工程态逆向桥（frx-director 44 工具 → param-blueprint → 签名插件）。
4. **长程自动化**：keeper / 断点 / 自愈 / 审计；任务丢给它即可。

## 2. 标准（继承《爬虫5轮优化工程》硬指标 + DR 终审教训）

| 指标 | 落点 | 状态 |
|---|---|---|
| A1 72h 无人值守 | keeper + 心跳 + 审计 | ✅ 框架（keeper.sh + engine.loop） |
| A2 被杀 ≤180s 恢复 + 零丢失 | keeper 60s 周期 + 托管重启 | ✅ 实测（t3/t6；含 inflight 补丁修复的零丢失） |
| A3 断电一致性 | 原子写 + repair + reconcile + inflight 快照 | ✅ 红队 11 项 + omni-patch |
| B1-B3 稳态/限流红线 | 自适应限速器（硬上限 0.2s 编译期） | ✅ 红队 8 项 |
| C1-C3 重试/兜底/keeper | retry 族 + reconcile + keeper | ✅ 红队 20 项 |
| D1 多通道 ≤30s | channel 层（探针+fallback+审计） | ✅ 红队 7 项 |
| D2 错误四分类 | classify + fatal 解耦 | ✅（fatal 不喂限速器/熔断） |
| D3 熔断 | 真状态机（禁空转事件） | ✅ 红队 14 项（404×30 负对照） |
| E1-E2 一致性 | 幂等 store + 事务 records | ✅ 红队 12 项 + e2e |

DR 六条教训对策（architecture.md §1.3）全部落到代码 + 红队断言。

## 3. 本轮范围（P1+P2）交付物

- `omni/core/` 引擎 8 模块（config/log/limiter/retry/circuit/checkpoint/store/engine）
- `omni/channel/` 通道层；`omni/fetch/` L0+L1+升级链；`omni/anticrawl/`（detect/ua/signers 位）；`omni/llm/`（降级链）
- `omni/adapters/social/`（sm-recon 74 文件迁移 + 三层适配 + 协议封装）——真实网络验证 PASS
- `omni/adapters/library/`（R3 v2.1 托管封装 + inflight 崩溃窗口补丁）——托管/自愈/续跑/收敛全链验证
- 红队 11 套件 96+ 断言全绿；`deploy/`（keeper/acceptance/deploy）
- 文档：architecture.md / inventory.md（盘点报告）/ runbook.md / OMNI-PATCHES.md

## 4. 明确不做（本轮）

- TIS/shuake 迁移（P4 按需）
- 抢课/注册等写入自动化（用户拍板：只做数据采集）
- frx 逆向桥的实战单（等真实签名壁垒 + 用户启动浏览器）
- R3 遗留 10 项必修的逐项复刻（已在框架层以 DR 对策形式覆盖；若要求逐项对齐需另立轮次）

## 5. 验收（机器判据）

```bash
cd ~/go/omni-crawler
bash deploy/acceptance.sh    # → OMNI-ACCEPTANCE: PASS 即通过
```
- 附加实测证据（迁移期）：social 网络冒烟 SMOKE-PASS；library t1/t2/t3/t6（含纯 inflight 窗口修复实证）；test_retry 181/0；test_converge 行为类全 PASS（2 血缘断言对补丁为预期 FAIL）；drill S1 运行记录见 adapter results/。
