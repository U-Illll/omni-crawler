# OMNI-PATCHES —— 迁移期对 R3 v2.1 基线的补丁记录

> 迁移工程：omni-crawler P2 library adapter（2026-09-27）
> 基线：`R3/archive/p4-merge/r3-candidate/scrape.py`（sha256 前缀 7f458e43，4184 行）

## 补丁清单

### P1: omni-inflight-snapshot（2026-09-27）

- **文件**：`omni-inflight-snapshot.patch`（74 行 diff，相对基线）
- **动机（迁移验证中发现）**：`_WorkQueue.inflight` 为纯内存集合——条目 `take()`
  后即从磁盘 `todo` 移除，在"条目被认领处理中"窗口内进程被杀（kill -9/断电/OOM），
  该条目在磁盘上**无痕**：不在 todo、不在 done → 重启后被判"无事可做"→ 若为
  最后一根/根条目则**假收敛 exit 0**（数据覆盖缺口）。违反 A2「恢复后零丢失」。
- **实证**：/tmp/omni-lib-test3（未打补丁）：kill 于根处理窗口 → P2 秒收敛 exit 0、
  X 根丢失；/tmp/omni-lib-test6（打补丁）：纯 inflight 窗口 SIGKILL（快照=['X']、
  todo=[]）→ 重启日志 `[omni] inflight 快照回队 1 项: ['X']` + audit
  `inflight_requeue` 事件（r3-audit-v2 格式）→ X 继续处理，不假收敛。
- **内容（5 处）**：
  1. `_WorkQueue.__init__`：新增 `self.inflight_items = {}`
  2. `_WorkQueue.take()`：认领后写 `prog['_inflight_snapshot']` 并立即 `save_progress`
  3. `_WorkQueue.finish()`：同步清除快照字段
  4. `_WorkQueue.save()`：保存前同步快照字段
  5. `main()` 启动段（`reconcile` 前）：快照回队（去重 todo/done）→ 强制 save + 审计
- **兼容**：新增字段 `_inflight_snapshot` 为下划线内部键；旧版读到可忽略；新版读旧
  progress（无该字段）行为不变。
- **回滚**：`patch -R` 本补丁或从基线重拷；无其他文件依赖。

## 对 R3 自带测试套件的影响（已知且预期）

| 套件 | 读数 | 说明 |
|---|---|---|
| test_retry.py | **181 PASS / 0 FAIL** | 完全不受影响 |
| test_converge.py | 2 FAIL（血缘类）| ①`scope-discipline §合流②`：外来改动=['_WorkQueue']（即本补丁）②`patch-roundtrip §合流④`：补丁链逐字节比对 identical=False（本补丁未纳入官方链）。**两failure均为血缘检查对本补丁的预期检出，非行为回归**；行为类断言（e2e-loopback×3、reconcile、converge 判据、scope①③④′④″）全 PASS |
| drill S1（kill-9×3 对照） | **补丁版：3/3 轮恢复、loss=0、audit=PASS；基线版：2/3 轮即死 + 假收敛嫌疑 1** | 完整读数对照见 `docs-drill-s1-verdicts-20260927.md`；补丁版唯一超差=dup_leaves 4>1（取舍：不丢失优先，登记为未来优化项）。drill 跑法：`--scrape`/`--keeper` 必须绝对路径 |

## 传染性评估（上游回填建议）

- 该缺口影响任何使用 R3 v2.1 conc 路径的运行（本机/云端生产现场）。
- 建议回填至 campaigns 的 R3 归档与云端部署件（`YOUR_SERVER_IP:/tmp/library-scrape/scrape.py`），
  或统一改用 omni-crawler 的 library adapter 版（推荐路径，天然携带本补丁）。
- 若上游采纳：把本补丁重命名为 `lineage/scrape-v2.2-inflight.patch` 纳入补丁链后，
  test_converge 的血缘断言即可恢复全绿。
