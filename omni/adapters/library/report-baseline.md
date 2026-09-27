# 演练基线报告（故障注入 · 修复前对照读数）

> 生成：2026-09-27T13:01:07.036 ｜ 演练台：`R3/impl/slot-drill/drill.py` ｜ 校验器：`audit-verify.py` ｜ run_id `drill-20260927T130041-32197`

被测系统：**R3 基线：R2 收口合成候选版（scrape-r2cand.py + memguard + heartbeat + keeper-v2.sh）**

| 成员 | 路径 | sha256(12) |
|---|---|---|
| scrape | `/tmp/scrape-baseline.py` | `f9559cb7c626` |
| memguard | `/tmp/lib-opt-work/R3/refs/memguard.py` | `84f75f167c16` |
| heartbeat | `/tmp/lib-opt-work/R3/refs/heartbeat.py` | `5e9a993ef86e` |
| keeper | `/home/user/go/omni-crawler/omni/adapters/library/keeper-v3.sh` | `2583db3c79a5` |

## 0. 判定口径
- **恢复时限**：注入 → 新进程被拉起且**重新出现工作证据**（scrape.log 工作行增加 / records 增长 / 完成标记）；判据 ≤ 180s（A2）。
- **unique 零丢失**：场景末态 records.jsonl 的 unique mms ⊇ 干净参照跑集合（旁路比对）。
- **重复抓取 ≤1 叶子块**：mock 侧统计 `(prefix,sort)` 整页请求（limit≥500）出现 >1 次的对数；干净跑应为 0。
- **审计链路**：`audit-verify.py` 按 `refs/r3-audit-schema.md` 校验字段/枚举/seq 单调 + 场景→事件链路断言。

## 2. 场景总表（修复前）

| 场景 | 注入 | 恢复(s) | 时限 | unique 丢失 | 重复叶子块 | 完成 | 审计链路 | schema 严格 | 判定 |
|---|---|---|---|---|---|---|---|---|---|
| S1 | kill -9 | 5.76 | 180 | 0 | 0 | ✓ | ✓ | ✓ | **FAIL** |

## 3. 逐场景明细

### S1 · kill -9 循环 ×3 → 恢复时限 + 数据完整性（C3/A2）

- 判定：**FAIL**（wall 25.18s，错误 无；系统 `baseline-r2cand`，run `drill-20260927T130041-32197`）
- 完整性：unique 1160/0（丢失 0，多余 1160）；损坏行 0；重复 mms 0；重复叶子块 0；mock 请求 41（相对参照 ×None）
- 注入：kill -9 pid=32986（pgid 32986）→ 重启 1.75s / 恢复 5.76s（新 pid 33884）；恢复靠干活=False，靠完成标记=False
- 注入：kill -9 pid=33884（pgid 33884）→ 重启 7.26s / 恢复 Nones（新 pid 35122）；恢复靠干活=False，靠完成标记=True
- 注入跳过：no_live_job（完成标记已在：keeper 守卫语义下不会再有重启 ⇒ 后续 kill 无观测对象）
- 场景读数：`{"probe_retry_lines": 0, "zero_child_lines": 0, "branch_lines": 0, "throttle_adjust_lines": 0, "http_retry_lines": 0, "exc_lines": 0, "progress_fallback_lines": 0, "records_repair_lines": 0, "records_rebuild_lines": 0, "final_repair_rounds": 0, "conc_item_errors": 0, "torn_requeue_entries": 0}`
- 审计：文件 ['/home/user/go/omni-crawler/omni/adapters/library/runs/drill-20260927T130041-32197/S1/audit.jsonl', '/home/user/go/omni-crawler/omni/adapters/library/runs/drill-20260927T130041-32197/S1/out/audit.jsonl', '/home/user/go/omni-crawler/omni/adapters/library/runs/drill-20260927T130041-32197/S1/out/memguard.jsonl']
- 审计判定：{"pass": true, "pass_count": 1, "check_count": 1, "fail_reasons": [], "missing_events": [], "derived_only": false}
- 断言：
    - [FAIL] recovery_within_deadline：value=[5.76, None] limit=180 — 每一次**实际做过的** kill -9 后「重新干活」耗时（s）；做过 2 次、成功恢复 1 次
    - [FAIL] kill_loop_3rounds：value=2 limit=3 — 要求的 3 轮 kill 是否都打到了活着的进程；不足 3 轮说明系统在中途已无作业可杀（往往是假收敛）—— 硬判据（不达下限即 FAIL）
    - [FAIL] recovery_no_false_convergence_suspect：value=1 limit=0 (soft) — kill 后未观测到重新干活、却已出现完成标记的注入次数（假收敛嫌疑；软读数：硬面由 kill_loop_3rounds + no_unique_loss 承担）
    - [PASS] no_unique_loss：value=0 limit=0 — 相对参照跑缺失的 unique mms 数（examples=[]）
    - [PASS] dup_leaves_within_tolerance：value=0 limit=1 — 重复抓取的叶子块（mock 侧 (prefix,sort) 整页请求 >1 次的对数）
    - [PASS] records_wellformed：value=0 limit=0 — records.jsonl 末态损坏行数
    - [PASS] run_completed：value=None limit=None — scrape.log 出现完成标记（S6/S6b 单列为软读数：跑完≠收敛对，假收敛由 no_unique_loss 判）
    - [PASS] audit_link：value=1 limit=1 — audit-verify-v2：canonical 通道硬通过；channel=audit；window_enforced=True；
    - [PASS] audit_channel_audit_only：value=audit limit=audit (soft) — 链路断言是否全部由结构化审计通道满足；derived-only 会被标注channel=derived / evidence_level=low 并降级
    - [PASS] audit_schema_strict：value=23 limit=0 (soft) — R3 契约 v2 严格布局校验（schema=r3-audit-v2 / ts_epoch / pid / seq 从 1 连续 / level / event / detail）

## 4. 机读产物
- `results/summary.json` + `summary.json`（全场景结构化读数；基线用 `summary.json`）
- 每场景目录：`runs/<run_id>/<SID>/`（sandbox.json / evidence.json / verdict.json / audit.jsonl / out/ / logs/）
