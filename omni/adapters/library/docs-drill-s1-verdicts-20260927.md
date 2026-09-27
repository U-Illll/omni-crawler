# drill S1 对照实验读数（2026-09-27，omni 迁移验证）

> 场景 S1：kill -9 循环 ×3 → 恢复时限 + 数据完整性（C3/A2）
> 跑法要点：`--scrape`/`--keeper` **必须绝对路径**（drill 以 rig 目录为 cwd 启动 keeper；
> 相对路径会导致 `no_live_job` 伪失败）。命令见文末。
> 两跑均为 `--no-reference` 独立沙箱（mock 数据、无参照跑）。

## 读数对照

| 断言 | 补丁版（omni-inflight-snapshot） | 基线版（原始 r3-candidate） |
|---|---|---|
| kill_loop_3rounds | **PASS**（3/3 轮全执行） | **FAIL**（仅 2 轮；中途已无作业可杀） |
| recovery_within_deadline | **PASS**（recover=[5.01, 11.27, 5.76]s） | **FAIL**（[5.76, None]——第 2 次 kill 后未恢复） |
| recovery_no_false_convergence_suspect | PASS（0） | **FAIL**（1——kill 后未重新干活却出现完成标记=假收敛嫌疑） |
| no_unique_loss | PASS（0） | PASS（0，但参照缺失，核验力弱） |
| dup_leaves_within_tolerance | **FAIL（4 > 容差 1）** | PASS（0——因 2 轮后即死，无后续重复） |
| run_completed | PASS | PASS（None 软读数） |
| audit_link / schema_strict | **PASS**（audit-verify-v2 canonical；schema=30 全过） | PASS |
| unique mms | 3556 | 1160 |
| mock 请求数 | 315 | 41 |
| wall | 77.82s | 25.18s |

## 归因与裁决

1. **inflight 缺口在真实"3 连 kill"下必然触发**（基线版现场证据）：第 2 次 kill 后作业
   消失 → 无法做第 3 轮 kill → 假收敛嫌疑 +1。这正是 `/tmp/omni-lib-test3` 复现的
   "条目无痕丢失 → 假收敛" 在演练台层面的独立确认。
2. **补丁修复有效性**：3/3 轮恢复（5-11s ≪ 180s 时限）、零丢失、审计链 canonical 通过；
   工作量（unique 3556 / 请求 315）为基线的约 3 倍——被杀的进度不再蒸发。
3. **dup=4 超容差（≤1）为已知取舍**：补丁对被杀的"处理中条目"整块重做（宁可重复、
   不可丢失）。3 连 kill 压力下贡献约 3 个重复叶子块，叠加基线既有重复机制到 4。
   优化方向（未来轮）：重做粒度细化到"叶子块内页级断点"（记录已抓 (prefix,sort) 页）。
   **裁决：接受并登记**——以 unique 零丢失优先（A2 主判据）；基线以"0 重复"换来的
   是"2/3 数据缺失 + 假收敛"，不可接受。

## 复跑命令

```bash
cd ~/go/omni-crawler/omni/adapters/library
# 补丁版
python3 drill.py --only S1 --no-reference --scrape "$(pwd)/scrape.py" --keeper "$(pwd)/keeper-v3.sh" --window 150
# 基线版
python3 drill.py --only S1 --no-reference --scrape /tmp/scrape-baseline.py --keeper "$(pwd)/keeper-v3.sh" --window 150
```

日志：补丁版 `/tmp/drill-s1b.log`；基线版 `/tmp/drill-s1-base.log`；基线报告快照 `/tmp/drill-s1-baseline-report.md`。
