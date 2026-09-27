# r3-candidate 交付物 v2.0 → v2.1 变更留档

> 出处：R3p4 · P4.5 · slot-candidate-fix（候选版修复槽），2026-09-16。
> 依据：`R3p4/refs/audit-contract-v2.md` §8（v2.1 增量，主指挥裁决）+
> `R3p4/verification/report.md` §6-洞 C / §7 遗留表（R-1/R-4/R-8/R-9）。
> 完整读数：`R3p4/p4p5/slot-candidate-fix/test-report.md`。

## 0. 一句话

v2.1 = v2.0 **加**行级写者分组键 `writer`（KI-1/R-1 的契约 §8 落地）**加** R-4/R-8/R-9 三项自洽性修复；
**不含**任何行为语义改写（抓取、重试、收敛判据、限流、keeper 逻辑逐字节不变，见 §3）。

## 1. 变更文件清单（7 个文件 + 本文档 + 3 个 lineage/读数件）

| 文件 | 变更 | 对应项 |
|---|---|---|
| `audit.py` | 新增 `WRITER="retry"` / `ENV_WRITER` / `writer()`；`init(writer=…)`；每行写 `writer`；selftest +8 断言（含同 pid 双写者分组护栏） | KI-1（契约 §8.1） |
| `scrape.py` | 新增 `R3_AUDIT_WRITER="converge"`；`audit_event()` 每行写 `writer`；**删除** 弃用导出 `RETRY_JITTER_LO/HI`，改为 `RETRY_JITTER_BAND=(0.5,1.0)` / `RETRY_JITTER_MEAN=0.75` 并注明 `E[wait]=0.75·d`；文档同步 | KI-1（§8.1）+ R-8 |
| `keeper-v3.sh` | `audit()` 行布局新增 `"writer":"keeper"`（字面量，printf 参数表不变） | KI-1（§8.1） |
| `audit-verify-v2.py` | **分组键** `(文件,pid)` → `(文件,pid,writer)`；`writer` 为强制字段（严格模式硬违规 `writer_missing`；`--compat-v1` 回退 `(pid,"")` 且 `writer_missing`/组级 seq 偏差降级为 warning + 降级原因 `compat_v1_seq_grouping_by_pid`）；事件扩展表补入 keeper 写入名 11 个（R-9）；输出新增 `verifier_version`/`contract_rev`/`schema.writer_counts`/`writer_groups`/`writerless_groups`；`--self-test` 23 → 33 断言 | KI-1（§8.2/§8.4）+ R-9 |
| `test_converge.py` | 新增 `--target-mode auto\|single\|merge`：合流靶上把 3 条单槽不变量换成合流作用域等价判据（分支重建 + 区域并集闭合 + fetch/get_total 归因 + 全链重放自洽）；单槽靶判据逐字不变 | R-4（KI-3 参数化） |
| `scope-check.py` | 新增 `--ref PATH`（换靶重跑同一算法；判据/ALLOWED/FORBIDDEN/退出码不变），输出增 `ref_override`/`mode` | R-4 支撑 |
| `test_retry.py` | T9 新增契约 §8 护栏断言（每行带 `writer` 且 = `"retry"`）；180 → 181 断言 | KI-1 回归护栏 |
| `acceptance/R3-v2.sh` | K2 探针**只加口径标注**（`jitter_legacy_multiplicative`/`jitter_band_equal`/`jitter_mean_over_d`），判据数值与退出码不变 | R-8（读者口径可见化） |
| `KNOWN-ISSUES.md` | 追加「v2.1 状态表」（KI-1..KI-6 逐条现状 + 遗留清单） | 交付物自洽性 |
| `lineage/scrape-v2.0-to-v2.1.diff` | `scrape.py` 的 v2.0→v2.1 单文件差异（合流链末环，供 `test_converge.py --target-mode merge` 重放） | R-4 |

## 2. 指纹变迁（sha256 前 16 位）

见同目录 `SHA256SUMS.txt`（v2.1 全量清单，38 → 42 条）。核心成员：

| 文件 | v2.0 | v2.1 |
|---|---|---|
| `scrape.py` | `22c6138dbcffd9dc…`（md5 `1eae2209`） | 见 `SHA256SUMS.txt` |
| `audit.py` | `a783fea881f7dce4…` | 见 `SHA256SUMS.txt` |
| `keeper-v3.sh` | `de676deb4d75ac6e…` | 见 `SHA256SUMS.txt` |
| `audit-verify-v2.py` | `fc511c61cc03cef7…` | 见 `SHA256SUMS.txt` |

> v2.0 的 `scrape.py` 指纹由**合流链重放独立复算**（`retry → retry-fatal → converge → converge-f1 →
> audit-align-converge`，全部 `--fuzz=0`）⇒ 链终点 = `1eae2209…` = v2.0 交付件，
> 再加 `lineage/scrape-v2.0-to-v2.1.diff` 得 v2.1 交付件（逐字节，见 `test-report.md` §3.3）。

## 3. 明确**未**变更（防止"顺手改"）

- `scrape.py` 的抓取/重试/收敛/限流/停滞判据逻辑：v2.1 差异仅 2 处（`audit_event` 加键、
  jitter 常量改名 + 注释）；`fetch`/`get_total` 函数体与 v2.0 逐字节相同。
- `keeper-v3.sh` 的看护逻辑与事件名：仅 audit 行多一个键。
- 验收判据与阈值：`acceptance/R3-v2.sh` 的 K1..K11 判定逻辑、阈值、退出码分级均未改动。
- `test_converge.py` **单槽模式**（`--target-mode single`）逐条判据与 v2.0 相同。
- 契约本身（`R3p4/refs/audit-contract-v2.md`）由契约持有者冻结，本槽只实现 §8，不修订。

## 4. 回滚

`candidate-v2.1.patch` 可正向施加（`patch -p1 --fuzz=0`）与反向撤销（`patch -R -p1 --fuzz=0`）；
反向撤销后候选目录逐字节回到 v2.0（合流链重放给出同样的结论）。
