# r3-candidate 已知问题清单（R3p4 merge 槽随产物交付 · **v2.1 状态表在文末**）

> 本清单只登记**合成候选版**在集成回归中实测到的问题，含根因归属与是否由合成引入。
> 完整证据见 `R3p4/merge/merge-report.md` §6/§7 与 `R3p4/merge/logs/`。
>
> **v2.1 增量（R3p4 · P4.5 · slot-candidate-fix，2026-09-16）**：KI-1 已按契约 §8 修复
> （`writer` 分组键）；R-4/R-8/R-9 已处置。逐条现状与证据见文末「v2.1 状态表」，
> 变更清单见 `lineage/v20-to-v21-changes.md`，差异见 `lineage/scrape-v2.0-to-v2.1.diff`
> 与 `candidate-v2.1.patch`。

## KI-1（HIGH · 阻塞 S4 审计链路）scrape 侧「两写者共用同一 pid 的 seq 计数器」冲突

- **现象**：`drill.py --system r3-candidate --only S1,S4` 中 S4 判定 FAIL，唯一硬失败为
  `audit_link`；`audit_schema_strict` 报 **75 条**严格违规，其中 3 条为
  `file=audit.jsonl pid=8631 seq=1/2/3 重复`（即 audit-contract-v2 §3 的 pid 组内 seq 唯一性被破坏）。
- **根因**：合成后的 `scrape.py` 里**同时存在两个审计写者**，且二者都写同一个
  `OUT/audit.jsonl`、都用 `os.getpid()` 作为 `pid`，却各持**独立**的 seq 计数器：
  1. converge 侧 `audit_event()` 的 `_AUDIT_SEQ`（实测发出 `run_start` seq=1，随后
     `convergence_decision` seq=2、`run_end` seq=3）；
  2. retry 侧 `_retry_audit()` 懒加载的 `audit.py`，其内部自有 seq（实测发出
     `error_classified`/`retry_scheduled` 共 62 条，seq 1..62）。
  两个计数器在同一 pid 组内交错 ⇒ (pid, seq) 重复。
- **是否由合成引入**：**不是"合成写错"，但确由合成首次暴露**（emergent）。
  实测：`audit_event` 仅存在于 converge 系产物（slot-converge / slot-converge-f1）；
  `_retry_audit`+`_AUDIT_MOD` 仅存在于 retry 系产物（slot-retry / slot-retry-fatal）；
  **只有合成版 r3-candidate 两者兼备**。故任何单槽系统的演练都不可能触发。
  audit-unify 槽自己的 S1/S4 真实产物 `out/audit.jsonl` 实测 dup(pid,seq)=**none**。
  另外 R3 基线的 S4 `audit_link` 之所以 PASS，是因为当时用的是 v1 校验器且判在
  **derived 通道**（`derived_only: true`）；P4 的 v2 校验器改为 canonical 硬通过后才显形。
- **归属**：scrape 侧（converge × retry 接口），**与 keeper 侧合流无关**——
  同一场景 keeper 的 sink（pid 7924）实测 `seq 1..10 ok=true`，keeper 审计链干净。
- **本槽处置**：**不修**。依据 `R3p4/refs/audit-contract-v2.md` 开头：
  「本契约是 R3-P4 全部槽的强制接口（写侧与读侧唯一权威）。若有槽发现契约不可行，
  须在回执中提出，**不得私自偏离**。」两个写者共用 seq 命名空间属**契约 §3 未覆盖**
  的情形（§3 按 pid 分组，隐含假设"一个 pid = 一个写者"），修订契约超出合成槽权限。
- **给契约持有者 / R4 的建议（三选一，按侵入性排序）**：
  1. **契约修订（最小侵入，推荐）**：§3 增加"同一 pid 组内的 `seq` 由**单一写者计数器**产生"
     的显式前提，并新增分组键（如规范化 `writer` 字段，"scrape/block" vs "scrape/request"），
     校验器改按 `(pid, writer)` 分组。仅需改校验器 + 两个写者各加一个字段。
  2. **合并计数器**：让 `scrape.py` 把 `_AUDIT_SEQ` 注入 `audit.py`（例如 `audit.py` 的
     `emit` 接受外部 seq），两写者共享一个进程内序列。改动落在两个模块的接口上。
  3. **分离 sink**：retry 侧改写到 `OUT/audit-request.jsonl`，与块级 `OUT/audit.jsonl` 分文件。
     最省事但会改变产物布局契约（§4 sink 与文件名）。
  > 无论选哪种，都建议同步补一条**多写者同 pid** 的 drill/校验器夹具，防止回归。

## KI-2（MEDIUM · 遗留，非本槽引入）S1 `no_unique_loss` = 2396（R2 并发改造回归）

- 合成版 S1：`loss=2396`，与 **R3 基线 `baseline-r2cand` 的 S1 读数逐值相同**
  （`R3/impl/slot-drill/report-baseline.md`：S1 FAIL，unique 1160/3556，丢 2396）。
- 该缺口在 R3 开工时即被登记为「R3 靶子」（报告 §38-44：`W_MAX=3` 的在飞条目在崩溃窗口丢失，
  串行基线最多丢 1 个，并发版最多丢 3 个），R3 本轮的实现槽并未收口。
- 连带 S1 的 `kill_loop_3rounds`(2/3) 与 `recovery_no_false_convergence_suspect`(soft)：
  第 2 次 kill 后完成标记已落盘 ⇒ 演练台 `no_live_job` 跳过第 3 轮，属演练台已文档化的语义。
- **建议**：R4/R5 以「入队出队与 progress 同事务化 + 重启按 records 反查重走」收口 A2。

## KI-3（LOW · 设计使然）`test_converge.py` 3 条断言在"合流靶"上必然失败

在合成版上跑 `test_converge.py` 得 **76 PASS / 3 FAIL / 12 套件**；3 条失败均系
**单槽不变量**，靶换成任何合流版都会失败，**不代表合成缺陷**：

| 失败断言 | 原因 |
|---|---|
| `scope-discipline`（`violations=417 regions=68`） | `scope-check.py` 判"改动是否全在 **converge 槽登记区域**内（相对 r2cand 基线）"；合成版合法地含 retry 槽 355+ 行改动 ⇒ 越界 |
| `fetch / get_total 函数体逐字节未变`（`{"get_total": true, "fetch": false}`） | converge 槽承诺不碰 `fetch()`；而 **retry 槽的全部重试逻辑就写在 `fetch()` 里** ⇒ 必然改动（`get_total` 未变，符合 converge 槽承诺） |
| `patch-roundtrip`（`sha=f471998c/1eae2209`） | 断言"`converge-f1.patch` 施加回 converge 产物 == 本目录 `scrape.py`"；合流的 `scrape.py` 还含 retry 段 ⇒ 自指血缘不变量失效 |

- **对照实验（已做）**：把同一份 `test_converge.py` 放回**模块忠实布局**（= converge-f1 槽自己的
  `scrape.py`）复跑 ⇒ **79 PASS / 0 FAIL / 12 套件全绿、exit 0**（`logs/regress-test_converge-control.out`）。
  故套件本身完好，3 条失败可完全归因于"多了一个 retry 模块"。
- **合流靶的等价判据由合成链自带的更强证明承担**（见 merge-report §4）：
  EQ1 交换律（反序施加得**逐字节相同**的 scrape.py）、EQ2 反演往返（合流 → -R×5 → 基线逐字节还原）、
  EQ3 AST 等价。链条级往返比单槽自指往返更强。

## KI-4（已处置）`test_retry.py` T9 断言与契约 v2 漂移

- audit-align-retry 按契约 §1 把写侧 `ts` → `ts_epoch`；而 retry 槽的套件早于该重命名写就，
  T9 断言遗留键 `ts` ⇒ 原始状态 **rc=1 + `KeyError: 'ts'`（连带 T10/T11/T12 未执行）**。
- 本槽已把 3 行断言对齐契约 v2（并把 §1 的"不得写 `ts`"变成负判据回归护栏）：
  `tools/adapt-test-retry-contract-v2.py`，**只改候选目录内的副本**，原件只读；
  差异留档 `lineage/test-retry-contract-v2.diff`，原件留档 `lineage/test_retry-original.py`。
- 适配后：**180 PASS / 0 FAIL / exit 0**（对照槽内自测 179 PASS，+1 即新增的负判据）。

## KI-5（已处置）keeper 侧唯一语义冲突

- `keeper-match.patch`（40 hunks）与 `audit-align-keeper.patch`（2 hunks）同时改写 `audit()` 的
  printf 行；**正序与逆序以 `--fuzz=0` 施加都失败**（非位置冲突，属语义冲突）。
- 裁决：行布局取 `audit-align-keeper`（契约 v2 §1 权威写侧；§1 明令 v2 行不再写 `ts` 键），
  `keeper-match` 的实质增补（扩展 `R3_ALIAS_EVENTS` 14 项、v3.1 全部逻辑）零丢失保留。
  依据 keeper-match 槽自陈「契约 v2 顶层 `schema` 迁移**留给 audit-unify / merge 槽**」。
- 产物：`audit-align-keeper-rebased.patch`（派生补丁，可 `--fuzz=0` 重放且逐字节等价）。
- 复核：合成 keeper 的 audit sink 在 S4 实测 `pid=7924 seq 1..10 ok=true`，契约 v2 合规。

## KI-6（观察项 · 非本槽引入）S4 `重复 mms 3525`

- 合成版 S4：`重复 mms 3525`、`重复叶子块 0`（治理断言 `dup_leaves_within_tolerance` **PASS**）。
- R3 基线 S4 读数为**完全相同的 3525** ⇒ 既有现象，非合成引入，本轮不追根因。

---

# v2.1 状态表（R3p4 · P4.5 · slot-candidate-fix · 2026-09-16）

> 依据：`R3p4/refs/audit-contract-v2.md` **§8（v2.1 增量）** + `R3p4/verification/report.md` §6-洞 C / §7 遗留表。
> 全部读数：`R3p4/p4p5/slot-candidate-fix/test-report.md` 与同目录 `readings/`、`logs/`。

| 项 | 级别 | v2.1 处置 | 关键读数 |
|---|---|---|---|
| **KI-1** 两写者共用 pid 的 seq 计数器 | HIGH | **已修**（契约 §8：行加 `writer` 分组键；校验器按 `(文件,pid,writer)` 分组；缺 writer 严格模式硬违规 `writer_missing`，compat 回退 `(pid,"")`） | 修复版真实 S4：`verdict=PASS audit=PASS`，校验器 `exit 0 / canonical_pass=True`，`writer 分组 3 个 / 无 writer 分组 0 个`（`retry` 62 × `converge` 3 同 pid 5196 各自 1..N 连续、`keeper` 10） |
| **R-4** `test_converge.py` 合流靶 3 红 | MED | **已修**（套件参数化 `--target-mode auto\|single\|merge`：合流靶上 3 条单槽不变量换为合流作用域等价判据，见 `test-report.md` §3） | 合流靶：**TOTAL PASS，失败断言 0**（原 76 PASS/3 FAIL） |
| **R-8** 继续导出【弃用】`RETRY_JITTER_LO/HI` | LOW | **已修**：删除旧导出，改为 `RETRY_JITTER_BAND=(0.5,1.0)` / `RETRY_JITTER_MEAN=0.75`，并注明 equal jitter 的 `E[wait]=0.75·d`（相对乘性基线收紧 25%，属 retry-fatal 已知取舍）；`acceptance/R3-v2.sh` 的 K2 探针只加**口径标注**，判据数值不变（K2 口径重新冻结属 R-2） | 旧导出 `[]`；4000 抽样 ×20 格实测 `E[wait]/d = 0.7497`、`wait/d ∈ [0.5000, 1.0000]`；K2 探针读数仍 `[0.75, 1.25]`（前后一致，不改变任何判据） |
| **R-9** 校验器把 keeper 的 `keeper_exit_observed` 记 `event_unknown` | LOW | **已修**（改读侧表 = 侵入小者）：扩展表补入 keeper 写入名 11 个；`--self-test` 增加 `keeper.event_table_aligned` 回归护栏（从 `keeper-v3.sh` 机器提取而非手抄） | 同一夹具：v2.0 校验器 `rc=1 event_unknown=1`；v2.1 校验器 `rc=0 canonical_pass=True event_unknown=0`；keeper 49 个写入名表外 **0** 个 |
| KI-2 S1 `no_unique_loss`=2396 | MED | **未处置**（R2 并发遗留，与本轮任务无关） | 见原条目；本槽未跑 S1（红线不 kill 进程） |
| KI-3 单槽不变量设计使然 | LOW | **已参数化**（同 R-4）：单槽靶仍走原判据 | 单槽对照（converge-f1 靶）：原判据全绿 |
| KI-4 `test_retry.py` T9 漂移 | — | 已处置（merge）；v2.1 追加 §8 写者键护栏 | `181 PASS / 0 FAIL`（180 → 181，+1 即 §8 护栏） |
| KI-5 keeper 侧语义冲突 | — | 已处置（merge），v2.1 在其行布局上追加 `writer:"keeper"` | 真实 S4 keeper sink：`writer=keeper` 10 行、seq 1..10 |
| KI-6 S4 `重复 mms 3525` | 观察项 | 未处置（既有现象） | 修复版 S4 仍为 3525（与本轮无关） |

**遗留（交 R4/R5）**：R-2（K2/K7/K8 判据本身不可用）、R-3（C2 侧无独立旁路）、R-5（候选版不带 runs/）、
R-7（K9b 默认演练 r3-full）、R-10（72h 长跑 / 真实网络 / kill 注入）、R-12（ADV 计数口径反了）——
均见 `verification/report.md` §7，本槽未处置也**不建议在未处理 R-2 之前**把 K2 当作有效判据。
