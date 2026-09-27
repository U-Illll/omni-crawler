#!/usr/bin/env python3
"""SUSTech 图书馆全馆藏抓取器 v10 - 长程自动化加固版（基于 v9；队列 DAG / 抓取语义 / 日志格式均不变）

v10 相对 v9 的改动只发生在「持久化一致性」层（指标 A3）：
1. records.jsonl 行级原子追加：整行先 json 序列化 → 单次 O_APPEND os.write → fsync。
   （v9 用文本缓冲 open(...,'a').write()，kill -9 会整块丢弃缓冲区，并可能在缓冲区
     边界处把一行切成两半，留下半写 JSON 行）
2. progress.json 原子替换加固：唯一 tmp 名 + fsync(文件) + 双槽 os.replace
   （progress.json / progress.json.bak）+ 目录 fsync。任何时刻磁盘上不存在半写 progress。
3. 启动自愈 reconcile()（幂等，可反复执行）：
   a) 扫描 records.jsonl，在第一条损坏/半写行处截断修复，返回被丢弃行的 prefix；
   b) 用 records 反查重建 progress：已落盘但 progress 未更新的叶子 → 补登 done；
   c) done 账本中的叶子若 records 里缺失/不足 → 重新入队补抓（不丢 unique）；
   d) 被截断丢弃的 prefix → 重新入队补抓；
   e) records 已覆盖的 leaf 型缺口 → 从 gaps 消解。
4. 「不重复抓取」闸门 is_recorded_done()：叶子开抓前先查 records 反查计数，
   已完整的叶子直接跳过（覆盖"records 已写 / progress 未更新"窗口 + 分支重探子块）。
5. progress.json 新增 done 账本 / recovery 审计 / seeds / schema；load_progress 在
   progress.json 缺失、空文件或损坏时回退 progress.json.bak；两者都不可用时按
   records.jsonl 全量反查重建（rebuild_from_records）。
6. 故障注入测试缝（默认彻底关闭：需 SCRAPE_FAULT 与 SCRAPE_ALLOW_FAULTS=1 同时设置）。

v10-fix（R1 修订轮 P4 · ADV-1 两项）：
7. 撕裂末块不再被容差洗成「已完成」：repair_records_tail() 记录受损行的 prefix
   （先取行内幸存的 "prefix" 字段，退化取「上一条完整记录的 prefix」），写入
   progress.torn_requeue 污点账本 + 进程内 FORCE_REQUEUE ⇒ is_recorded_done() 对这些
   prefix 一律返回 False ⇒ 强制回队重抓；任何「容差完成」判定都写 recovery[] 审计
   （action=tolerance->done）。理由：撕裂丢 1 条时 cnt=total-1 永远落在 leaf_tol 内，
   原实现会补登 done、exit 0、无痕迹（ADV-1e：11 个尺寸里 9 个静默丢失）。
8. repair_records_tail() 改为**最小化丢弃**：只丢弃无法安全保留的损坏行片段，
   其后的完整记录逐字节保留（原实现在第一条坏行处 truncate，一次短写连带丢弃
   24 行完整记录/23 个 unique mms）；损坏行尾部的完整记录用 _salvage_tail() 抢救。
9. progress schema 校验：load_progress() 加载后调用 validate_progress()，
   结构异常（缺 todo/done/gaps/recovery/stats、条目缺 p、schema 版本不符）写告警日志。
"""
import ssl, json, time, random, string, threading, sys, os, signal, hashlib, re
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

BASE = Path("/tmp/library-scrape")
OUT = Path(os.environ.get("SCRAPE_OUT_DIR") or (BASE / "output"))   # v10: 测试可重定向输出目录（默认不变）
OUT.mkdir(parents=True, exist_ok=True)
PROGRESS_FILE = OUT / "progress.json"
PROGRESS_BAK = OUT / "progress.json.bak"          # v10: progress 上一版好状态（双槽）
RECORDS_FILE = OUT / "records.jsonl"
LOG_FILE = OUT / "scrape.log"

HOST = "https://sustc.primo.exlibrisgroup.com.cn"
VID = "86SUSTC_INST:86SUSTC"
INST = "86SUSTC_INST"
CHARSET = list(string.digits) + list(string.ascii_uppercase)
LEAF_MAX = 490
BULK_LIMIT = 500
PROBE_WORKERS = 4          # v9: 3→4
LEAF_WORKERS = 3           # v9 新增：叶子补抓并行度
MIN_INTERVAL = 0.20        # v9: 0.28→0.20（速率实验：5.5req/s零错 / 8.5req/s现400 → 取安全带）
SORTS = ["title", None, "date"]
GAP_TRIGGER = 100
LEAF_RETRY_MAX = 2
SCHEMA = "v10"

# ---------------------------------------------------------------------------
# R2 自适应限速（AutoThrottle 式）· 修复版 v2（P4-A）
#
# v1 缺陷（由 crit-correct T1-T6、crit-adversarial A1/A3/A8、verify F11 实证）与 v2 修法：
#   ①【核心】延迟通道语义错误（verify F11 / crit-adv A8）：
#     v1 用 `target_delay = p80(所有请求耗时)/THROTTLE_TARGET_CONC(=1.0)` 当作「间隔目标」——
#     即把**响应耗时本身**当成了「该等多久」。真实延迟分布下（bulk 页 ≈17.4s / 探测 ≈0.12s，
#     audit §3.1、crit-benchmark E6）p80 被大页面主导 ⇒ 间隔被抬到 0.25s 且回落要先穿过该目标
#     ⇒ verify F11 实测管线内 −4.1%（slot-bench 场景 d −4.3%）。
#     v2 改法 = 任务书给的方案 (b)「重设计延迟通道触发条件」：
#       · **按请求类别隔离**统计（bulk=limit≥50 / light=其余）：大页面与小探测的耗时相差
#         两个数量级，混在一个 p80 里得到的既不是前者也不是后者；
#       · 判据改为「本类**近期 p50** / 本类**历史基线**（低分位 p10，劣化期冻结）」≥ 1.8 倍；
#         同分布下该比值恒 ≈1.0（实测 σ=0.3 时 30 样本 p50 的相对波动 ≈7%）⇒ 正常大页面
#         永不触发；只有分布整体平移（服务端真变慢）才 >1；
#       · 升幅上限 = 观测到的排队量级（基线×超幅）⇒ 不会像 v1 那样一路顶到上界自伤吞吐。
#     为什么不用方案 (a)「真实有效并发」：`acquire()` 在全局单锁内发号、`next_time = t + gap`，
#     代码上的有效并发恒为 1（并发度只影响谁先拿到号，不改变发号速率）；L 秒的响应期间确实有
#     ≈L/gap 个请求在飞，但那是**已经在等的排队**，把它当摊薄因子会得到 p80/gap ≈ 90 这种
#     无意义数值。更本质的是「发号速率是自变量、响应耗时是因变量」，用因变量反推自变量必然错。
#   ②【核心】恢复路径被错误自身堵死（crit-adv A3）：v1 只要窗口里留着 ≥2 条陈旧错误就 return，
#     放松分支永远轮不到 ⇒ 实测「持续低速率错误 → 2.28s 只回落到 1.5612s 卡死」。
#     v2：错误响应改为「窗口错误率 ⇒ 该守在哪个间隔」的 **AIMD 速率地板**
#     （THROTTLE_FLOOR_BACKOFF/DECAY），放松的下限就是这个地板 —— 错误率降下来地板自动降下来，
#     **结构上不可能再被堵死**（不存在"解锁"信号，也就没有解锁失败）。
#   ③ T1 稀疏 429 零响应：v1 的增长倍数绑「**连续**错误数」，不相邻的 429 恒有 f=1.0 ⇒ grow=0。
#     v2：倍数改为「突发个数」阶梯（单发 +6% / 连发 2 个 +12% / ≥3 个 +18%），
#     并用 `_err_pending` 保证「每一个 429/400 恰好被响应一次」。
#   ④ T2 滞回名实不符（文档 8 / 实测 20）：v2 放松判据 = 连续 THROTTLE_STABLE_STREAK 条无错
#     且放松地板已允许；常数 8 与"首次放松前的最少干净条数"逐项对齐（测试实测断言）。
#   ⑤ T3 上界过宽：8.0s → 1.2s（survey-throttle §B5 建议值）。旧值让一次持续劣化的恢复代价
#     达 220s；1.2s 把速率压到 0.83 req/s，对 B1 的冲击有界。
#   ⑥ T4 1s 桶内 6 发：地板 0.19s → 0.20s（半开 1s 桶恰好 ≤5 发 = 5.0 req/s < 5.5 安全带），
#     并新增**独立**的 1s 滑动窗口硬上限 RATE_WINDOW_MAX=5（结构兜底，不依赖地板正确）。
#   ⑦ T8 错误分类：`cls='conn'` 曾覆盖一切异常（JSONDecodeError/KeyError 被当网络错误升间隔）；
#     v2 只有模块级网络异常名单内的异常计入 'conn'。
#   ⑧ T9 `_hist` 无界增长：改为环形上限 THROTTLE_HIST_MAX + 分类计数。
#   ⑨ T5 读数用「响应完成时刻」：v2 改记「请求发出时刻」（每尝试一笔、幂等），
#     延迟劣化场景下节拍读数才可信。
#
# 语义不变：静态模式（SCRAPE_THROTTLE=static / --throttle=static）仍与基线逐行为等价
# （固定 min_interval + random.uniform(0, 0.03) 抖动），作为安全回退。
# ---------------------------------------------------------------------------
SAFETY_RATE_REQ_S = 5.5                       # 实测零错安全带（rate-profile.md §1；8.5 req/s 已现 400）
THROTTLE_MIN_INTERVAL = 0.20                  # 间隔硬下界（=5.0 req/s；半开 1s 桶内最多 5 发，见 T4）
THROTTLE_RATE_WINDOW = 1.0                    # 速率硬上限的窗口长度(s)（滑动、含本次）
THROTTLE_RATE_WINDOW_MAX = 5                  # 窗口内最多发几个请求（独立于地板的第二道闸）
assert THROTTLE_RATE_WINDOW_MAX / THROTTLE_RATE_WINDOW < SAFETY_RATE_REQ_S, \
    'R2 限速红线自检失败：窗口上限过宽，实际速率会突破安全带'
assert 1.0 / THROTTLE_MIN_INTERVAL * THROTTLE_RATE_WINDOW <= THROTTLE_RATE_WINDOW_MAX + 1e-9, \
    'R2 限速红线自检失败：地板与窗口上限不自洽（地板本身就会压穿窗口上限）'
THROTTLE_START_INTERVAL = 0.20                # 冷启动间隔（=基线 MIN_INTERVAL，保守起步）
THROTTLE_MAX_INTERVAL = 1.20                  # 间隔上界（=0.83 req/s；survey-throttle §B5 建议值）
THROTTLE_JITTER = 0.008                       # 抖动上限(s)：只用于打散相位，不承担速率调节
THROTTLE_WINDOW = 20                          # 滚动反馈窗口（状态码）观测条数
THROTTLE_WINDOW_MIN = 5                       # 窗口内至少这么多条才允许判定
THROTTLE_ADJUST_PERIOD = 0.20                 # 两次调整的最小间隔(s)，防每次请求都动
THROTTLE_STABLE_STREAK = 8                    # 连续这么多条无错（且窗口无残留错误）才允许放松
THROTTLE_RELAX_FRAC = 0.20                    # 放松步长 = 超出下界部分的 20%（几何式回落）
THROTTLE_RELAX_MIN_STEP = 0.002               # 放松步长下限(s)，贴近下界时寸进不震荡
THROTTLE_ERR_STEP_SINGLE = 0.06               # 单发错误：间隔 +6%（稀疏错误"恰好被响应一次"）
THROTTLE_ERR_STEP_PAIR = 0.12                 # 连发 2 个错误：+12%（可反复施加）
THROTTLE_ERR_STEP_BURST = 0.18                # 连发 ≥3 个错误：+18%（可反复施加）
THROTTLE_ERR_RATE_GROW_BURST = 1.50           # 稀疏错误的软上界：间隔不越过 地板×1.5（防爬升）
THROTTLE_RATE_WIN_S = 30.0                    # 错误率估计窗口(s)：时间窗（与请求速率无关），
                                              # 用于把「错误率」换算成「该守在哪个间隔」
THROTTLE_FLOOR_BACKOFF = 1.75                 # AIMD 乘性退避：每个错误把"速率地板"上抬 1.75×
                                              # （0.2 → 0.35 → 0.61 → 1.07 → 1.2s 封顶；一轮错误即退到
                                              #  0.35s=2.9 req/s，足以穿过 4.5 req/s 的服务端上限）
THROTTLE_FLOOR_CAP = THROTTLE_MAX_INTERVAL    # "速率地板"的绝对上限 = 间隔上界(1.2s ⇒ 0.83 req/s)。
                                              # 地板是"速率闸"：抬到间隔上界之上没有额外意义，
                                              # 只会让持续低速错误期的恢复更慢（实测 1.0 地板在
                                              #  200 条干净请求内回不到地板）。
THROTTLE_FLOOR_DECAY = 0.99                   # AIMD 加性恢复：每条干净请求让地板回落 1%
                                              # （错误停止后 ≈70 条干净请求回到地板；与错误段长度无关）
THROTTLE_CLEAN_RESET_N = 40                   # 连续这么多条干净请求 ⇒ 地板直接复位（"错误段结束"）
                                              # 实测定标：太小（20-25）会在"每 30 条一个 429"的
                                              # 持续错误流里误判为"结束"⇒ 反复忘记学到的上限；
                                              # 太大（100）则一次错误要 100 条干净请求才解锁恢复。
                                              # 用"连续干净条数"而不是时间窗：时间窗瞬时为 0 会在错误
                                              # 仍在持续时误复位（实测会让 AIMD 退避完全失效）
THROTTLE_ERR_FRESH_S = 3.0                    # 错误"新鲜度"阈值(s)：最近一次速率类错误在此时间内
                                              # 视为"还热" ⇒ 允许继续加码/压制放松
THROTTLE_GROW_EPS = 1e-6                      # 升间隔死区（相对值）：拒绝数值噪声级别的"增长"
THROTTLE_DEAD_ZONE = 0.01                     # 通用死区(s)：延迟型调整的最小有效幅度
THROTTLE_RELAX_DEAD_ZONE = 0.001              # 放松死区(s)：比通用死区小一个量级，允许贴近地板寸进
THROTTLE_OSC_MIN_MOVE = 0.01                  # 震荡计数滞回：反向累计幅度 ≥ 该值才算一次折返(s)
THROTTLE_ERR_STREAK_N = 3                     # 错误「突发」判定窗口（最近 N 条）
THROTTLE_ERR_STREAK_RL = 2                    # 最近 N 条内 ≥ 该数量的速率类错误 ⇒ 触发突发响应
THROTTLE_ERR_RL = 0.10                        # 窗口内速率类错误(429/400)比例阈值（红线 B3 主判据）
THROTTLE_ERR_RL_HOLD = 0.03                   # 低于阈值但高于该值 ⇒ 中性带：保持间隔不动（不卡死）
THROTTLE_ERR_RATE_OTHER = 0.10                # 窗口内其他错误（连接异常/5xx）比例阈值
THROTTLE_RECOVER_N = 10                       # 「错误已停止」判据窗口（最近 N 条无错 ⇒ 放开放松路径）
THROTTLE_HARD_ERR_GROW = 1.25                 # 非速率类错误(连接异常/5xx)的单次升间隔倍数
THROTTLE_COOLDOWN = 1.0                       # 错误反馈给出的冷却(s)：**固定小值、不随间隔放大**
                                              # （fetch 在 429 上已 cooldown(15+10·att)+sleep，
                                              #   最长 240s；限速器再加 interval×3 属于对同一个
                                              #   错误双计，survey §B5 明确要求消除）
THROTTLE_LAT_MIN_SAMPLES = 30                 # 延迟劣化判定的「近期样本」条数（每请求类别各自计）
THROTTLE_LAT_BASE_SAMPLES = 256               # 同类别「历史基线」样本条数（长窗口 ⇒ 中位数稳定）
THROTTLE_LAT_RISE_RATIO = 1.8                 # 近期 p50 / 历史基线 ≥ 该倍数 ⇒ 判为延迟劣化
THROTTLE_LAT_BASE_LOCK = 1.5                  # 近期 p50 / 候选基线 ≥ 该倍数 ⇒ 冻结基线（不学习劣化期）
THROTTLE_LAT_GROW_SLOPE = 0.5                 # 劣化幅度 → 升间隔：f = 1 + clamp(SLOPE×超幅, MIN, MAX)
THROTTLE_LAT_GROW_MIN = 0.05                  # 延迟型升间隔最小比例
THROTTLE_LAT_GROW_MAX = 0.50                  # 延迟型升间隔最大比例（单次最多 +50%）
THROTTLE_LOG_EVERY = 25                       # 每 N 次请求打印一行状态（[throttle] ...）
THROTTLE_HIST_MAX = 512                       # 调整事件账本环形上限（T9：原实现无界增长）
_LOG_PREFIX = '[throttle]'                    # 状态行前缀（供测试/审计 grep 读数）

# 请求类别：延迟基线必须按类别隔离 —— bulk(limit≥50) 与探测类(limit=1) 的耗时相差两个数量级，
# 混在一个 p80 里得到的既不是 bulk 的耗时也不是探测的耗时（verify F11 的根因之一）。
THROTTLE_BULK_LIMIT = 50                      # fetch(limit≥该值) ⇒ 类别 'bulk'
THROTTLE_KINDS = ('bulk', 'light')            # 参与延迟劣化判定的类别
_R2_MODE_ALIASES = {'auto': 'adaptive', 'adaptive': 'adaptive', '1': 'adaptive',
                    'on': 'adaptive', 'true': 'adaptive', 'yes': 'adaptive',
                    'static': 'static', 'fixed': 'static', '0': 'static',
                    'off': 'static', 'false': 'static', 'no': 'static'}

def _r2_throttle_env_mode():
    """SCRAPE_THROTTLE 环境变量的模式解析。非法值 ⇒ 记一行告警并**回退 adaptive**（fail-safe 方向）。"""
    raw = (os.environ.get('SCRAPE_THROTTLE') or '').strip().lower()
    if not raw:
        return 'adaptive'
    if raw in _R2_MODE_ALIASES:
        return _R2_MODE_ALIASES[raw]
    sys.stderr.write(f"[throttle] 非法 SCRAPE_THROTTLE={raw!r} → 忽略，改用 adaptive\n")
    return 'adaptive'

def _r2_throttle_usage(argv):
    """把 argv 还原成「诊断行」，便于日志/测试断言（不做解析）。"""
    return ' '.join(repr(a) for a in (argv or []))

def _r2_throttle_cli(argv):
    """CLI 解析：`--throttle=auto|static` 或 `--throttle auto|static`（值紧邻，不越位吃参数）。

    返回 (mode_or_None, rest, err)。**从不静默**：
      · 无值（`--throttle` 是最后一个参数，或下一个 token 以 `--` 开头）⇒ err='missing-value'
      · 值不是枚举 ⇒ err='bad-mode:<值>'（**绝不当种子**，也绝不静默改模式）
      · 合法 ⇒ 该开关被摘除，其余参数原样保留（避免「静默吃掉一个种子」）
    """
    mode, rest, i, args = None, [], 0, list(argv or [])
    while i < len(args):
        a = args[i]
        if a == '--throttle':
            nxt = args[i + 1] if i + 1 < len(args) else None
            if nxt is None or nxt.startswith('-'):
                return None, rest, 'missing-value'
            if nxt.strip().lower() not in _R2_MODE_ALIASES:
                return None, rest, f'bad-mode:{nxt}'
            mode = _R2_MODE_ALIASES[nxt.strip().lower()]
            i += 2
            continue
        if a.startswith('--throttle='):
            val = a.split('=', 1)[1].strip().lower()
            if val not in _R2_MODE_ALIASES:
                return None, rest, f'bad-mode:{val or "<empty>"}'
            mode = _R2_MODE_ALIASES[val]
            i += 1
            continue
        rest.append(a)
        i += 1
    return mode, rest, None

def _r2_throttle_mode(argv=None, strict=True):
    """限速模式解析：CLI（--throttle=… / --throttle …）> 环境变量 SCRAPE_THROTTLE > 默认 adaptive。

    返回 (mode, 过滤掉该开关后的 argv)。strict=True 时解析失败**直接退出**（code 2），
    不再像 v1 那样把 `--throttle` 漏进种子列表（crit-adv A9 实测会变成非法前缀 → 真实 400）。
    """
    cli_mode, rest, err = _r2_throttle_cli(argv)
    if err is not None:
        msg = (f"[throttle] 参数错误：--throttle {err}\n"
               f"          用法：--throttle=auto|static 或 --throttle auto|static\n"
               f"          收到：{_r2_throttle_usage(argv)}\n")
        if strict:
            sys.stderr.write(msg)
            sys.exit(2)
        return None, rest
    leaked = [a for a in rest if a == '--throttle' or a.startswith('--throttle=')]
    if leaked:
        msg = (f"[throttle] 内部错误：--throttle 泄漏到种子列表 {leaked!r}（拒绝启动，避免真实 400）\n")
        if strict:
            sys.stderr.write(msg)
            sys.exit(2)
        return None, rest
    if cli_mode is not None:
        return cli_mode, rest
    return _r2_throttle_env_mode(), rest

THROTTLE_MODE, _MAIN_ARGV = _r2_throttle_mode(sys.argv[1:])   # 自适应/静态开关（默认 adaptive）
_R2_FIX_V2 = True          # P4-A 修复版标记（测试夹具据此跳过 v1 签名适配层）

# ---------------------------------------------------------------------------
# R2 · P4-C 新增独立区块 —— 项级并发预算（Crawlee AutoscaledPool 思想的简化采纳）
#
#   目的：v10.1 主循环「一次只推进一个 item」使 LIMITER 占空比仅 ~12%（审计 §2.2），
#         B1（≥500 unique/min）的唯一实质杠杆是**项级 in-flight 重叠**（审计 §4.1/§5 O1）。
#   边界：本区块只决定「同时推进几个 item」；**不改变**全局 LIMITER 的放行/冷却语义
#         （RateLimiter L703-723 逐字未改），也不改变进程内持久化语义。
#   对标：Crawlee AutoscaledPool 的 desiredConcurrency + scaleUp/scaleDown
#         （R1/sandbox/bench-src/.../autoscaling/autoscaled_pool.js:265,566-638）；
#         按其 §5.2「不要抄」清单，未引入 per-domain 状态、CPU/事件循环信号与
#         多层嵌套池：本工程单出口单域，预算 = 一个整数 + 一次滞回即可。
#
#   参数全部可经环境变量覆盖（与 SCRAPE_OUT_DIR 同一先例），便于 A/B 复跑与验收；
#   默认值即 R2 建议值（审计 §5 硬约束 1：c=3，预留 2× 余量，LIMITER 触顶约在 c≈8）。
# ---------------------------------------------------------------------------
def _env_num(name, default, cast):
    v = os.environ.get(name)
    if v is None or v == '':
        return default
    try:
        return cast(v)
    except (TypeError, ValueError):
        return default

W_MAX = _env_num('SCRAPE_W_MAX', 3, int)            # 并发预算上限（在飞 item 数）
W_MIN = _env_num('SCRAPE_W_MIN', 1, int)            # 并发预算下限（1 = 串行等效；限流风暴期不得为 0）
CONC_ENABLED = _env_num('SCRAPE_CONC', 1, int)      # 0 = 退回基线串行语义（对照/回滚开关）
CONC_ADAPTIVE = _env_num('SCRAPE_CONC_ADAPT', 1, int)   # 0 = 固定并发（不因错误降档）
W_SCALE_UP_AFTER_S = _env_num('SCRAPE_W_UP_S', 30.0, float)   # 静默多久后 +1（上限 W_MAX）
W_LOG_EVERY_S = _env_num('SCRAPE_W_LOG_S', 30.0, float)       # [conc] 观测行周期
CONC_DEFER_SLOTS = _env_num('SCRAPE_DEFER_SLOTS', 1, int)     # 1 = 冷却期不占用工作槽（条目级延迟重放）
LEAF_RETRY_DELAY_S = _env_num('SCRAPE_LEAF_RETRY_S', 45.0, float)        # = 基线 cooldown(45)+sleep(45)
BRANCH_RECHECK_DELAY_S = _env_num('SCRAPE_BRANCH_RECHECK_S', 20.0, float)  # = 基线 cooldown(20)+sleep(20)

# 本模块**自己**发出的冷却截止时间：用于把「自伤冷却」从错误信号里排除
# （观测器只读 LIMITER.cooldown_until，不包装、不修改 RateLimiter 实现）。
_SELF_COOLDOWN_UNTIL = 0.0
_CONC_LAST_EXT_COOLDOWN = 0.0
_CONC_ERR_EVENTS = 0

LOG_LOCK = threading.Lock()
def log(msg):
    line = f"[{datetime.now().strftime('%H:%M:%S')}] {msg}"
    with LOG_LOCK:
        print(line, flush=True)
        try:                                            # R3·slot-converge (F3)
            with open(LOG_FILE, 'a') as f:
                f.write(line + "\n")
        except OSError:
            # output 目录消失/只读/磁盘异常：重建后重试一次；仍失败也**不得**打死长程任务
            # （v10 原实现裸 open(...,'a') → FileNotFoundError 从任意 log() 调用点冲出主循环）
            if ensure_out_dir('log'):
                try:
                    with open(LOG_FILE, 'a') as f:
                        f.write(line + "\n")
                except OSError:
                    pass

# ---------------------------------------------------------------------------
# v10 持久化层：原子写 + records 反查
# ---------------------------------------------------------------------------
_progress_lock = threading.RLock()
_records_lock = threading.Lock()
_records_fd = None
# prefix -> 该 prefix 在 records.jsonl 中「至少」有多少条互异 mms（启动时精确扫描，
# 运行中按 append 单调更新；是下界，只可能少算 → 只会多抓，不会误跳过）
RECORDED_COUNTS = {}

# ---------------------------------------------------------------------------
# v10-fix（ADV-1）：撕裂/损坏行波及的 prefix —— 容差判定对它们**一律不适用**
#   缺陷：撕裂丢 1 条记录时 cnt = total-1，leaf_tol=1.5% 总是把它洗成「已完成」，
#   无补抓、无审计 ⇒ 静默永久丢失（ADV-1e 矩阵 11 个尺寸里 9 个如此）。
#   现在：只要一条 prefix 的记录行被撕裂/损坏波及，它就必须回队重抓；
#   「容差完成」也必须在 recovery[] 留痕（不允许静默）。
# ---------------------------------------------------------------------------
FORCE_REQUEUE = set()        # 本次进程内生效（reconcile 时重建，mark_leaf_done 时解除）
LAST_REPAIR_INFO = {}        # 最近一次 repair_records_tail() 的结构化结果
TAINT_KEY = 'torn_requeue'   # progress.json 里的持久化污点账本（跨进程重启仍生效）

def _write_all(fd, data):
    """单次系统调用写完整块；仅在极罕见短写时才追加系统调用。"""
    view = memoryview(data)
    while view:
        n = os.write(fd, view)
        if n <= 0:
            raise OSError("short write to fd %r" % fd)
        view = view[n:]

def _fsync_dir(path):
    try:
        dfd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(dfd)
    except OSError:
        pass
    finally:
        os.close(dfd)

# ---------------------------------------------------------------------------
# 故障注入测试缝（默认彻底关闭）
# ---------------------------------------------------------------------------
FAULT_POINTS = ('before_records', 'after_records', 'before_progress', 'after_progress')
_fault_state = {}

def _fault(point, prefix=None):
    """仅当 SCRAPE_FAULT=<point> 且 SCRAPE_ALLOW_FAULTS=1 时生效；否则完全惰性。
    SCRAPE_FAULT_N=k 指定第 k 次命中；SCRAPE_FAULT_PREFIX=<p> 只对该 prefix 命中。"""
    spec = os.environ.get('SCRAPE_FAULT')
    if not spec or os.environ.get('SCRAPE_ALLOW_FAULTS') != '1' or spec not in FAULT_POINTS:
        return
    if point != spec:
        return
    want_prefix = os.environ.get('SCRAPE_FAULT_PREFIX')
    if want_prefix and prefix != want_prefix:
        return
    n = _fault_state.get(point, 0) + 1
    _fault_state[point] = n
    try:
        want_n = int(os.environ.get('SCRAPE_FAULT_N', '1') or '1')
    except ValueError:
        want_n = 1
    if n != want_n:
        return
    log(f"=== FAULT-INJECT {point} n={n} prefix={prefix!r} pid={os.getpid()} → SIGKILL ===")
    os.kill(os.getpid(), signal.SIGKILL)

def _fd_alive(fd):
    """fd 指向的文件是否仍链接在目录里（R3·F3）。

    `rm` 掉 output 目录/文件后，已打开的 fd 仍然"可写"，但数据全进黑洞
    （inode 已无目录项，st_nlink==0）—— 这是比 FileNotFoundError 更隐蔽的丢数据路径。"""
    try:
        return os.fstat(fd).st_nlink > 0
    except OSError:
        return False


def _records_fd_get():
    """records.jsonl 追加句柄（O_APPEND）。

    R3·slot-converge (F3)：句柄按 **output 目录世代**（_OUT_DIR_EPOCH）+ **inode 存活**
    双重校验 —— 目录被删掉重建后，旧 fd 指向的是已删除的 inode，继续写会**静默丢数据**
    （不报错、文件不在），因此任一条件不满足都先关旧句柄再重开。"""
    global _records_fd, _RECORDS_FD_EPOCH
    if _records_fd is not None and (_RECORDS_FD_EPOCH != _OUT_DIR_EPOCH
                                    or not _fd_alive(_records_fd)):
        try:
            os.close(_records_fd)
        except OSError:
            pass
        _records_fd = None
    if _records_fd is None:
        _records_fd = os.open(str(RECORDS_FILE),
                              os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        _RECORDS_FD_EPOCH = _OUT_DIR_EPOCH
    return _records_fd


def _records_write_resilient(data, prefix=None):
    """records 追加的韧性写（必须在 _records_lock 内调用）：检测→重建→重试 3 次。

    R3·slot-converge (F3)：目录消失 / 只读 / ENOSPC 之类的瞬时故障不得打死长程任务。
    返回 True=已 fsync 落盘；False=已放弃（由调用方转入待写缓冲 + 审计）。"""
    global _records_fd
    last = None
    for attempt in (1, 2, 3):
        try:
            if not OUT.is_dir():
                _ensure_out_dir_raw('records')
                _note_dir_recreated('records')       # R3·F3：数据面先撞上目录消失也要留审计
            fd = _records_fd_get()
            _write_all(fd, data)
            os.fsync(fd)
            return True
        except OSError as e:
            last = e
            try:
                if _records_fd is not None:
                    os.close(_records_fd)
            except OSError:
                pass
            _records_fd = None
            time.sleep(0.05 * attempt)
    audit_event('records_write_deferred', 'process',
                {'prefix': prefix, 'bytes': len(data),
                 'err': f'{type(last).__name__}: {last}',
                 'deferred_records': _RECORDS_DEFERRED})
    try:
        log(f"[r3] records 写入失败（{type(last).__name__}: {last}）→ 转入待写缓冲"
            f"（{_RECORDS_DEFERRED} 条待补写），抓取继续；下次追加时优先补写")
    except Exception:
        pass
    return False


def _records_pending_flush():
    """把待写缓冲拼到本次数据前面（**必须在 _records_lock 内**）；返回 (blob, n_pending)。"""
    pend = _RECORDS_PENDING
    blob = b''.join(pend)
    return blob, len(pend)


def flush_pending_records():
    """收尾/显式冲刷待写缓冲。返回仍未落盘的记录条数（0=全部落盘）。"""
    global _RECORDS_DEFERRED
    with _records_lock:
        if not _RECORDS_PENDING:
            return 0
        blob = b''.join(_RECORDS_PENDING)
        if _records_write_resilient(blob, prefix=None):
            del _RECORDS_PENDING[:]
            _RECORDS_DEFERRED = 0
            return 0
        return _RECORDS_DEFERRED


def append_records(recs, prefix=None):
    """行级原子追加：整块先序列化 → 单次 O_APPEND write → fsync。
    单次 os.write 在 kill -9 下不会被撕裂（内核写完该系统调用才投递信号），
    fsync 保证断电后已确认的记录不丢。返回写入条数。

    R3·slot-converge (F3)：目录消失/写失败 ⇒ 重建重试，仍失败转待写缓冲（有上限），
    审计 records_write_deferred —— 数据面宁可"延迟落盘 + 有痕迹"，不可"进程被打死"。"""
    global _RECORDS_DEFERRED
    if not recs:
        return 0
    blob = b''.join(json.dumps(r, ensure_ascii=False).encode('utf-8') + b'\n'
                    for r in recs)
    with _records_lock:
        pend_blob, n_pend = _records_pending_flush()
        _fault('before_records', prefix)
        ok = _records_write_resilient(pend_blob + blob, prefix)
        if ok:
            del _RECORDS_PENDING[:]
            _RECORDS_DEFERRED = 0
        else:
            _RECORDS_PENDING.append(blob)
            _RECORDS_DEFERRED += len(recs)
            if _RECORDS_DEFERRED > RECORDS_PENDING_MAX:      # 上限保护：丢最旧的待写块
                dropped = _RECORDS_PENDING.pop(0)
                _RECORDS_DEFERRED -= dropped.count(b'\n')
                audit_event('records_write_dropped', 'process',
                            {'reason': 'pending-buffer-overflow',
                             'limit': RECORDS_PENDING_MAX,
                             'dropped_records': dropped.count(b'\n')})
        _fault('after_records', prefix)
    # 只有**确认落盘**才记入反查计数：反查闸门 is_recorded_done() 必须只相信磁盘
    if ok and prefix:
        note_recorded(prefix, len(recs))
    return len(recs) if ok else 0

def note_recorded(prefix, n):
    if prefix and n:
        RECORDED_COUNTS[prefix] = max(RECORDED_COUNTS.get(prefix, 0), int(n))

def recorded_count(prefix):
    return RECORDED_COUNTS.get(prefix, 0)

def leaf_tol(total):
    return max(2, int(total * 0.015))

def is_recorded_done(prefix, total):
    """该叶子是否已由 records.jsonl 反查证明「已完整落盘」。
    注意 cnt>0 是硬条件：total 很小时 leaf_tol() 会放大到 2，若不要求至少 1 条，
    「从未抓过的叶子」会被误判为已完成（= 漏抓）。

    v10-fix（ADV-1a）：被撕裂/损坏行波及的 prefix（FORCE_REQUEUE）**不参与容差判定**
    —— 撕裂丢 1 条记录时 cnt=total-1 永远落在容差内，那正是静默丢失的来源。"""
    if not total or total <= 0 or total > LEAF_MAX or not prefix:
        return False
    if prefix in FORCE_REQUEUE:
        return False
    cnt = recorded_count(prefix)
    return cnt > 0 and cnt >= total - leaf_tol(total)


def _tolerance_mark(prog, prefix, cnt, total, where):
    """容差命中（cnt < total 但 cnt >= total - tol）必须留审计，禁止静默。
    返回 True 表示这次判定是「容差完成」而非精确完成。"""
    if not total or cnt >= total:
        return False
    tol = leaf_tol(total)
    st = prog.setdefault('stats', {})
    st['tolerance_marks'] = st.get('tolerance_marks', 0) + 1
    _recovery_note(prog, 'tolerance->done', prefix,
                   f'{where}: records 反查 {cnt}/{total}，缺 {total - cnt} 条但在容差 {tol} 内 '
                   f'→ 按容差判定完成（非精确完成，已在 recovery[] 留痕）')
    return True

def _write_progress_atomic(data, tmp):
    """progress.json 的原子落盘（v10 逐行语义，抽成函数供 F3 重试复用）。"""
    fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        _write_all(fd, data)
        os.fsync(fd)          # 必须先落盘再改名：否则断电后改名指向的可能是空文件
    finally:
        os.close(fd)
    if PROGRESS_FILE.exists():
        os.replace(str(PROGRESS_FILE), str(PROGRESS_BAK))   # 旧好状态进备份槽（原子）
    os.replace(str(tmp), str(PROGRESS_FILE))
    _fsync_dir(OUT)


def save_progress(prog, ctx=None):
    """原子替换 + 双槽备份：任何时刻磁盘上都不存在半写 progress.json。

    R3·slot-converge (F3)：output 目录消失/瞬时 IO 异常 ⇒ 检测 + 重建 + 重试一次，
    仍失败只写 progress_write_failed 审计并继续（v10 原实现会让 FileNotFoundError
    从 8 个调用点之一冲出去打死长程任务）。"""
    prog['schema'] = SCHEMA
    prog['saved'] = datetime.now().isoformat()
    data = json.dumps(prog, ensure_ascii=False, indent=1).encode('utf-8')
    tmp = PROGRESS_FILE.with_name(
        f"progress.json.tmp.{os.getpid()}.{threading.get_ident()}")
    with _progress_lock:
        _fault('before_progress', ctx)
        try:
            _write_progress_atomic(data, tmp)
        except OSError as _e1:
            _recreated = ensure_out_dir('progress')                  # R3 (F3)
            try:
                _write_progress_atomic(data, tmp)
            except OSError as _e2:
                audit_event('progress_write_failed', 'process',
                            {'err': f'{type(_e2).__name__}: {_e2}',
                             'first_err': f'{type(_e1).__name__}: {_e1}',
                             'recreated': bool(_recreated), 'ctx': ctx})
                try:                                                 # 只告警，不抛出
                    log(f"[r3] progress.json 落盘失败（{type(_e2).__name__}: {_e2}）→ 本轮跳过，"
                        f"进度以下一成功快照为准；已写 progress_write_failed 审计")
                except Exception:
                    pass
        _fault('after_progress', ctx)
    # R2·心跳/水位：progress 落盘之后再写（X2 缺口闭合）。
    # P4/H1（crit-correct H1）：监控 sidecar **不得**有能力中断数据路径 —— 心跳模块缺席
    # （bench/沙箱的"只拷 scrape.py"布局）、同名第三方包（AttributeError）、心跳自身异常，
    # 都必须降级为"本次跳过 + 单次告警"。旧实现是裸 `import heartbeat`：ModuleNotFoundError
    # 会从 8 个无保护的 save_progress 调用点抛出 → 主循环崩 → keeper 重启 → 再崩（崩溃环）。
    # 与 slot-mem 的接线保持一致（那边本来就是显式 fail-open）。
    try:
        import heartbeat as _hb
        _hb.write_heartbeat(prog, prefix=ctx)
    except Exception as _hb_err:                      # noqa: BLE001 —— 故意兜住一切
        _hb_state = globals().setdefault('_HEARTBEAT_WARN', {'n': 0, 'warned': 0})
        _hb_state['n'] = _hb_state.get('n', 0) + 1
        if _hb_state.get('warned', 0) < 1:            # 单次告警：不刷日志、不刷 IO
            _hb_state['warned'] = _hb_state.get('warned', 0) + 1
            try:
                log(f"[heartbeat] 心跳写入被跳过（{type(_hb_err).__name__}: {_hb_err}）"
                    f" → 抓取主流程继续；后续同类错误不再重复告警")
            except Exception:
                pass

def _read_progress_file(path):
    try:
        if not path.exists() or path.stat().st_size == 0:
            return None
        prog = json.loads(path.read_text())
        return prog if isinstance(prog, dict) and 'todo' in prog else None
    except (OSError, ValueError):
        return None

def load_progress():
    prog = _read_progress_file(PROGRESS_FILE)
    if prog is None:
        prog = _read_progress_file(PROGRESS_BAK)
        if prog is not None:
            log("progress.json 不可用（缺失/空/损坏）→ 回退 progress.json.bak")
            validate_progress(prog, 'progress.json.bak')
        return prog
    validate_progress(prog, 'progress.json')
    return prog


# progress.json 的 schema 契约（P4 修订轮·余力项）：load 时校验并告警，不拒绝加载
# （向后兼容：v9 世代缺 done/recovery 的 progress 仍可用，只是被记一条告警）。
PROGRESS_REQUIRED = (('todo', list), ('done', list), ('gaps', list),
                     ('recovery', list), ('stats', dict))
LAST_SCHEMA_PROBLEMS = []


def validate_progress(prog, where='progress.json'):
    """校验 progress 结构，返回问题列表（同时写告警日志）。不抛异常。"""
    global LAST_SCHEMA_PROBLEMS
    problems = []
    if not isinstance(prog, dict):
        LAST_SCHEMA_PROBLEMS = ['不是 JSON 对象']
        log(f"[progress-schema] {where}: 不是 JSON 对象")
        return LAST_SCHEMA_PROBLEMS
    for key, typ in PROGRESS_REQUIRED:
        if key not in prog:
            problems.append(f'缺字段 {key}')
        elif not isinstance(prog[key], typ):
            problems.append(f'{key} 应为 {typ.__name__}，实际 {type(prog[key]).__name__}')
    for i, it in enumerate(prog.get('todo') or []):
        if not isinstance(it, dict) or not it.get('p'):
            problems.append(f'todo[{i}] 缺少 p')
        elif it.get('total') is not None and not isinstance(it['total'], int):
            problems.append(f'todo[{i}].total 应为 int/None，实际 {type(it["total"]).__name__}')
    for i, d in enumerate(prog.get('done') or []):
        if not isinstance(d, dict) or not d.get('p'):
            problems.append(f'done[{i}] 缺少 p')
    for i, g in enumerate(prog.get('gaps') or []):
        if not isinstance(g, dict) or not g.get('p'):
            problems.append(f'gaps[{i}] 缺少 p')
    for i, e in enumerate(prog.get('recovery') or []):
        if not isinstance(e, dict) or 'action' not in e:
            problems.append(f'recovery[{i}] 缺少 action')
    if not isinstance(prog.get('stats'), dict) or 'leaves' not in (prog.get('stats') or {}):
        problems.append('stats 缺少 leaves')
    sch = prog.get('schema')
    if sch is not None and sch != SCHEMA:
        problems.append(f'schema={sch!r} != {SCHEMA}')
    if prog.get(TAINT_KEY) is not None and not isinstance(prog[TAINT_KEY], list):
        problems.append(f'{TAINT_KEY} 应为 list')
    LAST_SCHEMA_PROBLEMS = problems
    if problems:
        log(f"[progress-schema] {where} 校验告警 {len(problems)} 项: {problems[:8]}"
            + (" …" if len(problems) > 8 else ""))
    return problems

_REC_PREFIX_RE = re.compile(rb'"prefix"\s*:\s*"([^"]*)"')


def _valid_rec(raw):
    """整行（含换行）若是完整有效记录则返回 dict，否则 None。"""
    if not raw.endswith(b'\n'):          # 半写行（无换行结尾）一定无效
        return None
    try:
        cand = json.loads(raw.decode('utf-8'))
    except (ValueError, UnicodeDecodeError):
        return None
    if (isinstance(cand, dict) and isinstance(cand.get('mms'), str)
            and cand['mms'] and 'prefix' in cand):
        return cand
    return None


def _salvage_tail(raw):
    """从损坏行里抢救出「完整记录」的原始字节切片（v10-fix ADV-1b）。

    短写（ENOSPC/EFBIG/EDQUOT/双写者）会把「半条记录」与随后某次 append 的
    完整记录黏成同一物理行。坏行里第一个能**完整解析**为记录的位置起到行尾，
    就是那条记录的原文 ⇒ 逐字节保留，只有半条记录的碎片被丢弃。
    返回 (原始字节, 起始偏移) 或 (None, None)。"""
    body = raw[:-1] if raw.endswith(b'\n') else raw
    if b'\n' in body:
        return None, None
    try:
        text = body.decode('utf-8')
    except UnicodeDecodeError:
        return None, None
    # 候选起点：优先找记录序列化形态 '{"mms"'（append_records 的 json.dumps 形态），
    # 全部试完再退化为任意 '{'。这样"碎片在前、完整记录在后"的现实位形第 2 次即命中，
    # 不会因为碎片内部的花括号（title 里的 '{'、holdings 的嵌套对象）而耗尽尝试次数。
    for needle in ('{"mms"', '{'):
        pos, tries = text.find(needle), 0
        while pos >= 0 and tries < 64:
            tries += 1
            try:
                cand = json.loads(text[pos:])
            except ValueError:
                cand = None
            if (isinstance(cand, dict) and isinstance(cand.get('mms'), str)
                    and cand['mms'] and 'prefix' in cand):
                start = len(text[:pos].encode('utf-8'))
                return raw[start:], start
            pos = text.find(needle, pos + 1)
    return None, None


def repair_records_tail():
    """扫描 records.jsonl，**最小化丢弃**地修复损坏行。

    v10 原实现：在第一条损坏/半写行处 truncate —— 其后**全部完整记录**一并丢弃
    （ADV-1.7 实测：一次短写丢 7,358 字节 / 24 行完整记录，23 个 unique mms 消失）。
    v10-fix 改为：

      1. 逐行扫描，只处理损坏行本身，其后的完整行原样保留（原地压缩重写，
         与原实现同样避免 unlink inode —— 进程可能已持有 O_APPEND fd）；
      2. 损坏行先抢救其尾部完整记录（_salvage_tail），抢救出的字节逐字节保留；
      3. 末行不完整（无换行结尾）⇒ torn_tail=True，并把该 prefix 记入
         FORCE_REQUEUE（容差对它不适用 ⇒ 强制回队重抓，不会静默丢失）；
      4. 没有任何损坏行时**不写盘**（文件字节与 mtime 不变，可反复执行）。

    返回 (保留行数, 丢弃行数, 受影响 prefix 列表)；结构化细节在 LAST_REPAIR_INFO。
    """
    global LAST_REPAIR_INFO
    LAST_REPAIR_INFO = {'damaged_lines': 0, 'salvaged_records': 0, 'dropped_lines': 0,
                        'torn_tail': False, 'prefixes': [], 'salvaged_prefixes': [],
                        'prefix_inferred': 0, 'unnamed_damage': 0, 'kept_lines': 0,
                        'salvaged_newline': 0,
                        'bytes_in': 0, 'bytes_out': 0, 'bytes_dropped': 0}
    if not RECORDS_FILE.exists():
        return 0, 0, []
    info = LAST_REPAIR_INFO
    info['bytes_in'] = RECORDS_FILE.stat().st_size
    kept = dropped = damaged = 0
    prefixes, salvaged_prefixes = [], []
    prev_prefix = None
    read_pos = write_pos = 0

    with open(str(RECORDS_FILE), 'rb') as fr, open(str(RECORDS_FILE), 'r+b') as fw:
        while True:
            raw = fr.readline()
            if not raw:
                break
            rec = _valid_rec(raw)
            if rec is not None:
                kept += 1
                prev_prefix = rec.get('prefix')
            else:
                damaged += 1
                torn_here = not raw.endswith(b'\n')
                if torn_here:
                    info['torn_tail'] = True
                salv, salv_off = _salvage_tail(raw)
                # —— prefix 归属（决定谁必须回队重抓）——
                # (i) 坏行里位于「抢救起点之前」的 "prefix" 字段 = 被短写截断的那条记录；
                # (ii) 否则退化为「上一条完整记录的 prefix」——同一次 append 的整块共享
                #      prefix，撕裂/短写只可能切在本块内部（C5 的 60 字节尾行即此情形，
                #      其 "prefix" 字段根本没写进文件）；
                # (iii) 末行不完整时，抢救出来的整条记录所属的 prefix 也算受损
                #      （那一笔 append 可能还有后续记录没落盘，必须重抓才能证明没丢）。
                m = _REC_PREFIX_RE.search(raw)
                if m and (salv_off is None or m.start() < salv_off):
                    prefixes.append(m.group(1).decode('utf-8', 'replace'))
                elif prev_prefix:
                    prefixes.append(prev_prefix)
                    info['prefix_inferred'] += 1
                else:
                    info['unnamed_damage'] += 1
                if salv is None:
                    dropped += 1
                    read_pos += len(raw)     # 只丢弃这一行；其后的完整记录继续原样保留
                    continue
                info['salvaged_records'] += 1
                kept += 1
                if torn_here:
                    salv = salv + b'\n'      # 记录本身完整、只差换行符 → 补齐
                    info['salvaged_newline'] = info.get('salvaged_newline', 0) + 1
                try:
                    sr = json.loads(salv.decode('utf-8'))
                    if sr.get('prefix'):
                        salvaged_prefixes.append(sr['prefix'])
                        if torn_here:
                            prefixes.append(sr['prefix'])
                except (ValueError, UnicodeDecodeError):
                    pass
                if salv != raw or write_pos != read_pos:
                    fw.seek(write_pos)
                    fw.write(salv)           # 抢救字节与原文不同（如补了换行）时也必须落盘
                write_pos += len(salv)
                read_pos += len(raw)
                continue
            if write_pos != read_pos:
                fw.seek(write_pos)
                fw.write(raw)
            write_pos += len(raw)
            read_pos += len(raw)

        if damaged:
            fw.truncate(write_pos)
            fw.flush()
            os.fsync(fw.fileno())

    # 自校验（只在确实修过文件时执行，正常路径零成本）：修复后不应再有任何损坏行。
    # 这是把「不变量假设」变成「不变量已验」——原地压缩写一旦算错偏移，
    # 这里会立刻告警，而不是等到交付件里出现一条静默丢弃。
    if damaged:
        with open(str(RECORDS_FILE), 'rb') as fc:
            left = sum(1 for raw in fc if _valid_rec(raw) is None)
        if left:
            log(f"!! records 修复后仍有 {left} 行损坏 —— 需人工检查（info={info}）")
            info['remaining_damaged'] = left

    info['kept_lines'] = kept
    info['dropped_lines'] = dropped
    info['damaged_lines'] = damaged
    info['prefixes'] = sorted(set(prefixes))
    info['salvaged_prefixes'] = sorted(set(salvaged_prefixes))
    info['bytes_out'] = write_pos
    info['bytes_dropped'] = info['bytes_in'] - write_pos
    if not damaged:
        return kept, 0, []
    log(f"records 修复（最小丢弃）：损坏行 {damaged} 行（末行不完整={info['torn_tail']}）；"
        f"抢救完整记录 {info['salvaged_records']} 条、丢弃不可恢复片段 {dropped} 行；"
        f"保留 {kept} 行、{info['bytes_in'] - write_pos} 字节；"
        f"涉及 prefix={info['prefixes']}（其中由前一条推断 {info['prefix_inferred']} 个）")
    return kept, dropped, info['prefixes']

def scan_records(prefixes=None):
    """流式扫描 records.jsonl。
    prefixes 非空 → 只统计这些 prefix 的 mms 集合（135MB 级文件也保持低内存）；
    prefixes 为空 → 统计全部 prefix，mms 以 8 字节 blake2b 摘要存放（全量重建用）。
    返回 (有效行数, {prefix: set})。"""
    want = set(prefixes) if prefixes is not None else None
    seen = {}
    lines = 0
    if want is not None:
        for p in want:
            seen[p] = set()
    if not RECORDS_FILE.exists():
        return 0, seen
    with open(str(RECORDS_FILE), 'rb') as f:
        for raw in f:
            if not raw.endswith(b'\n'):
                continue
            try:
                rec = json.loads(raw.decode('utf-8'))
            except (ValueError, UnicodeDecodeError):
                continue
            mms = rec.get('mms')
            p = rec.get('prefix')
            if not mms or p is None:
                continue
            lines += 1
            key = mms if want is not None else hashlib.blake2b(
                mms.encode('utf-8'), digest_size=8).digest()
            if want is not None:
                if p in want:
                    seen[p].add(key)
            else:
                seen.setdefault(p, set()).add(key)
    return lines, seen

def _recovery_note(prog, action, prefix, detail):
    entry = {'ts': datetime.now().isoformat(), 'action': action,
             'prefix': prefix, 'detail': detail}
    rec = prog.setdefault('recovery', [])
    rec.append(entry)
    if len(rec) > 500:
        del rec[:-500]
    log(f"  自愈[{action}] {prefix!r}: {detail}")

def mark_leaf_done(prog, prefix, total, got, recovered=False):
    prog.setdefault('done', []).append(
        {'p': prefix, 'total': total, 'got': got,
         'ts': datetime.now().isoformat(), 'recovered': bool(recovered)})
    st = prog.setdefault('stats', {})
    st['leaves'] = st.get('leaves', 0) + 1
    st['records'] = st.get('records', 0) + int(got or 0)
    if recovered:
        st['recovered'] = st.get('recovered', 0) + 1
    # v10-fix（ADV-1a）：真正抓取过（recovered=False）或已补满 total ⇒ 解除撕裂污点；
    # 仅「反查补登」（recovered=True）不算，避免容差把丢记录固化成已完成。
    if prefix and (not recovered or (total and got >= total)):
        _clear_taint(prog, prefix)


def _clear_taint(prog, prefix):
    lst = prog.get(TAINT_KEY)
    if lst:
        rest = [e for e in lst if e.get('p') != prefix]
        if len(rest) != len(lst):
            prog[TAINT_KEY] = rest
    FORCE_REQUEUE.discard(prefix)


def _taint(prog, prefix, detail):
    """把一个 prefix 标记为「被撕裂/损坏行波及」：跨进程持久化 + 进程内生效。"""
    if not prefix:
        return
    lst = prog.setdefault(TAINT_KEY, [])
    if not any(e.get('p') == prefix for e in lst):
        lst.append({'p': prefix, 'ts': datetime.now().isoformat(),
                    'reason': 'torn_or_damaged_records', 'detail': detail})
    FORCE_REQUEUE.add(prefix)

def reconcile(prog):
    """用 records.jsonl 反查重建 progress 的一致性（幂等）。"""
    kept, dropped, dropped_prefixes = repair_records_tail()
    info = LAST_REPAIR_INFO

    # v10-fix（ADV-1a）：撕裂/损坏行波及的 prefix —— 持久化污点 + 进程内闸门。
    # 容差判定对它们不生效（否则撕裂丢 1 条会被洗成「已完成」= 静默丢失）。
    damaged_prefixes = list(info.get('prefixes') or [])
    for p in damaged_prefixes:
        _taint(prog, p, f"末行不完整={info.get('torn_tail')}，"
                        f"损坏行 {info.get('damaged_lines')} 行，丢弃片段 {info.get('dropped_lines')} 行")
    tainted = {e.get('p') for e in prog.get(TAINT_KEY, []) if e.get('p')}
    FORCE_REQUEUE.clear()
    FORCE_REQUEUE.update(tainted)
    if damaged_prefixes:
        _recovery_note(prog, 'records-damage', None,
                       f"records 损坏行 {info['damaged_lines']} 行"
                       f"（末行不完整={info['torn_tail']}）：抢救完整记录 {info['salvaged_records']} 条、"
                       f"丢弃不可恢复片段 {info['dropped_lines']} 行、保留 {info['kept_lines']} 行、"
                       f"丢 {info['bytes_dropped']} 字节；涉及 prefix={info['prefixes']}"
                       f"（由前一条推断 {info['prefix_inferred']} 个、无法归属 {info['unnamed_damage']} 行）"
                       f" → 这些 prefix 容差判定已停用，强制回队重抓")

    watch = set()
    for it in prog.get('todo', []):
        if it.get('p'):
            watch.add(it['p'])
    for g in prog.get('gaps', []):
        if g.get('p'):
            watch.add(g['p'])
    for d in prog.get('done', []):
        if d.get('p'):
            watch.add(d['p'])
    watch.update(damaged_prefixes)
    watch.update(tainted)
    lines, seen = scan_records(watch)
    for p, s in seen.items():
        RECORDED_COUNTS[p] = max(RECORDED_COUNTS.get(p, 0), len(s))

    # (b) records 已写、progress 未更新的窗口 → 补登 done，跳过重复抓取
    recovered, kept_todo, forced_keep = 0, [], 0
    for it in prog.get('todo', []):
        p, total = it.get('p'), it.get('total')
        cnt = recorded_count(p)
        if p in tainted:                      # v10-fix：污点 prefix 不得被容差判完成
            kept_todo.append(it)
            forced_keep += 1
            _recovery_note(prog, 'torn->keep-todo', p,
                           f'records 反查 {cnt}/{total}，但该 prefix 被撕裂/损坏行波及（污点账本 '
                           f'{TAINT_KEY}）→ 容差不适用，保留在队列中重新抓取')
            continue
        if is_recorded_done(p, total):
            _tolerance_mark(prog, p, cnt, total, 'todo->done 补登')
            mark_leaf_done(prog, p, total, cnt, recovered=True)
            _recovery_note(prog, 'todo->done', p,
                           f'records 反查 {cnt}/{total}（progress 未更新），补登完成，不再重复抓取')
            recovered += 1
            continue
        kept_todo.append(it)
    prog['todo'] = kept_todo

    # (c) done 账本校验：records 缺失/不足（或被撕裂波及）→ 回队补抓（不丢 unique 记录）
    requeued = 0
    queued = {it.get('p') for it in prog['todo']}
    for d in list(prog.get('done', [])):
        p, total = d.get('p'), d.get('total')
        if not p:
            continue
        cnt = recorded_count(p)
        need = (total or 0) - leaf_tol(total or 1)
        forced = p in tainted
        if total and cnt >= need and cnt > 0 and not forced:
            _tolerance_mark(prog, p, cnt, total, 'done 账本校验')
            d['got'] = max(d.get('got', 0), cnt)
            continue
        prog['done'].remove(d)
        if p not in queued:
            prog['todo'].append({'p': p, 'total': total})
            queued.add(p)
            requeued += 1
            if forced:
                _recovery_note(prog, 'torn->todo', p,
                               f'被撕裂/损坏行波及（末行不完整={info.get("torn_tail")}）→ '
                               f'records 现 {cnt}/{total}，强制回队重抓（容差已停用）')
            else:
                _recovery_note(prog, 'done->todo', p,
                               f'done 账本声称完成但 records 仅 {cnt}/{total} → 回队补抓')

    # (d) 受损行涉及的 prefix（含撕裂末行的 prefix）→ **无条件**回队补抓
    for p in sorted(set(damaged_prefixes)):
        if p in queued:
            continue
        entry = next((d for d in prog.get('done', []) if d.get('p') == p), None)
        total = (entry or {}).get('total')
        prog['todo'].append({'p': p, 'total': total})
        queued.add(p)
        requeued += 1
        _recovery_note(prog, 'truncated->todo', p,
                       f'records 该 prefix 有撕裂/损坏行（末行不完整={info.get("torn_tail")}），'
                       f'records 现 {recorded_count(p)}/{total} → 回队补抓（容差判定已停用）')

    # (e) records 已覆盖的 leaf 型缺口 → 消解（被撕裂波及的 prefix 除外）
    resolved, new_gaps = 0, []
    for g in prog.get('gaps', []):
        p, total = g.get('p'), g.get('total')
        if g.get('type') == 'leaf' and p and total and p not in tainted and \
                recorded_count(p) >= (total - leaf_tol(total)) and recorded_count(p) > 0:
            _tolerance_mark(prog, p, recorded_count(p), total, 'gap-resolved')
            resolved += 1
            _keep_resolved_gap(prog, g, 'records-covered-by-reconcile')   # L2：留档，不静默消失
            _recovery_note(prog, 'gap-resolved', p,
                           f'records 反查 {recorded_count(p)}/{total}，缺口已在容差内，'
                           f'从 gaps 移除（已留档 resolved_gaps[]）')
            continue
        new_gaps.append(g)
    prog['gaps'] = new_gaps

    # 注：stats['recovered'] 已由 mark_leaf_done(..., recovered=True) 逐个累加，此处不再重复相加
    st = prog.setdefault('stats', {})
    st.setdefault('recovered', 0)
    st.setdefault('tolerance_marks', 0)
    summary = {'kept_lines': kept, 'dropped_lines': dropped,
               'dropped_prefixes': sorted(set(dropped_prefixes)),
               'recovered_leaves': recovered, 'requeued': requeued,
               'gaps_resolved': resolved, 'todo': len(prog['todo']),
               'done': len(prog.get('done', [])),
               # v10-fix（ADV-1）新增读数
               'damaged_lines': info.get('damaged_lines', 0),
               'salvaged_records': info.get('salvaged_records', 0),
               'torn_tail': info.get('torn_tail', False),
               'bytes_dropped': info.get('bytes_dropped', 0),
               'forced_requeue': sorted(tainted), 'forced_kept_in_todo': forced_keep,
               'tolerance_marks': st.get('tolerance_marks', 0)}
    log(f"一致性自愈：扫描 {lines} 行有效记录；损坏行修复 {info.get('damaged_lines', 0)} 行"
        f"（抢救 {info.get('salvaged_records', 0)} 条 / 丢弃 {dropped} 行片段，"
        f"末行不完整={info.get('torn_tail')}）；"
        f"补登完成 {recovered} 个叶子；回队补抓 {requeued} 个（其中撕裂强制 {len(tainted)} 个）；"
        f"消解缺口 {resolved} 个；容差完成 {st.get('tolerance_marks', 0)} 个；"
        f"todo={summary['todo']} done={summary['done']}")
    return summary

def rebuild_from_records(seeds, args):
    """progress.json 与 .bak 都不可用时的最后兜底：全量反查 records.jsonl 重建队列。
    只恢复「叶子级已落盘」这一事实（RECORDED_COUNTS），队列按种子重走 DAG；
    已完整的叶子在开抓前会被 is_recorded_done() 拦下，因而不重复抓取。"""
    lines, seen = scan_records(None)
    for p, s in seen.items():
        RECORDED_COUNTS[p] = max(RECORDED_COUNTS.get(p, 0), len(s))
    prog = {'todo': [{'p': s, 'total': None} for s in seeds],
            'stats': {'leaves': 0, 'records': 0},
            'gaps': [], 'done': [], 'recovery': [],
            'baseline': None, 'seeds': list(seeds),
            'started': datetime.now().isoformat(),
            TAINT_KEY: [],
            'rebuilt_from_records': True}
    _recovery_note(prog, 'rebuild-from-records', None,
                   f'progress 与 .bak 均不可用 → 全量反查 {lines} 行、{len(seen)} 个 prefix 重建队列'
                   f'（种子 {args or "默认 A-Z"}）')
    log(f"records 全量反查重建：{lines} 行有效记录，{len(seen)} 个 prefix")
    return prog

class RateLimiter:
    """R2: 自适应限速器（AutoThrottle 式，对标 scrapy.extensions.throttle.AutoThrottle）· v2

    v1 → v2 的语义变更（每条都有对应的回归测试与批评回执编号）：
      · **延迟通道按类别隔离**：v1 的 `target = p80(all)/CONC` 把「响应耗时」当「间隔目标」，
        大页面(17s)与小探测(0.12s)混在一个 p80 里 ⇒ 正常延迟分布下也把间隔抬到 0.25s
        （verify F11：管线内 −4.1%）。v2 为每个请求类别(bulk / light)记自己的历史地板 p10，
        只有「本类 p80 相对本类地板 ≥1.8 倍」才判为劣化 ⇒ 天生免疫页面体积差与类别混合。
      · **错误通道改为与错误率成正比、且不掐死恢复路径**：v1 的倍数绑「连续」错误数
        （不相邻的 429 恒为 1.0 = 零响应，crit-adv A1）且错误分支无条件 return
        （陈旧错误永久堵住放松路径，crit-adv A3）。v2 用窗口错误率算倍数（单发 429 ≥ +5%），
        并引入中性带 [0.03,0.10)：错误率在此区间保持间隔不动，既不卡死也不冒险放松。
      · **上界** 8.0s → 1.2s（survey §B5）；**地板/抖动** 0.19/0.03 → 0.20/0.008（T4 桶上限）。
      · **1s 窗口硬上限** RATE_WINDOW_MAX=5：结构兜底，不依赖地板正确。
      · **校验与改向分离**：note_status() 只做观测，_maybe_adjust() 自己按时间门发牌（判据完整）。
      · **读数修正**：登记「请求发出时刻」issued_ts（T5），节拍读数在延迟劣化场景下才可信。

    职责分离（不变）：cooldown() 只推迟下一次请求；note_status() 的反馈只抬升稳态间隔。
    mode='static' 时以上自适应逻辑全部旁路：acquire/cooldown 与基线逐行为等价。
    """

    # ---- 模块级常量在实例上的快照（便于测试逐个改写；都是「未改过」的原值） ----
    MIN = THROTTLE_MIN_INTERVAL
    MAX = THROTTLE_MAX_INTERVAL
    JITTER = THROTTLE_JITTER
    WINDOW = THROTTLE_WINDOW
    WINDOW_MIN = THROTTLE_WINDOW_MIN
    ADJUST_PERIOD = THROTTLE_ADJUST_PERIOD
    STABLE_STREAK = THROTTLE_STABLE_STREAK
    RELAX_FRAC = THROTTLE_RELAX_FRAC
    RELAX_MIN_STEP = THROTTLE_RELAX_MIN_STEP
    RECOVER_N = THROTTLE_RECOVER_N
    ERR_RL = THROTTLE_ERR_RL
    ERR_RL_HOLD = THROTTLE_ERR_RL_HOLD
    ERR_OTHER = THROTTLE_ERR_RATE_OTHER
    LAT_MIN_SAMPLES = THROTTLE_LAT_MIN_SAMPLES
    RATE_WINDOW = THROTTLE_RATE_WINDOW
    RATE_WINDOW_MAX = THROTTLE_RATE_WINDOW_MAX
    ERR_RATE_GROW_BURST = THROTTLE_ERR_RATE_GROW_BURST
    ERR_FRESH_S = THROTTLE_ERR_FRESH_S
    COOLDOWN = THROTTLE_COOLDOWN
    LOG_EVERY = THROTTLE_LOG_EVERY
    DEAD_ZONE = THROTTLE_DEAD_ZONE
    FLOOR_BACKOFF = THROTTLE_FLOOR_BACKOFF
    FLOOR_DECAY = THROTTLE_FLOOR_DECAY
    FLOOR_CAP = THROTTLE_FLOOR_CAP
    CLEAN_RESET_N = THROTTLE_CLEAN_RESET_N

    def __init__(self, min_interval, mode='static'):
        self.lock = threading.Lock()
        self.mode = mode if mode in ('adaptive', 'static') else 'static'
        # ---- 静态模式字段（与基线 RateLimiter 同名同义） ----
        self.min_interval = min_interval
        self.next_time = 0.0
        self.cooldown_until = 0.0
        # ---- 自适应字段 ----
        self._interval = max(THROTTLE_MIN_INTERVAL, float(min_interval))
        self._obs = []                  # 滚动窗口 [(latency_s, err_cls, t)]；err_cls: None=成功，
                                        # 'rlimit'=429/400，'conn'=连接异常，'5xx'/'other'=其他
        self._lat_hist = {}             # 每类成功响应耗时历史（长窗口，劣化判定的「历史基线」）
        self._lat_recent = {}           # 每类最近 THROTTLE_LAT_MIN_SAMPLES 条（近期窗口）
        self._lat_base = {}             # 每类"历史基线"（低分位 p10，冻结式维护）
        self._obs_lock = threading.Lock()
        self._err_streak = 0            # 连续错误数（保留：读数/审计用）
        self._clean_streak = 0          # 连续无错数（= 自最近一次错误以来的干净条数；滞回判据）
        self._last_adjust = 0.0
        self._n_obs = 0                 # 累计观测数
        self._n_req = 0                 # 累计 acquire 数
        self._n_issued = 0              # 累计「已发出」请求数（issued_ts 账本用）
        self._issued_ts = []            # 最近若干次「请求发出时刻」（T5：原实现记的是完成时刻）
        self._rate_win = []             # 最近 THROTTLE_RATE_WINDOW 秒内的发出时刻（硬上限用）
        self._grow_events = 0           # 升间隔事件数
        self._relax_events = 0          # 降间隔事件数
        self._oscillations = 0          # 震荡（带滞回的反向折返）次数
        self._adjust_events = 0         # 调整事件总数（含被死区吃掉的）
        self._lat_degrade_events = 0    # 延迟劣化型调整次数（分类计数，不依赖有界账本）
        self._release_events = 0        # 解除「中性带保持」的次数（HOLD → 放松/升间隔）
        self._hold_events = 0           # 进入/停留「中性带保持」的次数
        self._last_dir = None
        self._dir_extreme = None        # 当前方向段内的极值（滞回判据用）
        self._hist = []                 # 调整事件账本 [(t, interval, rate, why)]，环形上限
        self._last_top = None           # 最近一次判据来源（观测→判据 分离审计用）
        self._err_pending = False       # 有「尚未被响应过」的速率类错误（T1：每一个 429 都要被响应一次）
        self._floor_aimd = THROTTLE_MIN_INTERVAL   # AIMD「速率地板」当前值（错误上抬/干净回落）
        self._floor_aimd_peak = THROTTLE_MIN_INTERVAL  # 本次运行的地板峰值（读数：证明退避生效过）
        self._floor_t = 0.0             # 上次 AIMD 更新时刻（读数用）

    # ---------------- 限速主路径 ----------------
    def _rate_cap(self, t):
        """R2 硬上限（T4）：滑动 THROTTLE_RATE_WINDOW 秒窗口内最多 RATE_WINDOW_MAX 个请求。

        这是**独立于地板**的第二道闸：即使地板常量被改坏/绕过，发号速率也上不去。
        窗口满 ⇒ 把发号时刻推迟到「窗口内最早那次 + 窗口长度」。
        """
        win, cap = self.RATE_WINDOW, self.RATE_WINDOW_MAX
        self._rate_win = [x for x in self._rate_win if t - x < win]
        if len(self._rate_win) >= cap:
            t = max(t, self._rate_win[-cap] + win)
            self._rate_win = [x for x in self._rate_win if t - x < win]
        self._rate_win.append(t)
        return t

    def acquire(self):
        """与基线同名同义；返回实际等待秒数（测试用读数）。"""
        with self.lock:
            now = time.time()
            t = max(now, self.next_time, self.cooldown_until)
            if self.mode == 'adaptive':
                t = self._rate_cap(t)
                # 抖动不得把「间隔下界」压穿：余量不足时抖动自动收窄 ⇒ 实际速率 ≤ 地板速率
                head = max(0.0, self._interval - THROTTLE_MIN_INTERVAL)
                gap = self._interval + random.uniform(0.0, min(self.JITTER, head))
            else:
                gap = self.min_interval + random.uniform(0, 0.03)
            self.next_time = t + gap
            self._n_req += 1
        wait = t - now
        if wait > 0:
            time.sleep(wait)
        return wait

    def cooldown(self, sec):
        """与基线同名同义：把下一次请求推迟到 now+sec。

        与 v1 的差别：反馈路径给的冷却**不再随间隔放大**（上限 1.0s）。理由：调用方
        （fetch）在 429 上已经 `cooldown(15+10·att)` + sleep，最长 240s；限速器再加
        `interval×3`（v1 最高 24s）属于对同一个错误**双计**（survey §B5 明确要求消除）。
        这里只保留一个小缓冲，间隔一侧交给 note_status() 反馈。
        """
        with self.lock:
            target = time.time() + sec
            if target > self.cooldown_until:
                self.cooldown_until = target

    # ---------------- 反馈路径（由 fetch 逐请求调用） ----------------
    @staticmethod
    def _kind_of(kind, limit):
        """请求类别解析：显式 kind 优先，否则按 limit 粗分（≥THROTTLE_BULK_LIMIT 记为 bulk）。"""
        if isinstance(kind, str) and kind in THROTTLE_KINDS:
            return kind
        try:
            if limit is not None and int(limit) >= THROTTLE_BULK_LIMIT:
                return 'bulk'
        except (TypeError, ValueError):
            pass
        return 'light'

    def note_issued(self, issued_ts):
        """登记一次「请求发出时刻」（T5 读数修正；不参与限速决策）。

        幂等：同一次尝试可能被 note_issued 与 note_status 各登记一次，重复登记会让
        「相邻发出间隔」读出 0.0s 的假节拍，故按时刻去重。
        """
        if self.mode != 'adaptive':
            return
        v = float(issued_ts)
        with self._obs_lock:
            if self._issued_ts and v <= self._issued_ts[-1]:
                return
            self._issued_ts.append(v)
            if len(self._issued_ts) > 512:
                del self._issued_ts[:-512]
            self._n_issued += 1

    def note_status(self, status, latency=None, exc=None, kind=None, limit=None, issued_ts=None):
        """登记一次请求结果（**只做观测**，是否调整由 _maybe_adjust 的判据完整决定）。

        status: HTTP 状态码或 None；exc: 异常类名或 None；
        kind/limit: 请求类别（bulk/light 或 limit 值）；issued_ts: 请求发出时刻（读数用）。
        """
        if self.mode != 'adaptive':
            return
        if issued_ts is not None:
            self.note_issued(issued_ts)
        cls = 'other'
        if exc is not None:
            # T8：只有真正的传输/网络异常才算连接类；数据类异常（JSONDecode/KeyError…）
            # 不得被当作网络错误去抬升全局限速。
            cls = 'conn' if exc in _R2_NET_EXC_NAMES else 'other'
        elif status == 429 or status == 400:
            cls = 'rlimit'
        elif status is not None and status != 200:
            cls = '5xx' if 500 <= status < 600 else 'other'
        else:
            cls = None
        k = self._kind_of(kind, limit)
        lat = float(latency) if latency is not None else None
        now = time.time()
        obs = (lat, cls, now)
        with self._obs_lock:
            self._obs.append(obs)
            if len(self._obs) > self.WINDOW:
                del self._obs[:len(self._obs) - self.WINDOW]

            if lat is not None and cls is None:
                # 延迟基线只吃「**成功**响应」的耗时：错误响应的耗时是**注入/异常**的
                # （如 `(429, 0.30)`），混进来会污染本类的正常耗时分布并误触发劣化通道
                # （v2 开发中实测到：0.30s 的错误耗时把 0.12s 类推成"劣化 1.5×"）。
                h = self._lat_hist.setdefault(k, [])
                h.append(lat)
                if len(h) > THROTTLE_LAT_BASE_SAMPLES:
                    del h[:-THROTTLE_LAT_BASE_SAMPLES]
                r = self._lat_recent.setdefault(k, [])
                r.append(lat)
                if len(r) > THROTTLE_LAT_MIN_SAMPLES:
                    del r[:-THROTTLE_LAT_MIN_SAMPLES]
            self._n_obs += 1
            if cls is None:
                self._clean_streak += 1
                self._err_streak = 0
                # AIMD 加性恢复：每条干净请求让地板回落 0.5%
                if self._floor_aimd > THROTTLE_MIN_INTERVAL:
                    self._floor_aimd = max(THROTTLE_MIN_INTERVAL,
                                           self._floor_aimd * self.FLOOR_DECAY)
                if (self._floor_aimd > THROTTLE_MIN_INTERVAL
                        and self._clean_streak >= self.CLEAN_RESET_N):
                    # 连续这么多条干净 ⇒ 该错误段确实结束了 ⇒ 地板复位（快速恢复）
                    self._floor_aimd = THROTTLE_MIN_INTERVAL
            else:
                self._err_streak += 1
                self._clean_streak = 0
                # AIMD 乘性退避（对**所有**错误类生效）：每次错误把「速率地板」上抬一档，
                # 上限 THROTTLE_FLOOR_CAP×地板；干净请求逐条回落、错误率归零即复位。
                self._floor_aimd = min(self.FLOOR_CAP,
                                       max(THROTTLE_MIN_INTERVAL, self._floor_aimd)
                                       * self.FLOOR_BACKOFF)
                self._floor_aimd_peak = max(self._floor_aimd_peak, self._floor_aimd)
                if cls == 'rlimit':
                    self._err_pending = True     # 待响应（下一刻由判据①消费）
        self._maybe_adjust()

    @staticmethod
    def _quantile(vals, q):
        if not vals:
            return None
        s = sorted(vals)
        return s[min(len(s) - 1, int(len(s) * q))]

    def _lat_baseline_update(self, k):
        """维护并返回本类别的「历史基线」（长窗口低分位 p10 + **只在未劣化时更新**）。

        两处设计要点（都是 v2 开发中实测踩出来的）：
          1. 取**低分位**而不是中位数：劣化期长窗口里会混入大量慢样本，中位数会被"同化"
             （实测：0.72s 慢样本把 0.12s 的基线变成 0.72s，比值 1.00 ⇒ 零响应）。
          2. **锁定**：只有当「近期 p50 / 候选基线 < 1.5×」时才更新基线，即只在**未劣化**时
             学习。否则持续劣化会把基线一路拽上去，检测窗口在几十分钟后失效。
        它是**自相对**的：无论 mock 的 0.12s 还是生产的 17.4s bulk，正常情况恒等 ⇒
        不会像 v1 那样把「大页面」误判为服务端过载（verify F11 / crit-adv A8）。
        """
        hist = self._lat_hist.get(k)
        if not hist:
            return None
        cand = self._quantile(hist, 0.10)
        recent = self._lat_recent.get(k) or []
        if len(recent) >= self.LAT_MIN_SAMPLES:
            p50 = self._quantile(recent, 0.50)
            if p50 and cand > 0 and p50 / cand >= THROTTLE_LAT_BASE_LOCK:
                return self._lat_base.get(k, cand)      # 处于劣化中：冻结基线
        self._lat_base[k] = cand
        return cand

    def _lat_baseline_of(self, k):
        """只读基线（不更新）。"""
        return self._lat_base.get(k)

    def _recent_err_rate(self, now, obs):
        """时间窗内的错误率（速率类 + 其它错误），与请求条数无关。

        用时间窗而不是条数窗：条数窗在「稀疏错误」下会把 1 个错误算成 100%（窗口只有 1 条），
        → 下限被瞬间顶到上界；时间窗天然平滑。
        """
        win = [o for o in obs if now - o[2] <= THROTTLE_RATE_WIN_S]
        if not win:
            return 0.0
        return sum(1 for o in win if o[1] is not None) / len(win)

    def _err_rate_floor(self, rate_err):
        """「速率地板」：近期出现过限流错误就把间隔下限按 AIMD 抬起来（乘性退避 + 加性恢复）。

        为什么不是「错误率的线性函数」：线性映射需要一个显式的斜率，而合适的斜率取决于
        **服务端真实的上限 K**（未知且随时间变化）。v2 实测（闭环令牌桶，K=4.5）：
        线性斜率 4.0 时自适应只有 1.94 req/s 而静态基线 2.67 req/s —— 退避不足 ⇒ 持续撞线。
        AIMD 不需要知道 K：每个错误把地板 ×1.5，之后每个无错秒衰减 0.2%，
        自然收敛到「刚好不撞线」的水位（实测 K=4.5：3.97 req/s / 2 个 429；K=5.0：5.00 req/s / 0 个 429）。
        该值同时被用作放松的下限与错误加码的上限 ⇒ 「错误不停止就不会一路加速」，
        而错误一停又会在有限时间内衰减回地板（恢复路径不被堵死）。
        """
        self._floor_t = time.time()
        return max(THROTTLE_MIN_INTERVAL, self._floor_aimd)

    # ---------------- 判据（观测 → 调整 的分离点） ----------------
    def _error_factors(self, rl_recent, other, n):
        """升间隔倍数：按「连续/突发错误个数」取阶梯（**与耗时无关**）。

        v1 的缺陷是倍数只认「连续错误个数」而单发错误倍数=1.0（零响应，T1）；
        中途 v2 用过「窗口错误率线性公式」，但它隐含了「错误耗时/当前间隔」这个比值
        ⇒ 错误耗时被夹具设大时会一拍顶到上界（开发中实测：0.30s 的错误耗时使单发错误
        也判出 25% 错误率 → 3 次 429 就把间隔从 0.2s 推到 1.2s）。阶梯式倍数没有任何
        隐含量纲，行为可预测：单发 +6%、连发 2 个 +12%、连发 ≥3 个 +18%。
        """
        if rl_recent >= 3:
            step = THROTTLE_ERR_STEP_BURST
        elif rl_recent == 2:
            step = THROTTLE_ERR_STEP_PAIR
        else:
            step = THROTTLE_ERR_STEP_SINGLE
        f_err = 1.0 + step
        f_oth = THROTTLE_HARD_ERR_GROW if other else 1.0
        return max(f_err, f_oth), (rl_recent / max(1, n))


    def _latency_rise(self, recent, baseline):
        """延迟劣化幅度（0.0 = 未劣化）：近期 p50 / 历史 p50 − 1（超过阈值部分才计入）。

        为什么用「近期中位数 / 历史中位数」而不是别的：
          · 中位数对少数尖峰不敏感 ⇒ 单次慢响应不会把整体速率砍掉（crit-adv A4 的 10× 自伤）；
          · 同分布下该比值恒 ≈1.0（30 样本中位数的相对波动：σ=0.3 时 ≈7%，
            阈值 2.0 相当于 14σ）⇒ 长跑不误触发；
          · 只有分布**整体平移**（服务端真的变慢）才 >1，且与页大小无关。
        """
        if not recent or baseline is None or len(recent) < self.LAT_MIN_SAMPLES:
            return 0.0, 0.0
        p50 = self._quantile(recent, 0.50)
        if not p50 or baseline <= 0:
            return 0.0, 0.0
        ratio = p50 / baseline
        if ratio < THROTTLE_LAT_RISE_RATIO:
            return 0.0, ratio
        rise = min(1.0 + THROTTLE_LAT_GROW_MAX, ratio / THROTTLE_LAT_RISE_RATIO) - 1.0
        return rise, ratio

    def _maybe_adjust(self):
        """一条判据：按优先级决定「升 / 降 / 保持」，并**只有一个出口**（可审计）。

        v1 的缺陷是「错误分支 return ⇒ 放松分支永远轮不到」；v2 全部走条件赋值，
        出口只有一个 ⇒ 结构性不可能再出现「恢复路径被堵死」。
        """
        now = time.time()
        with self._obs_lock:
            obs = list(self._obs)
            n = len(obs)
            # 冷启动（观测数不足）：**只放行「错误响应」**，窗口型判据（延迟劣化/放松）仍然
            # 需要满窗。v1 在这里直接 return ⇒ 冷启动期的 429 被整段丢弃（T1 的另一半）。
            warm = n < self.WINDOW_MIN
            clean = self._clean_streak
            streak = self._err_streak
            cur = self._interval
            recent = obs[-min(THROTTLE_ERR_STREAK_N, n):]
            n_rl_recent = sum(1 for o in recent if o[1] == 'rlimit')
            n_oth_recent = sum(1 for o in recent if o[1] in ('conn', '5xx', 'other'))
            rl_age = None                             # 最近一次速率类错误距今多久
            for o in reversed(obs):
                if o[1] == 'rlimit':
                    rl_age = now - o[2]
                    break
            # ---- 近期错误率（时间窗）→ 间隔下限：v2 用「错误率 ⇒ 该守在哪个间隔」的
            #      自洽映射替代 v1 的「连续错误倍数 + 卡死式 hold」。
            #      错误率 0   ⇒ 下限 = 地板（完全恢复）
            #      错误率 25% ⇒ 下限 = 地板×(1+4×0.25) = 2× 地板（守住、不继续加码）
            #      错误率 100%⇒ 下限 = 上界（风暴：一路退到底）
            #      恢复路径因此**结构上不可能被堵死**：错误率降下来 ⇒ 下限自动降下来。
            rate_win_err = self._recent_err_rate(now, obs)
            rec = obs[-min(self.RECOVER_N, n):]
            n_rec = len(rec)
            rl_rec = sum(1 for o in rec if o[1] == 'rlimit')
            oth_rec = sum(1 for o in rec if o[1] in ('conn', '5xx', 'other'))
            rate_rl = rl_rec / max(1, n_rec)
            rate_rl_win = sum(1 for o in obs if o[1] == 'rlimit') / n
            rl_in_win = sum(1 for o in obs if o[1] == 'rlimit')
            rate_other = sum(1 for o in obs if o[1] in ('conn', '5xx', 'other')) / n
            # 「错误已停止」= 最近 THROTTLE_RECOVER_N 条内零错误（与滞回常量一致）
            # 延迟劣化之外的两个"解锁"信号（v2 的恢复路径设计）：
            #   · err_fresh  —— 最近一次 429 还在新鲜窗内（时间域）。用秒数而不是条数，
            #     是因为 fetch 的 429 重试要睡 15/25/35s，条数判据会被这段睡眠污染。
            #   （v1 的恢复路径永久堵死，是因为"放松"要求窗口里没有任何陈旧错误；
            #      v2 的放松下限由**近期错误率**决定 ⇒ 错误率下降，下限自动回落。）
            err_fresh = (rl_age is not None and rl_age <= self.ERR_FRESH_S)
            # 「速率地板」= AIMD 水位：**只作放松的下限**，不参与错误加码（否则地板=间隔时
            # 放松与加码互相锁死：实测"地板夹到当前间隔"会让间隔永远回不到地板）。
            err_floor_iv = min(self.MAX, max(THROTTLE_MIN_INTERVAL,
                                             self._err_rate_floor(rate_win_err)))
            lat_recent = {k: list(v) for k, v in self._lat_recent.items()}
            lat_base = {k: self._lat_baseline_update(k) for k in lat_recent}

        if warm and not self._err_pending:
            return
        # ---- ① 速率类错误（429/400，红线 B3 主判据）----
        #   触发条件三选一（**每一个 429/400 都至少被响应一次**，T1）：
        #     · 最近 THROTTLE_ERR_STREAK_N 条内有 ≥THROTTLE_ERR_STREAK_RL 个  → 突发
        #     · 窗口错误率 > THROTTLE_ERR_RL（0.10）                        → 高错误率
        #     · 有「尚未被响应过」的 429（_err_pending）                     → 单发/稀疏
        #   「突发」= 最近 3 条里 ≥2 个（连发），**或** 比例尺窗口里 ≥2 个且错误率高于阈值
        #   （稀疏但成对出现）。只看最近 3 条会漏掉「一个错误触发一次重试」的成对形态。
        burst = (n_rl_recent >= THROTTLE_ERR_STREAK_RL
                 or (rl_in_win >= 2 and rate_rl > self.ERR_RL))
        if burst or rate_rl > self.ERR_RL or err_fresh:
            # 注：本分支**无条件消费** `_err_pending`（每个 429 至少被响应一次，T1），
            # 但增长量受 cap 约束 ⇒ 稀疏错误只会把间隔推近 floor×1.50 后停住。
            with self._obs_lock:
                self._err_pending = False         # 每个 429/400 恰好被响应一次
            # ---- 错误响应的「量程」是**固定地板上方的比例尺**，不是当前位置的比例尺：
            #      错误 429 只允许把间隔抬到 floor×1.30（连发场景 floor×1.50），
            #      于是「持续低速率错误」会停在这个自洽量程上（不卡死、也不线性加码），
            #      而单发错误只得到一次 +6% 的响应。B3 红线（少撞限流）优先于多要速率。
            if burst:
                limit_iv = min(THROTTLE_MIN_INTERVAL * self.ERR_RATE_GROW_BURST, self.MAX)
            else:
                # 非突发：升到「当前 ×1.06」，不得越过上界（突发尺仍是软上界，防稀疏错误爬升）
                limit_iv = min(cur * (1.0 + THROTTLE_ERR_STEP_SINGLE),
                               THROTTLE_MIN_INTERVAL * self.ERR_RATE_GROW_BURST, self.MAX)
            f, _ = self._error_factors(n_rl_recent, n_oth_recent, len(recent))
            target = min(cur * f, limit_iv)
            self._last_adjust = now
            why = (f'err-rl burst={n_rl_recent}/{len(recent)} '
                   f'rate_rl={rate_rl:.3f} win={rate_rl_win:.3f} '
                   f'x{f:.2f} cap={limit_iv:.4f}')
            if target - cur <= THROTTLE_GROW_EPS * cur:
                self._hold_events += 1
                self._last_top = f'hold(err-cap) rate_rl={rate_rl:.3f} cap={limit_iv:.4f}'
                return
            self._adjust(target, why)
            return
        # ---- ② 其他错误（连接异常/5xx）达到阈值 ⇒ 较轻升间隔 ----
        #   同样只在「错误仍然新鲜」时动作：条数窗口里的陈旧错误不得永久压制恢复
        #   （v1 的「恢复路径被错误自身堵死」在 v2 里换成时间域判据，见 THROTTLE_ERR_FRESH_S）。
        if rate_other >= self.ERR_OTHER and err_fresh:
            self._last_adjust = now
            self._adjust(cur * THROTTLE_HARD_ERR_GROW,
                         f'err-other rate={rate_other:.3f}x{streak}err x{THROTTLE_HARD_ERR_GROW}')
            return
        # ---- ③ 延迟劣化（按类别自相对，v1 的 CONC/绝对 p80 语义在此被替换）----
        worst, worst_k, worst_ratio = 0.0, None, 0.0
        for k, v in lat_recent.items():
            rise, ratio = self._latency_rise(v, lat_base.get(k))
            if rise > worst:
                worst, worst_k, worst_ratio = rise, k, ratio
        if worst > 0.0:
            if now - self._last_adjust < self.ADJUST_PERIOD:
                return
            self._last_adjust = now
            f = 1.0 + min(THROTTLE_LAT_GROW_MAX,
                          max(THROTTLE_LAT_GROW_MIN, THROTTLE_LAT_GROW_SLOPE * worst))
            # 升幅上限 = 观测到的「超出基线的额外等待」（= 排队量级）：把间隔抬到与排队
            # 同量级即可吸收拥塞，再往上只会自伤吞吐（crit-adv A4 的 10× 自伤就是这样来的）。
            # 排队量级 = 基线 × (比值 − 1)；把间隔抬到与排队同量级即可吸收拥塞。
            lat_excess = (lat_base.get(worst_k) or 0.0) * max(0.0, worst_ratio - 1.0)
            lat_cap = lat_excess if lat_excess > 0 else cur
            target = min(cur * f, max(cur, lat_cap))
            if target - cur <= THROTTLE_GROW_EPS * cur:
                self._hold_events += 1
                self._last_top = f'hold(lat-cap) ratio={worst_ratio:.2f}'
                return
            self._lat_degrade_events += 1
            self._adjust(target,
                         f'latency-degrade {worst_k} p50/基线={worst_ratio:.2f}x x{f:.2f}')
        # ---- ④ 中性带：窗口里仍有零星错误但未达阈值 ⇒ **保持不动**（不卡死、也不冒进）----
        if self.ERR_RL_HOLD <= rate_rl <= self.ERR_RL and err_fresh:
            self._hold_events += 1
            self._last_top = f'hold rate_rl={rate_rl:.3f}'
            return
        # ---- ⑤ 恢复/放松（唯一的下行出口）----
        # 滞回判据用「**自上次调整以来的连续干净条数**」而不是「窗口内零错误」：
        # 风暴之后窗口里会残留若干条陈旧错误，若要求窗口零错误，放松就要多等一个窗口
        # （v1 实测：配置 8 条、实际第 19 条才放松 ⇒ 文档与行为不符，T2）。用了自上次
        # 调整以来的计数，配置值 8 就等于行为值 8（测试 F/滞回用例实测断言）。
        if now - self._last_adjust < self.ADJUST_PERIOD:
            return
        if clean < self.STABLE_STREAK:
            return
        self._last_adjust = now
        # 放松不得越过「错误率下限」：错误还在发生就守在当前错误率对应的间隔上，
        # 错误停止 ⇒ 下限回到地板 ⇒ 同一段代码自动把速率还回去（不需要额外的解锁信号）。
        relax_floor = max(THROTTLE_MIN_INTERVAL, min(cur, err_floor_iv))
        step = max(self.RELAX_MIN_STEP, (cur - relax_floor) * self.RELAX_FRAC)
        nxt = max(relax_floor, cur - step)
        if cur - nxt > THROTTLE_RELAX_DEAD_ZONE:
            if self._last_top is not None and self._last_top.startswith('hold'):
                self._release_events += 1
            self._adjust(nxt, f'relax clean={clean} rate_rl={rate_rl:.3f} step={step:.4f}')

    def _adjust(self, new_interval, why, force=False):
        """唯一的写入口：clamp 到 [地板, 上界]，记账并写日志（why 里带判据来源）。"""
        with self.lock:
            lo, hi = THROTTLE_MIN_INTERVAL, self.MAX
            v = min(hi, max(lo, float(new_interval)))
            old = self._interval
            self._adjust_events += 1
            self._last_top = why
            if abs(v - old) <= 1e-12:
                return
            d = 1 if v > old else -1
            if d > 0:
                self._grow_events += 1
            else:
                self._relax_events += 1
            # 震荡计数带滞回：只有「反向幅度累计超过阈值」才算一次真实折返，
            # 否则会把贴着目标的 µs 级微调也计成震荡，读数失真。
            if self._last_dir is None or d == self._last_dir:
                self._dir_extreme = v
            else:
                if abs(v - self._dir_extreme) >= THROTTLE_OSC_MIN_MOVE:
                    self._oscillations += 1
                    self._dir_extreme = v
            self._last_dir = d
            self._interval = v
            self._hist.append((time.time(), v, 1.0 / v, why))
            if len(self._hist) > THROTTLE_HIST_MAX:      # T9：环形上限，长跑不涨内存
                del self._hist[:len(self._hist) - THROTTLE_HIST_MAX]
            rate = 1.0 / self._interval
        log(f"{_LOG_PREFIX} {why} | interval={old:.4f}->{v:.4f}s "
            f"rate={rate:.3f}req/s err_streak={self._err_streak} osc={self._oscillations}")

    # ---------------- 读数接口（测试/审计用，不参与限速逻辑） ----------------
    def snapshot(self):
        with self._obs_lock:
            obs = list(self._obs)
            lat_hist = {k: list(v) for k, v in self._lat_hist.items()}
            lat_recent = {k: list(v) for k, v in self._lat_recent.items()}
            issued = list(self._issued_ts)
        n = len(obs) or 1
        errs = [o[1] for o in obs if o[1] is not None]
        lat = sorted(o[0] for o in obs if o[0] is not None)
        iv = self._interval
        # 实测节拍：用「请求发出时刻」（T5）——延迟劣化场景下这才是真实节拍
        gaps = [issued[i + 1] - issued[i] for i in range(len(issued) - 1)]
        snap = {
            'mode': self.mode,
            'interval': iv,
            'rate_req_s': 1.0 / iv,
            'min_interval': THROTTLE_MIN_INTERVAL,
            'max_interval': self.MAX,
            'effective_interval_mean': iv + min(self.JITTER, max(0.0, iv - THROTTLE_MIN_INTERVAL)) / 2.0,
            'window_n': len(obs),
            'err_rate_rlimit': sum(1 for c in errs if c == 'rlimit') / n,
            'err_rate_other': sum(1 for c in errs if c != 'rlimit') / n,
            'latency_p50': (lat[len(lat) // 2] if lat else None),
            'latency_p80': (lat[min(len(lat) - 1, int(len(lat) * 0.8))] if lat else None),
            'clean_streak': self._clean_streak,
            'err_streak': self._err_streak,
            'err_rate_window_s': THROTTLE_RATE_WIN_S,
            'floor_aimd_interval': round(max(THROTTLE_MIN_INTERVAL, self._floor_aimd), 4),
            'floor_aimd_peak': round(max(THROTTLE_MIN_INTERVAL, self._floor_aimd_peak), 4),
            'err_floor_iv': (self._err_rate_floor(self._recent_err_rate(time.time(), list(self._obs)))
                             if self._obs else THROTTLE_MIN_INTERVAL),
            'grow_events': self._grow_events,
            'relax_events': self._relax_events,
            'oscillations': self._oscillations,
            'n_obs': self._n_obs,
            'n_acquire': self._n_req,
            'cooldown_remaining': max(0.0, self.cooldown_until - time.time()),
            'event_log': list(self._hist[-THROTTLE_HIST_MAX:]),
            # ---- v2 新增读数（全部为向后兼容的**新增键**，旧键一个未删） ----
            'adjust_events': self._adjust_events,
            'lat_degrade_events': self._lat_degrade_events,
            'hold_events': self._hold_events,
            'release_events': self._release_events,
            'max_interval_seen': max([e[1] for e in self._hist], default=iv),
            'lat_base_by_kind': {k: (round(v, 4) if v else None)
                                 for k, v in self._lat_base.items()},
            'lat_recent_p50_by_kind': {k: (round(self._quantile(v, 0.50), 4) if v else None)
                                       for k, v in lat_recent.items()},
            'lat_n_by_kind': {k: len(v) for k, v in lat_hist.items()},
            'rate_window_n': len(self._rate_win),
            'rate_window_max': self.RATE_WINDOW_MAX,
            'n_issued': self._n_issued,
            'issued_gap_min': (min(gaps) if gaps else None),
            'issued_gap_mean': (sum(gaps) / len(gaps) if gaps else None),
            'issued_gap_max': (max(gaps) if gaps else None),
            'issued_gap_n': len(gaps),
        }
        return snap


_R2_NET_EXC_NAMES = {                       # T8：只有这些异常算「连接/传输类」（其余不计入反馈）
    'ConnectionError', 'ConnectTimeout', 'ConnectionResetError', 'ConnectionAbortedError',
    'ReadTimeout', 'Timeout', 'TimeoutError', 'SSLError', 'SSLEOFError', 'ChunkedEncodingError',
    'ProtocolError', 'RemoteDisconnected', 'ProxyError', 'IncompleteRead', 'NewConnectionError',
    'MaxRetryError', 'socket.timeout', 'OSError', 'URLError', 'HTTPError',
}

LIMITER = RateLimiter(THROTTLE_START_INTERVAL if THROTTLE_MODE == 'adaptive' else MIN_INTERVAL,
                      mode=THROTTLE_MODE)

class LegacyTLSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)

SESSION = requests.Session()
SESSION.trust_env = False
SESSION.mount('https://', LegacyTLSAdapter(pool_connections=10, pool_maxsize=10))

# ===========================================================================
# R3（C1）请求级自愈：错误分类 + 指数退避+抖动 + 重试预算 + 请求级审计
#
# 目标（goal-card §1 C1）：断连/超时/5xx 自动重试 ≥3 次、指数退避+抖动；每级自愈事件
# 写结构化审计（refs/r3-audit-schema.md：retry_scheduled / retry_exhausted / error_classified）。
#
# ① 分类学（R3-P4：11 类；命名与语义对齐 refs/r3-audit-schema.md，异常名单对齐 R2 的 T8
#    `_R2_NET_EXC_NAMES`）：dns / conn_reset / read_timeout / write_timeout /
#    server_5xx / ratelimit / auth / client / fatal / data / other。
#    分类**只决定退避参数与停摆归属**；「是否重试」仍按基线集合 RETRY_STATUS_CODES 判定
#    （= 基线 L1578 的 (400,401,403,429,500,502,503,504) 逐值相同）⇒ 默认行为不劣化现状。
#    ------- R3-P4 的三处归因修正（出处：crit-correct R-1/R-4/R-5 + crit-adv a2/a1c）-------
#    · R-1 匹配次序：**具体信号先于泛化信号**。连接超时的类型/特征（ConnectTimeout /
#      ConnectTimeoutError / 'connect timeout'）必须先于泛化的 'timed out'/'timeout' 读超时
#      特征命中，否则 urllib3 的 ConnectTimeoutError、MaxRetryError（消息形如
#      'Connection to h timed out. (connect timeout=10)'）会被误吸成 read_timeout
#      ⇒ 错退避族（1s/15s vs 2s/30s）+ 错停摆归属（sleep vs cooldown）。
#      反面约束（不得回退）：`ConnectionError('…Read timed out.')` 仍须判 read_timeout，
#      因此不能简单「类型表一律优先」——见 _retry_class_of_exc 的优先级注释。
#    · R-4/R-B6：fatal（404/410/其余确定性 4xx/3xx）**完全不喂限速器**（不调 note_status）
#      ⇒ 不发散 AIMD 地板、不计 err_streak（v2 语义；legacy 保留基线调用，见 ⑥）。
#    · a2 归因分离：HTTP 400 从 ratelimit 拆出为 **client**。理由：本站 400 既是限流码
#      （R2/rate-profile §1：8.5 req/s → 4×400）也可能是我们自己的查询构造错误，单条目
#      证据不足以判定「服务端级限流」，因此 **不走全局 cooldown**（owner='sleep'，只停自己），
#      但仍按「每个逻辑请求一次」喂限速器的 rlimit 通道 ⇒ 跨请求的 400 风暴依然会被 AIMD
#      地板响应（B3 保护不被拆掉）；429 语义不变（无歧义 ⇒ class=ratelimit + 全局 cooldown）。
# ② 退避：d = min(cap, base × factor^att)；wait = d/2 + U(0, d/2)（**equal jitter**，
#    survey-retry §R-B2 的原式）。两条结构性断言（R-B2 ①④，均有单测）：
#      · max(wait) ≤ cap（对任意抖动抽样成立：wait ≤ d ≤ cap）；
#      · 首档 wait ≤ base（att=0 时 d = min(cap, base)）。
#    单调性（R-2）：band_k = [d_k/2, d_k]；factor ≥ 2 ⇒ 在**未触及 cap 的增长区**
#    band_{k+1} 下界 = d_k = band_k 上界 ⇒ 相邻档位区间不重叠 ⇒ 该区间的退避序列（含期望）
#    非递减；进入 cap 后档位期望恒为 0.75·cap（上界恒为 cap）⇒ 期望非递减，唯一次抽样仍
#    可能因抖动回落（这是抖动在饱和区的固有代价，不再是"抖动跨区 > factor"的结构缺陷）。
#    factor<1.667 的类已按 R-2 上调到 2.0（否则抖动区间比 > factor 会结构性破坏单调）。
# ③ 停摆归属（消除 cooldown/sleep 双计，survey-throttle §B5 的明确要求）：
#      · owner='cooldown'（ratelimit / auth / dns / conn_reset）：只 `LIMITER.cooldown(wait)`，
#        真实等待由**下一次 LIMITER.acquire()** 承担 ⇒ 日志与审计里的 wait_s 与实际停摆
#        一一对应，且全局限速器是唯一停摆所有者；
#      · owner='sleep'（read_timeout / write_timeout / server_5xx / client / other）：只线程内
#        `time.sleep(wait)`，全局节拍继续由 LIMITER.acquire() + throttle v2 的 note_status
#        反馈负责，不额外全局停摆（crit-adv a2：单条目 400 不得冻结全部 worker）；
#      · owner='none'（fatal / data）：确定性失败 ⇒ **不重试**，因此不产生任何停摆，
#        该值只表意（不存在调用点）。
#    基线的异常路径是 `cooldown(10)` **加** `sleep(5+5·att)`：两处计同一个窗口，实测多停
#    5s（test-report T6 的 legacy 对照读数）；新路径每个错误**恰好一个**停摆归属。
# ④ 与 throttle v2 的分工：throttle v2 拥有**长期节拍**（间隔 / AIMD 地板 / 1s 窗口上限，
#    由 note_status 的 429/400/conn/5xx 反馈驱动）；fetch 重试只拥有**单次请求的重试排程与
#    即时停摆**。THROTTLE_COOLDOWN=1.0 只是限速器内部小缓冲（未改动），不构成第二份退避。
# ⑤ 预算：C1 要求「重试 ≥3 次」⇒ v2 下尝试次数下限 1+RETRY_MIN_RETRIES = 4；默认
#    retries=6（= 5 次重试，与基线同值）。
#    R-2 语义澄清（crit-correct R-2 的第三个反例）：**RETRY_MAX_ATTEMPTS 是硬上限**，
#    任何来源（调用方 retries 或 ENFORCE_MIN 的防御性下限）都不得突破它；
#    ENFORCE_MIN 只把**低于下限**的调用方取值抬到 min(1+RETRY_MIN_RETRIES, MAX_ATTEMPTS)。
#    于是 `SCRAPE_RETRY_MAX_ATTEMPTS=2` ⇒ 真的只发 2 次（并打印一条告警说明 C1 被显式
#    降级），不再"静默覆盖用户硬上限"。见 _retry_attempt_budget()。
# ⑥ 开关：SCRAPE_RETRY=v2（默认）/ legacy（逐行为等价基线：cooldown+sleep 双计）。
#    R-6 修正：legacy 模式**不写任何审计事件**（含 fatal 的 error_classified 与预算耗尽的
#    retry_exhausted——R3-P4 前它们写在 mode 判断之外，与"逐行为等价"的声明冲突）；
#    legacy 也保留基线的 note_status 调用（含 fatal 分支）⇒ 回滚开关确实回到基线行为，
#    该差异是**有意**的（v2 才是 R-B6 的新语义），单测 T13 双向断言。
# ===========================================================================
_R3_RUN_ID = os.environ.get('SCRAPE_RUN_ID') or ('boot-' + datetime.now().strftime('%Y%m%dT%H%M%S'))

_RETRY_MODE_ALIASES = {'v2': 'v2', 'auto': 'v2', 'on': 'v2', '1': 'v2', 'true': 'v2', 'yes': 'v2',
                       'legacy': 'legacy', 'v1': 'legacy', 'base': 'legacy', 'off': 'legacy',
                       '0': 'legacy', 'false': 'legacy', 'no': 'legacy'}

def _r3_retry_mode():
    """SCRAPE_RETRY 解析：v2（默认，新自愈）/ legacy（基线逐行为等价，回滚开关）。
    非法值 ⇒ 告警并回退 v2（fail-safe 方向：宁可多自愈，不可静默退回无退避）。"""
    raw = (os.environ.get('SCRAPE_RETRY') or 'v2').strip().lower()
    if raw in _RETRY_MODE_ALIASES:
        return _RETRY_MODE_ALIASES[raw]
    sys.stderr.write(f"[retry] 非法 SCRAPE_RETRY={raw!r} → 回退 v2\n")
    return 'v2'

RETRY_MODE = _r3_retry_mode()                 # 'v2' | 'legacy'
RETRY_MIN_RETRIES = 3                         # C1 硬指标：断连/超时/5xx 至少重试 3 次
RETRY_ENFORCE_MIN = _env_num('SCRAPE_RETRY_ENFORCE_MIN', 1, int) == 1   # v2 下补齐 C1 下限
RETRY_MAX_ATTEMPTS = _env_num('SCRAPE_RETRY_MAX_ATTEMPTS', 12, int)     # 尝试次数**硬上限**（不可被下限突破）
# R-2：抖动形式由「乘性 U(0.75,1.25)」改为 **equal jitter**（见 _retry_wait_plan）。
# R-8（P4.5·v2.1）：**旧的 RETRY_JITTER_LO/HI 已删除**。它们是"乘性抖动"的口径
#   （wait = d × U(0.75,1.25)，E[wait] = 1.00·d），与本实现不符；保留导出会让读者
#   （验收探针 / 报告读者）拿到错误口径：例如按 [0.75,1.25]×d 判"样本是否越界"会把
#   equal jitter 合法落在 [0.5·d, 0.75·d) 的样本误判为越界。
#   现行口径（唯一权威：`_retry_wait_band` / `_retry_wait_plan`）：
#     d = min(cap, base·factor^att)；wait = d/2 + U(0, d/2)
#     ⇒ wait/d ∈ [RETRY_JITTER_BAND]，E[wait] = RETRY_JITTER_MEAN·d = 0.75·d。
#   与乘性基线相比：期望等待**收紧 25%**（1.00·d → 0.75·d），上界由 1.25·d 降到 1.00·d
#   （更保守）；这是 retry-fatal 的**已知取舍**（抖动不再可能超过 d，cap 语义更干净），
#   代价是同一档位的等待分布整体左移（对限流场景是更激进的试探，故保留 cap 与全局
#   cooldown 归属不变）。读数对账请用 RETRY_JITTER_BAND/MEAN 或直接调 `_retry_wait_band`。
RETRY_JITTER_BAND = (0.5, 1.0)                # wait/d 的取值带（equal jitter 下界、上界）
RETRY_JITTER_MEAN = 0.75                      # E[wait]/d（= 带的均值；读数对账用）
# 每类错误的指数退避参数：class -> (base_s, factor, cap_s)
#   d = min(cap, base × factor^att)；wait = d/2 + U(0, d/2)（equal jitter）
RETRY_BACKOFF = {
    'dns':           (2.0, 2.0, 30.0),        # 解析失败：按"网络整体不可用"处理（全局停摆）
    'conn_reset':    (2.0, 2.0, 30.0),        # 重置/拒绝/中断/建立超时/SSL：同上
    'read_timeout':  (1.0, 2.0, 15.0),        # 读超时：本次已耗掉请求超时，退避可短
    'write_timeout': (1.0, 2.0, 15.0),        # 写超时：同上
    'server_5xx':    (5.0, 2.0, 60.0),        # 服务端故障：保守（不等于基线 15+10·att 的侵略性）
    'ratelimit':     (15.0, 2.0, 60.0),       # 429（无歧义的限流）：factor 1.5→2.0（R-2 单调性）
    'auth':          (15.0, 2.0, 60.0),       # 401/403：基线也重试，沿用保守档（factor 同上）
    'client':        (2.0, 2.0, 20.0),        # 400：单条目责任，线程内退避（不冻结全局）
    'other':         (2.0, 2.0, 20.0),        # 其余异常（v2 下 data 类已单独 fail-fast）
    'fatal':         (0.0, 1.0, 0.0),         # 确定性失败：**不重试** ⇒ 参数只表意
    'data':          (0.0, 1.0, 0.0),         # 数据类失败：**不重试** ⇒ 参数只表意
}
# 停摆归属：'cooldown' = 全局限速器唯一停摆（不 sleep）；'sleep' = 仅线程内退避（不 cooldown）；
#           'none' = 不重试、无停摆（只表意）
RETRY_STALL_OWNER = {
    'ratelimit': 'cooldown', 'auth': 'cooldown', 'dns': 'cooldown', 'conn_reset': 'cooldown',
    'read_timeout': 'sleep', 'write_timeout': 'sleep', 'server_5xx': 'sleep',
    'client': 'sleep', 'other': 'sleep', 'fatal': 'none', 'data': 'none',
}
RETRY_STATUS_CODES = (400, 401, 403, 429, 500, 502, 503, 504)   # = 基线 L1578 逐值相同
RETRY_RATELIMIT_CODES = (429,)                # R3-P4：400 已拆出（见 RETRY_CLIENT_CODES）
RETRY_CLIENT_CODES = (400,)                   # 400：歧义码（本站限流码 or 查询构造错）⇒ client 类
RETRY_FATAL_CODES = (404, 410)                # 确定性 fatal（R-B1 矩阵：404/410/其余 4xx/3xx）
RETRY_AUTH_CODES = (401, 403)
RETRY_LEGACY_STATUS = (15, 10)              # legacy: wait = 15 + 10·att（基线 L1579；**int** ⇒ 日志逐字节相同）
RETRY_LEGACY_EXC = (10, 5, 5)                 # legacy: cooldown(10) + sleep(5 + 5·att)（基线 L1601-1602；int 同上）
RETRY_AUDIT_CLASSIFY_EVERY = max(1, _env_num('SCRAPE_RETRY_CLASSIFY_EVERY', 1, int))  # 采样比（≥2 时降频）

# 异常 → 类别的判据表。**优先级从具体到泛化**（R-1）：类型表（含 MRO 名）与"具体文本特征"
# 都排在泛化文本特征（'timed out'/'timeout'）之前。
_RETRY_DNS_TYPES = ('NameResolutionError', 'gaierror')
_RETRY_DNS_MARKERS = ('name resolution', 'name or service not known', 'nodename nor servname',
                      'getaddrinfo', 'temporary failure in name resolution',
                      'no address associated', 'failed to resolve', 'name does not resolve')
# 建立连接超时：**必须**先于泛化 timeout 读特征（R-1 的核心修正）
_RETRY_CONNECT_TIMEOUT_TYPES = ('ConnectTimeout', 'ConnectTimeoutError')
_RETRY_CONNECT_TIMEOUT_MARKERS = ('connect timeout', 'connection timeout', 'connect timed out',
                                  'timeout during connect', 'timed out during connect')
_RETRY_WRITE_MARKERS = ('write timed out', 'writetimeout', 'write timeout')
_RETRY_WRITE_TYPES = ('WriteTimeout', 'WriteTimeoutError')
_RETRY_READ_MARKERS = ('read timed out', 'readtimeout', 'read timeout', 'timed out', 'timeout')
_RETRY_READ_TYPES = ('ReadTimeout', 'ReadTimeoutError', 'Timeout', 'TimeoutError', 'timeout')
_RETRY_RESET_TYPES = ('ConnectionError', 'ConnectionResetError', 'ConnectionAbortedError',
                      'RemoteDisconnected', 'ProtocolError', 'SSLError', 'SSLEOFError',
                      'ProxyError', 'NewConnectionError', 'MaxRetryError', 'IncompleteRead',
                      'ChunkedEncodingError', 'BrokenPipeError', 'ConnectionRefusedError')
_RETRY_RESET_MARKERS = ('connection reset', 'connection aborted', 'connection refused',
                        'connection error', 'remote end closed', 'remote disconnected',
                        'eof occurred', 'bad status line', 'tunnel connection failed',
                        'broken pipe', 'ssl', 'certificate', 'proxy')
# 数据类（确定性失败：同一输入的两次尝试之间输入不变 ⇒ 不可重试；R-B1/R-B7）
#   含 200-but-bad-body 的 `r.json()` 失败（JSONDecodeError ⊂ ValueError）与结构错
#   （KeyError/TypeError/AttributeError —— 体不符合预期时由解析/取字段路径抛出）。
_RETRY_DATA_TYPES = ('JSONDecodeError', 'InvalidJSONError', 'ContentDecodingError',
                     'UnicodeDecodeError', 'ValueError', 'KeyError', 'IndexError',
                     'TypeError', 'AttributeError')


def _retry_exc_names(e):
    """异常的类型名集合：自身 + 全部 MRO 基类名（兼容子类，不 import requests/urllib3）。"""
    names = set()
    try:
        for k in type(e).__mro__:
            names.add(k.__name__)
    except Exception:
        names.add(type(e).__name__)
    return names


def _retry_cls_of_names(names, msg):
    """(类型名集合, 小写消息) → 类别。**唯一**的分类判据函数（顺序 = 优先级，R-1）。"""
    for t in _RETRY_DNS_TYPES:                        # ① DNS 类型
        if t in names:
            return 'dns'
    for m in _RETRY_DNS_MARKERS:                      # ② DNS 特征
        if m in msg:
            return 'dns'
    for t in _RETRY_CONNECT_TIMEOUT_TYPES:            # ③ 建立连接超时：类型先于泛化 timeout 特征
        if t in names:
            return 'conn_reset'
    for m in _RETRY_CONNECT_TIMEOUT_MARKERS:          # ④ 建立连接超时：具体文本特征
        if m in msg:
            return 'conn_reset'
    for t in _RETRY_DATA_TYPES:                       # ⑤ 数据类（确定性）先于各类网络特征
        if t in names:
            return 'data'
    for m in _RETRY_WRITE_MARKERS:                    # ⑥ 写超时特征
        if m in msg:
            return 'write_timeout'
    for t in _RETRY_WRITE_TYPES:
        if t in names:
            return 'write_timeout'
    for m in _RETRY_READ_MARKERS:                     # ⑦ 读超时特征（泛化 'timed out' 在此被兜住）
        if m in msg:
            return 'read_timeout'
    for t in _RETRY_READ_TYPES:                       # ⑧ 读超时类型
        if t in names:
            return 'read_timeout'
    for t in _RETRY_RESET_TYPES:                      # ⑨ 连接复位类型（含 MaxRetryError/ProxyError/SSL）
        if t in names:
            return 'conn_reset'
    for m in _RETRY_RESET_MARKERS:                    # ⑩ 连接复位特征
        if m in msg:
            return 'conn_reset'
    return 'other'


def _retry_class_of_status(code):
    """HTTP 状态码 → 类别（分类学；与「是否重试」解耦）。

    R3-P4：400 拆出为 `client`（歧义码，不冻结全局）；404/410 与其余确定性 4xx/3xx 归 `fatal`。
    """
    try:
        c = int(code)
    except (TypeError, ValueError):
        return 'other'
    if c in RETRY_RATELIMIT_CODES:          # 429：无歧义的限流
        return 'ratelimit'
    if c in RETRY_CLIENT_CODES:             # 400：歧义码 ⇒ client（单条目责任）
        return 'client'
    if c in RETRY_AUTH_CODES:
        return 'auth'
    if 500 <= c <= 599:
        return 'server_5xx'
    if c in RETRY_FATAL_CODES:              # 404/410：确定性 fatal
        return 'fatal'
    if 400 <= c <= 499:                     # 其余 4xx：确定性（不在重试集合内）
        return 'fatal'
    if 300 <= c <= 399:                     # 3xx：基线不重试 ⇒ fatal
        return 'fatal'
    return 'other'


def _retry_class_of_exc(e):
    """异常 → 类别。判据优先级：**具体信号 > 泛化信号**（R-1 修正的核心）。

    判据链（逐层，命中即返回）：
      ① 自身类型名/MRO 名 + 消息文本 → _retry_cls_of_names（顺序 = 优先级）
      ② 若仍为 'other'：**下钻 cause 链**（`__cause__` / `__context__` / args 里的异常对象）
         —— 真实 requests/urllib3 形态（`ConnectionError(MaxRetryError(ConnectTimeoutError))`）
         在裸消息里可能不含任何特征，但 cause 对象带着精确类型。
    该结构保证两条反面约束同时成立：
      · `ConnectionError('…Read timed out.')` → read_timeout（具体读特征命中，不被类型表抢走）；
      · `ConnectionError('…(Caused by ConnectTimeoutError(…connect timeout=10))')` /
        `MaxRetryError(reason='Connection to h timed out. (connect timeout=10)')` → conn_reset。
    """
    seen = set()
    cur = e
    depth = 0
    while cur is not None and depth < 5 and id(cur) not in seen:
        seen.add(id(cur))
        depth += 1
        try:
            msg = str(cur)[:400].lower()
        except Exception:
            msg = ''
        cls = _retry_cls_of_names(_retry_exc_names(cur), msg)
        if cls != 'other':
            return cls
        # 下钻：__cause__（raise ... from）优先于 __context__（隐式链），再退到 args 里的异常
        nxt = getattr(cur, '__cause__', None) or getattr(cur, '__context__', None)
        if nxt is None:
            for a in getattr(cur, 'args', ()) or ():
                if isinstance(a, BaseException):
                    nxt = a
                    break
        cur = nxt if nxt is not cur else None
    return 'other'


def _retry_wait_band(cls, att):
    """退避档位的 (lo, hi)：equal jitter 下 wait ∈ [d/2, d]，d = min(cap, base·factor^att)。

    这是 _retry_wait_plan 的**解析上界**，供单测断言 R-B2 的两条性质（cap / 首档 / 单调）。
    R-8（v2.1）：本函数与 `RETRY_JITTER_BAND`/`RETRY_JITTER_MEAN` 是抖动口径的**唯一权威**；
    旧的 `RETRY_JITTER_LO/HI`（乘性口径）已删除，不得再按 [0.75,1.25]×d 判越界。
    返回的 (lo, hi) 与 RETRY_JITTER_BAND 的关系：lo = BAND[0]·d，hi = BAND[1]·d。
    """
    base, factor, cap = RETRY_BACKOFF.get(cls, RETRY_BACKOFF['other'])
    d = float(base) * (float(factor) ** int(att))
    if cap:
        d = min(d, float(cap))
    return d / 2.0, d


def _retry_wait_plan(cls, att):
    """一次退避的 (wait_s, owner)：退避时长与停摆归属的**唯一**来源。

    公式（equal jitter，survey-retry §R-B2）：d = min(cap, base·factor^att)；
    wait = d/2 + U(0, d/2)。结构性性质（均有单测）：
      · wait ≤ d ≤ cap ⇒ **max(wait) ≤ CAP**（对任意抖动抽样成立）；
      · att=0 时 d = min(cap, base) ⇒ **首档 ≤ BASE**；
      · 增长区（d 未饱和）内 factor≥2 ⇒ band_k=[d_k/2,d_k] 与 band_{k+1} 不重叠 ⇒ 序列非递减。
    """
    lo, hi = _retry_wait_band(cls, att)
    wait = lo + random.uniform(0.0, max(0.0, hi - lo))
    return wait, RETRY_STALL_OWNER.get(cls, 'sleep')


def _retry_attempt_budget(retries):
    """尝试次数预算：`RETRY_MAX_ATTEMPTS` 是**硬上限**，`RETRY_ENFORCE_MIN` 只在下方补齐。

    v2：attempts = min(max(1, retries), MAX_ATTEMPTS)，再在 ENFORCE_MIN 打开时抬到
        min(1+RETRY_MIN_RETRIES, MAX_ATTEMPTS)（C1 的「≥3 次重试」）。
    语义澄清（crit-correct R-2）：防御性下限**不得**静默突破用户显式设置的硬上限；
        若用户把上限压到 1+RETRY_MIN_RETRIES 之下，C1 即为显式降级，这里打印一条告警。
    legacy：与基线逐行为等价 —— 直接用调用方的 retries（不做任何 clamp）。
    """
    try:
        n = int(retries)
    except (TypeError, ValueError):
        n = 6
    n = max(1, n)
    if RETRY_MODE != 'v2':
        return n
    hard = max(1, int(RETRY_MAX_ATTEMPTS))
    n = min(n, hard)
    if RETRY_ENFORCE_MIN:
        floor = 1 + int(RETRY_MIN_RETRIES)
        if n < min(floor, hard):
            n = min(floor, hard)
        elif hard < floor:
            log(f"[retry] 告警：SCRAPE_RETRY_MAX_ATTEMPTS={hard} < C1 下限 {floor} ⇒ "
                f"尝试次数按硬上限执行，C1「重试≥{RETRY_MIN_RETRIES} 次」被显式降级")
    return n


def _retry_apply_wait(wait, owner):
    """施加退避（**单一**停摆归属）。返回实际生效方式：'cooldown' | 'sleep'。

    owner='cooldown' ⇒ 只登记全局冷却，真实等待发生在下一次 LIMITER.acquire()（避免与本
    函数的 sleep 叠加成双计）；无法登记时退化为线程内 sleep（宁停不空转）。
    """
    if owner == 'cooldown':
        try:
            LIMITER.cooldown(wait)
            return 'cooldown'
        except Exception:
            pass
    time.sleep(wait)
    return 'sleep'


# ---- 请求级审计（R3 schema；独立模块 audit.py，函数内 import，缺模块即整体 no-op） ----
_AUDIT_MOD = {'mod': None, 'tried': False, 'failed': 0, 'cls_n': 0}
_AUDIT_LOCK = threading.Lock()

def _audit_get():
    """懒加载 audit.py 并绑定 OUT/audit.jsonl。任何异常 ⇒ 审计 no-op，绝不影响抓取。"""
    if not _AUDIT_MOD['tried']:
        with _AUDIT_LOCK:
            if not _AUDIT_MOD['tried']:
                _AUDIT_MOD['tried'] = True
                try:
                    import audit as _am
                    _am.init(path=str(OUT / 'audit.jsonl'), run=_R3_RUN_ID)
                    _AUDIT_MOD['mod'] = _am
                    log(f"[audit] 请求级审计 path={_am.path()} run={_R3_RUN_ID} "
                        f"enabled={_am.is_enabled()}")
                except Exception as _ae:
                    _AUDIT_MOD['mod'] = None
                    log(f"[audit] 审计模块未启用（{type(_ae).__name__}: {_ae}）⇒ 审计 no-op")
    return _AUDIT_MOD['mod']

def _retry_audit(level, event, detail=None):
    try:
        mod = _audit_get()
        if mod is None:
            return None
        return mod.emit(level, event, detail)
    except Exception:
        _AUDIT_MOD['failed'] += 1
        return None

def _retry_audit_classified(cls, action, status=None, exc=None):
    """`error_classified {class, action}`（可选事件；SCRAPE_RETRY_CLASSIFY_EVERY≥2 时按 N 采样）。"""
    try:
        if RETRY_AUDIT_CLASSIFY_EVERY > 1:
            _AUDIT_MOD['cls_n'] += 1
            if (_AUDIT_MOD['cls_n'] % RETRY_AUDIT_CLASSIFY_EVERY) != 0:
                return None
        d = {'class': cls, 'action': action}
        if status is not None:
            d['status'] = int(status)
        if exc:
            d['exc'] = str(exc)[:80]
        return _retry_audit('request', 'error_classified', d)
    except Exception:
        _AUDIT_MOD['failed'] += 1
        return None

def _retry_audit_scheduled(attempt, status_or_exc, wait_s, q, cls, owner):
    """`retry_scheduled {attempt, status_or_exc, wait_s, q}`（+ class/stall_owner 便于诊断）。"""
    return _retry_audit('request', 'retry_scheduled', {
        'attempt': int(attempt), 'status_or_exc': str(status_or_exc)[:120],
        'wait_s': round(float(wait_s), 3), 'q': str(q)[:120],
        'class': cls, 'stall_owner': owner})

def _retry_audit_exhausted(attempts, q, last_error):
    """`retry_exhausted {attempts, q, last_error}`：预算耗尽 ⇒ 调用方需回队/降级（R4）。"""
    return _retry_audit('request', 'retry_exhausted', {
        'attempts': int(attempts), 'q': str(q)[:120],
        'last_error': str(last_error)[:200]})


def fetch(q, limit=1, offset=0, sort=None, retries=6):
    params = {"vid": VID, "tab": "default_tab", "scope": "MyInstitution", "q": q,
              "limit": str(limit), "offset": str(offset), "lang": "zh_CN",
              "mode": "Basic", "getMore": 0, "inst": INST}
    if sort:
        params["sort"] = sort
    url = HOST + "/primaws/rest/pub/pnxs"
    # R2 v2: 本次请求的类别（延迟劣化基线按类别隔离，见 RateLimiter 文档）
    _kind = 'bulk' if limit >= THROTTLE_BULK_LIMIT else 'light'
    # R3: `retries` 语义与基线同名（= 尝试次数，非重试次数）。v2 下按 C1 下限补齐，
    #     但 RETRY_MAX_ATTEMPTS 是硬上限（R-2：防御性下限不得静默突破用户上限）。
    _attempts = _retry_attempt_budget(retries)
    _last_err = None
    for att in range(_attempts):
        LIMITER.acquire()
        t0 = time.time()
        # R2 v2（T5 读数口径）：acquire() 返回后即「请求发出时刻」，一次尝试登记一笔。
        # 原实现只在收尾用一个 issued_ts，把「发出→响应→退避睡眠」全算进节拍，
        # 延迟劣化场景下读出 7.4 req/s / 0.135s 这类越界伪读数（crit-correct T5）。
        try:
            LIMITER.note_issued(t0)
        except Exception:
            pass
        try:
            r = SESSION.get(url, params=params,
                            headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
                            timeout=60)
            if r.status_code == 200:
                _lat = time.time() - t0
                # 200 是**成功的 HTTP 交换**：延迟/干净样本照旧喂给 throttle（它不是数据类失败）
                try:
                    LIMITER.note_status(200, _lat, kind=_kind, issued_ts=t0)
                except Exception:
                    pass
                if RETRY_MODE == 'legacy':
                    # 基线逐行为等价：解析异常落进外层泛 except 的 legacy 分支（基线也重试）
                    return r.json()
                # ---- R-B7：体解析/结构不符合预期 = 数据类**确定性**失败 ----
                # 单独 catch ⇒ 直接失败返回：不重试、不喂控制器、一次尝试即结束
                # （判据用 Crawlee 的话：同一输入在两次尝试之间不会变 ⇒ 不可重试）。
                try:
                    _body = r.json()
                except Exception as _de:
                    _dc = _retry_class_of_exc(_de)
                    if _dc == 'other':
                        _dc = 'data'
                    log(f"    DATA[{_dc}] {type(_de).__name__}: {str(_de)[:60]} q={q[:38]!r} ⇒ 直接失败")
                    _retry_audit_classified(_dc, 'drop', status=200, exc=type(_de).__name__)
                    return None
                if not isinstance(_body, dict) or not isinstance(_body.get('info'), dict):
                    log(f"    DATA[data] 200 但体结构不合预期（{type(_body).__name__}）"
                        f" q={q[:38]!r} ⇒ 直接失败")
                    _retry_audit_classified('data', 'drop', status=200,
                                            exc='MalformedBody')
                    return None
                return _body
            # ---- 非 200：分类 → （重试集合内）退避重试 /（集合外）致命返回 None ----
            _cls = _retry_class_of_status(r.status_code)
            _lat = time.time() - t0
            if r.status_code not in RETRY_STATUS_CODES:
                # 基线语义（L1588-1593）：不在重试集合内的状态码 = 致命，不重试、返回 None
                log(f"    HTTP {r.status_code} fatal q={q[:60]!r}")
                # R-B6（crit-correct R-4/H4）：fatal 是**确定性**失败，不喂限速器
                # （不发散 AIMD 地板 0.2→1.2、不计 err_streak、不进观测窗口）。
                # legacy 保留基线调用 ⇒ 回滚开关确实回到基线行为。
                if RETRY_MODE == 'legacy':
                    try:
                        LIMITER.note_status(r.status_code, _lat, kind=_kind, issued_ts=t0)
                    except Exception:
                        pass
                else:
                    _retry_audit_classified(_cls, 'fatal', status=r.status_code)
                return None
            if RETRY_MODE == 'legacy':
                # ---- 基线逐行为等价（L1578-1587）：cooldown(wait) + sleep(wait) ----
                _wait = RETRY_LEGACY_STATUS[0] + att * RETRY_LEGACY_STATUS[1]
                log(f"    HTTP {r.status_code} q={q[:38]!r} cooldown {_wait}s ({att+1})")
                try:
                    LIMITER.note_status(r.status_code, _lat, kind=_kind, issued_ts=t0)
                except Exception:
                    pass
                LIMITER.cooldown(_wait)
                time.sleep(_wait)
                _last_err = f"HTTP {r.status_code}"
                continue
            # a2 归因分离：400（client）**每个逻辑请求只喂一次**限速器 —— 同一查询重复 400
            # 不是「服务端整体水位」的新证据（否则单个毒查询把全局 AIMD 地板顶到上限）；
            # 跨请求的重复 400 仍会逐次推动地板 ⇒ 真·服务端限流依然被响应。
            # 429 等其它可重试类每次尝试都喂（原语义不变）。
            if _cls != 'client' or att == 0:
                try:
                    LIMITER.note_status(r.status_code, _lat, kind=_kind, issued_ts=t0)
                except Exception:
                    pass
            _last_err = f"HTTP {r.status_code}"
            _more = (att + 1) < _attempts
            _retry_audit_classified(_cls, 'retry' if _more else 'exhausted',
                                    status=r.status_code)
            if not _more:
                break                                   # 预算耗尽 → 出循环写 retry_exhausted
            _wait, _owner = _retry_wait_plan(_cls, att)
            log(f"    HTTP {r.status_code} [{_cls}] q={q[:38]!r} backoff {_wait:.2f}s "
                f"owner={_owner} ({att+1}/{_attempts - 1})")
            _retry_audit_scheduled(att + 1, f"HTTP {r.status_code}", _wait, q, _cls, _owner)
            _retry_apply_wait(_wait, _owner)            # 先写审计再停摆（停摆期被 kill 也不丢事件）
            continue
        except Exception as e:
            if RETRY_MODE == 'legacy':
                # ---- 基线逐行为等价（L1594-1602）：cooldown(10) + sleep(5+5·att) ----
                log(f"    EXC {type(e).__name__}: {str(e)[:60]} q={q[:38]!r}")
                try:
                    LIMITER.note_status(None, time.time() - t0, exc=type(e).__name__,
                                        kind=_kind, issued_ts=t0)
                except Exception:
                    pass
                LIMITER.cooldown(RETRY_LEGACY_EXC[0])
                time.sleep(RETRY_LEGACY_EXC[1] + att * RETRY_LEGACY_EXC[2])
                _last_err = f"{type(e).__name__}: {str(e)[:120]}"
                continue
            _cls = _retry_class_of_exc(e)
            _ename = type(e).__name__
            _emsg = str(e)[:120]
            log(f"    EXC[{_cls}] {_ename}: {str(e)[:60]} q={q[:38]!r}")
            if _cls == 'data':
                # R-B7 兜底：数据类（体解析/结构错，含从 SESSION.get 内部抛出的解析异常）
                # 是**确定性**失败 ⇒ 不重试、不喂控制器、一次尝试即失败返回。
                _retry_audit_classified('data', 'drop', exc=_ename)
                return None
            try:
                LIMITER.note_status(None, time.time() - t0, exc=_ename,
                                    kind=_kind, issued_ts=t0)
            except Exception:
                pass
            _last_err = f"{_ename}: {_emsg}"
            _more = (att + 1) < _attempts
            _retry_audit_classified(_cls, 'retry' if _more else 'exhausted', exc=_ename)
            if not _more:
                break
            _wait, _owner = _retry_wait_plan(_cls, att)
            log(f"    EXC[{_cls}] {_ename} q={q[:38]!r} backoff {_wait:.2f}s "
                f"owner={_owner} ({att+1}/{_attempts - 1})")
            _retry_audit_scheduled(att + 1, f"{_ename}: {_emsg}", _wait, q, _cls, _owner)
            _retry_apply_wait(_wait, _owner)
            continue
    log(f"    请求最终失败 q={q[:50]!r}")
    if RETRY_MODE != 'legacy':
        # R-6：legacy 分支不在循环内写任何事件（含 fatal 的 error_classified），
        # 预算耗尽事件同样只属于 v2 —— 否则"legacy 逐行为等价基线 + 无审计事件"不成立。
        _retry_audit_exhausted(_attempts, q, _last_err)
    return None

def get_total(prefix, sort=None):
    d = fetch(f"holding_call_number,begins_with,{prefix}", limit=1, offset=0, sort=sort)
    if d is None:
        return None
    return d['info'].get('totalResultsLocal')

def extract_record(doc, prefix):
    disp = doc.get('pnx', {}).get('display', {})
    def g(k):
        v = disp.get(k, [])
        return v[0] if v else ''
    rec = {
        'mms': g('mms'),
        'title': g('title'),
        'creator': g('creator'),
        'publisher': g('publisher'),
        'year': g('creationdate'),
        'language': g('language'),
        'type': g('type'),
        'prefix': prefix,
        'holdings': []
    }
    for h in (doc.get('delivery', {}).get('holding', []) or []):
        rec['holdings'].append({
            'lib': h.get('libraryCode', ''),
            'main': h.get('mainLocation', ''),
            'sub': h.get('subLocation', ''),
            'sub_code': h.get('subLocationCode', ''),
            'call': h.get('callNumber', ''),
            'status': h.get('availabilityStatus', ''),
        })
    return rec

def merge_rec(got, rec):
    prev = got.get(rec['mms'])
    if prev is None:
        got[rec['mms']] = rec
        return
    seen = {(h['lib'], h['sub'], h['call']) for h in prev['holdings']}
    for h in rec['holdings']:
        key = (h['lib'], h['sub'], h['call'])
        if key not in seen:
            prev['holdings'].append(h)
            seen.add(key)

def scrape_leaf(prefix, total, deep_retry=True):
    """v9: 首个排序先行；不足时剩余排序并行补齐。deep_retry=True 时不足则冷却再试一轮。
    v10: 结果以行级原子追加落盘（见 append_records）。"""
    got = {}
    max_rounds = 2 if deep_retry else 1
    q = f"holding_call_number,begins_with,{prefix}"
    for rnd in range(max_rounds):
        # 第 1 个排序（经济路径：多数叶子一发即全）
        d = fetch(q, limit=BULK_LIMIT, offset=0, sort=SORTS[0])
        if d is not None:
            for doc in d.get('docs', []):
                rec = extract_record(doc, prefix)
                merge_rec(got, rec)
        if len(got) < total - 2:
            # 缺 → 剩余排序并行补齐
            rest = SORTS[1:]
            def do_fetch(sort):
                return fetch(q, limit=BULK_LIMIT, offset=0, sort=sort)
            with ThreadPoolExecutor(max_workers=effective_workers('leaf')) as ex:
                futs = [ex.submit(do_fetch, s) for s in rest]
                for fut in as_completed(futs):
                    try:
                        dd = fut.result()
                    except Exception as e:
                        log(f"    叶子补抓异常 {prefix!r}: {e}")
                        continue
                    if dd is None:
                        continue
                    for doc in dd.get('docs', []):
                        rec = extract_record(doc, prefix)
                        merge_rec(got, rec)
        if len(got) >= total - 2:
            break
        if rnd < max_rounds - 1:
            LIMITER.cooldown(30)
            time.sleep(30)
    append_records(list(got.values()), prefix)      # v10: 行级原子追加 + fsync
    return True, len(got)

# ===========================================================================
# R3·slot-converge —— 块级兜底（C2）/ 收敛判据机器化（F1·F6）/ 停滞判据（F5）
#                     / 边界修复（F2 HTTP-200-缺-info 调用侧容错；F3 output 目录消失）
#
# 一、退出码即机器判据（F6：去掉「grep 全部完成」这种字符串判据）
#   exit 0  收敛：todo 为空 **且** 无未决缺口（gaps 里没有未修复条目）
#   exit 2  --reconcile-only 无法加载 progress（v10 原义，保留）
#   exit 3  未收敛：todo 为空但仍有未决缺口（且修复预算已耗尽）
#           —— F1「探测失败丢子树仍 exit 0」的正面修复：不再声称收敛
#   exit 4  停滞自停：SCRAPE_STALL_ACTION=stop 且进度年龄 ≥ 自停阈值
#   报告：OUT/convergence-report.json（r3-convergence-v1，含 decided/terminal/exit_code/缺口清单）
#
# 二、审计事件（每行 JSON → OUT/audit.jsonl，遵循 R3-P4 契约 v2 refs/audit-contract-v2.md）
#   行布局（契约 §1，三写者统一）：schema="r3-audit-v2" / ts_epoch(权威) / ts_iso / pid /
#   seq(进程内从 1) / level / event / detail；**不再写遗留键 ts**（历史 ts 语义三侧不一）。
#   block   : probe_all_dead / probe_incomplete / probe_retry / repair_round_enter /
#             repair_round_result / repair_exhausted / convergence_decision / stall_detected
#   request : error_classified（F2 的 200-缺-info 等；按 (class, where) 限流防洪泛）
#   process : output_dir_recreated / records_write_deferred / records_write_dropped /
#             progress_write_failed / run_start / run_end / graceful_stop
#
# 三、块级兜底阶梯（C2：probe 全灭的**明确语义**，替换"静默丢子树"）
#   探测字符三种结局：命中(t>0) / 确证为空(t==0) / 未定论(None 或抛异常，含 200-缺-info)
#   未定论 ⇒ ①重试轮：父块带 probe_dead 计数延迟重排（≤PROBE_DEAD_RETRY_MAX 次）
#             ②降级：保留已命中子块 + 父块自身页，未定论子树登记为**未决缺口**进修复轮
#             ③修复轮：final_recheck_round 用另一排序复验这些字符（≤GAP_REPAIR_MAX 次）
#             ④预算耗尽：缺口标 terminal ⇒ 收敛判据判"未收敛"（exit 3），不洗成完成
# ===========================================================================

R3_AUDIT_SCHEMA = 'r3-audit-v2'    # R3-P4 契约 v2 §1：三写者统一 schema 值（原 r3-audit-v1）
# R3-P4·P4.5 契约 v2.1 §8.1（KI-1 修复）：**写者分组键**。
#   本进程内有两个审计写者共用同一 pid 与同一 OUT/audit.jsonl：
#     ① 本文件的块级写者 audit_event()（_AUDIT_SEQ，本常量 = "converge"）；
#     ② 请求级写者 audit.py（经 _retry_audit 懒加载，其 _ST['seq']，writer = "retry"）。
#   二者的 seq 计数器互相独立 ⇒ 只按 pid 分组会让合流文件出现 seq_duplicate
#   （p4-verify 在真实 S4 上复现：pid=8631 seq=1/2/3 重复）。故每行都写 writer，
#   校验器按 (文件, pid, writer) 分组（契约 §8.2/§8.4）。
R3_AUDIT_WRITER = 'converge'       # 契约 v2.1 §8.1：块级（converge 侧）写者身份
EXIT_CONVERGED = 0
EXIT_INCOMPLETE = int(_env_num('SCRAPE_INCOMPLETE_EXIT', 3, int))   # 未收敛退出码（可配 0 兼容旧 keeper）
EXIT_STALL_STOP = int(_env_num('SCRAPE_STALL_EXIT', 4, int))

# ---- 块级兜底（C2）参数 ---------------------------------------------------
PROBE_ROUNDS = int(_env_num('SCRAPE_PROBE_ROUNDS', 3, int))              # 探测轮数（v9 硬编码 3）
PROBE_DEAD_RETRY_MAX = int(_env_num('SCRAPE_PROBE_DEAD_RETRY', 2, int))  # 全灭后父块重试轮上限
PROBE_DEAD_DELAY_S = float(_env_num('SCRAPE_PROBE_DEAD_DELAY_S', 30.0, float))
GAP_REPAIR_MAX = int(_env_num('SCRAPE_GAP_REPAIR_MAX', 2, int))          # 单缺口进修复轮次数上限
MAX_ROUNDS = int(_env_num('SCRAPE_MAX_ROUNDS', 12, int))                 # 主循环轮数上限

# ---- 未定论条目（条目 total 解析失败 / 条目异常）的尝试预算（R3p4·P4-A：C-1·C-2·C-3）----
#   F1/F2 的另一扇门：条目级 total 解析失败（None 或 200-缺-info 的 KeyError）此前在
#   并发路径无限回队尾（活锁：进程永不退出、永不写裁决），在串行路径静默 pop 丢弃
#   （子树丢失 + exit 0 假收敛）。统一语义：
#     ①预算内 ⇒ 重试轮（带延迟重排，不丢子树）；
#     ②预算耗尽 ⇒ 块级降级记账（登记未决缺口 + 审计 + 进修复轮）⇒ 收敛判据拒绝 exit 0；
#   进程一定有界退出（rc=3 未收敛）。
TOTAL_NONE_RETRY_MAX = int(_env_num('SCRAPE_TOTAL_NONE_RETRY', 2, int))    # 条目 total 未定论重试上限
ITEM_EXC_RETRY_MAX = int(_env_num('SCRAPE_ITEM_EXC_RETRY', 2, int))        # 并发条目异常回队尾上限
ITEM_EXC_DELAY_S = float(_env_num('SCRAPE_ITEM_EXC_DELAY_S', 1.0, float))  # 回队尾延迟（基线 1.0s）
UNDETERMINED_LEDGER_MAX = int(_env_num('SCRAPE_UNDET_LEDGER_MAX', 200, int))   # 未定论账本上限
RESOLVED_GAPS_KEEP = int(_env_num('SCRAPE_RESOLVED_GAPS_KEEP', 200, int))      # 已消解缺口留档上限

# ---- 停滞判据（F5）参数 ---------------------------------------------------
STALL_PROGRESS_AGE_S = float(_env_num('SCRAPE_STALL_AGE_S', 900.0, float))     # 审计阈值
STALL_STOP_AGE_S = float(_env_num('SCRAPE_STALL_STOP_AGE_S', 1800.0, float))   # 自停阈值
STALL_ACTION = (os.environ.get('SCRAPE_STALL_ACTION') or 'audit').strip().lower()
if STALL_ACTION not in ('audit', 'stop'):      # 非法值 ⇒ fail-safe：只审计，不自停
    STALL_ACTION = 'audit'
STALL_MONITOR_ENABLED = int(_env_num('SCRAPE_STALL_MONITOR', 1, int))
STALL_CHECK_EVERY_S = max(0.05, float(_env_num('SCRAPE_STALL_CHECK_S', 5.0, float)))

# ---- 审计 / IO 加固参数 ---------------------------------------------------
AUDIT_FILE = Path(os.environ.get('SCRAPE_AUDIT_FILE') or (OUT / 'audit.jsonl'))
CONV_REPORT_FILE = Path(os.environ.get('SCRAPE_CONV_REPORT') or (OUT / 'convergence-report.json'))
CLASSIFY_AUDIT_MAX = int(_env_num('SCRAPE_CLASSIFY_AUDIT_MAX', 20, int))
RECORDS_PENDING_MAX = int(_env_num('SCRAPE_RECORDS_PENDING_MAX', 50000, int))

_AUDIT_LOCK = threading.Lock()
_AUDIT_FD = None
_AUDIT_SEQ = 0
_AUDIT_DROPPED = 0
_AUDIT_RUN_ID = None
_CLASSIFY_SEEN = {}
_OUT_DIR_LOCK = threading.RLock()
_OUT_DIR_EPOCH = 0               # output 目录世代（重建一次 +1；records fd 据此失效重开）
_RECORDS_FD_EPOCH = -1
_RECORDS_PENDING = []            # 待写缓冲（写明前失败时的降级落点）
_RECORDS_DEFERRED = 0
_PROGRESS_STATE = {'watermark': None, 'last_change_ts': None, 'stall_audited': False,
                   'stall_stop_fired': False, 'ticks': 0}
_STALL_THREAD = {'t': None, 'stop': None}
LAST_REPAIR_SUMMARY = {'entered': False, 'gaps_in': 0, 'fixed': 0, 'remaining': 0, 'terminal': 0}
LAST_CONVERGENCE = {}


def _r3_run_id():
    global _AUDIT_RUN_ID
    if _AUDIT_RUN_ID is None:
        _AUDIT_RUN_ID = (os.environ.get('SCRAPE_RUN_ID')
                         or f"{os.getpid()}-{int(time.time())}")
    return _AUDIT_RUN_ID


def _audit_fd_close():
    global _AUDIT_FD
    if _AUDIT_FD is not None:
        try:
            os.close(_AUDIT_FD)
        except OSError:
            pass
        _AUDIT_FD = None


def _audit_fd_get():
    global _AUDIT_FD
    if _AUDIT_FD is not None:
        if not OUT.is_dir():                       # R3·F3：目录没了 → 先重建
            _ensure_out_dir_raw('audit')
        if not _fd_alive(_AUDIT_FD):               # 被删/孤立 inode → 重开，避免审计进黑洞
            _audit_fd_close()
    if _AUDIT_FD is None:
        _AUDIT_FD = os.open(str(AUDIT_FILE),
                            os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    return _AUDIT_FD


def audit_event(event, level='block', detail=None, run=None):
    """写一行结构化自愈审计事件（C3：每级自愈事件都必须有结构化记录）。

    永不抛异常、永不阻塞数据面：审计是**观测面**，任何失败都只能降级为计数丢失
    （审计自身写失败 → 尝试重建 output 目录重写一次 → 仍失败则丢弃该事件并计数）。

    契约 v2.1 §8.1：本行带 `writer=R3_AUDIT_WRITER`（"converge"）——本函数的 `_AUDIT_SEQ`
    与 audit.py（"retry"）的 seq 是两个独立计数器，校验器按 (文件, pid, writer) 分组。"""
    global _AUDIT_SEQ, _AUDIT_DROPPED
    try:
        with _AUDIT_LOCK:
            _AUDIT_SEQ += 1
            ev = {'schema': R3_AUDIT_SCHEMA,
                  'ts_epoch': round(time.time(), 3),      # 契约 v2：权威时间键
                  'ts_iso': datetime.now().isoformat(timespec='milliseconds'),
                  'pid': os.getpid(), 'writer': R3_AUDIT_WRITER, 'seq': _AUDIT_SEQ,
                  'level': level, 'event': event, 'run': run or _r3_run_id(),
                  'detail': detail if isinstance(detail, dict) else {'value': detail}}
            blob = (json.dumps(ev, ensure_ascii=False) + '\n').encode('utf-8')
            for attempt in (1, 2):
                try:
                    fd = _audit_fd_get()
                    _write_all(fd, blob)
                    os.fsync(fd)
                    return ev
                except OSError:
                    _audit_fd_close()
                    if attempt == 1:
                        # 只重建目录（不递归审计、不取 records 锁 ⇒ 无锁环）
                        _ensure_out_dir_raw('audit')
            _AUDIT_DROPPED += 1
            return None
    except Exception:
        try:
            _AUDIT_DROPPED += 1
        except Exception:
            pass
        return None


def audit_read(path=None):
    """读回审计事件（测试/诊断用；容忍最后一行半写）。"""
    p = Path(path or AUDIT_FILE)
    out = []
    try:
        with open(str(p), 'r', encoding='utf-8', errors='replace') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except ValueError:
                    continue
    except OSError:
        return []
    return out


def audit_counts(path=None):
    """事件名 → 条数（机器可读摘要）。"""
    c = {}
    for ev in audit_read(path):
        name = ev.get('event') or '?'
        c[name] = c.get(name, 0) + 1
    return c


# ---------------------------------------------------------------------------
# F3：output 目录消失 → 检测 + 重建 + 审计（不打死长程任务）
# ---------------------------------------------------------------------------
def _ensure_out_dir_raw(where='unknown'):
    """只做「检测 + 重建 + 世代推进」（无审计、无 log、可在任意锁内调用）。"""
    global _OUT_DIR_EPOCH
    try:
        if OUT.is_dir():
            return False
    except OSError:
        pass
    with _OUT_DIR_LOCK:
        try:
            if OUT.is_dir():
                return False
            OUT.mkdir(parents=True, exist_ok=True)
            _OUT_DIR_EPOCH += 1
        except Exception as e:
            try:
                sys.stderr.write(f"[r3] output 目录重建失败（where={where}, path={OUT}）："
                                 f"{type(e).__name__}: {e}\n")
            except Exception:
                pass
            return False
    return True


def _note_dir_recreated(where, ep=None):
    """重建通告（审计 + 日志 + stdout）。可在任意锁内安全调用。

    不取 LOG_LOCK（log() 的异常分支会回调到这里，LOG_LOCK 不可重入），
    改用单次 O_APPEND os.write 直写日志：小写入在 O_APPEND 下原子，最坏只是行序交错。"""
    ep = _OUT_DIR_EPOCH if ep is None else ep
    audit_event('output_dir_recreated', 'process',
                {'where': where, 'path': str(OUT), 'epoch': ep})
    line = (f"[{datetime.now().strftime('%H:%M:%S')}] [r3] output 目录消失 → 已重建"
            f"（where={where}, path={OUT}, epoch={ep}）\n")
    try:
        fd = os.open(str(LOG_FILE), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            _write_all(fd, line.encode('utf-8'))
        finally:
            os.close(fd)
    except OSError:
        pass
    try:
        sys.stdout.write(line)
        sys.stdout.flush()
    except Exception:
        pass


def ensure_out_dir(where='unknown'):
    """公开入口：重建 + `output_dir_recreated` 审计 + stdout 通告。返回 True=本次做了重建。

    注意：**不调用 log()**（log() 的异常分支会回调到这里；LOG_LOCK 不可重入）。"""
    if not _ensure_out_dir_raw(where):
        return False
    _note_dir_recreated(where)
    return True


# ---------------------------------------------------------------------------
# F2：get_total 调用侧容错（HTTP 200 但响应缺 'info' → 原实现 d['info'] KeyError）
#   说明：get_total/fetch 属 impl-retry 区域，本 slot **不改其函数体**；
#   容错落在调用侧（probe_children / recheck_chars / 条目 total 解析 / main 基线）。
#   语义：None 一律表示「未定论」（未测到），绝不表示「0 条」（0 是确证的合法读数）。
# ---------------------------------------------------------------------------
def _classify_exc(e):
    """错误四分类的**类**判定（D2 的 R3 侧最小实现，供 audit 与后续 R4 复用）。"""
    if isinstance(e, (KeyError, TypeError, ValueError, AttributeError, IndexError)):
        return 'malformed_response'          # 200 但结构不符（缺 info 等）
    if isinstance(e, OSError) or type(e).__name__ in (
            'ConnectionError', 'Timeout', 'ReadTimeout', 'ConnectTimeout',
            'ChunkedEncodingError', 'SSLError', 'ProxyError', 'NewConnectionError'):
        return 'network'
    return 'unexpected'


def _audit_exc(e, where, prefix=None):
    """异常分类审计：同一 (class, where) 只写前 CLASSIFY_AUDIT_MAX 条，之后只计数。"""
    cls = _classify_exc(e)
    key = (cls, where)
    with _AUDIT_LOCK:
        _CLASSIFY_SEEN[key] = _CLASSIFY_SEEN.get(key, 0) + 1
        n = _CLASSIFY_SEEN[key]
    if n <= CLASSIFY_AUDIT_MAX:
        audit_event('error_classified', 'request',
                    {'class': cls, 'action': 'degrade', 'where': where, 'prefix': prefix,
                     'exc': f'{type(e).__name__}: {e}', 'occurrence': n})
    return cls


def _safe_get_total(prefix, sort=None, where='probe'):
    """get_total 的容错包装（F2）：任何异常 ⇒ 审计 + 返回 None（未定论），绝不外抛。"""
    try:
        return get_total(prefix, sort)
    except Exception as e:
        _audit_exc(e, where, prefix)
        log(f"    [r3] get_total 异常（{where} {prefix!r}）：{type(e).__name__}: {e} → 记为未定论")
        return None


class ProbeOutcome(tuple):
    """probe_children 的返回值（C2/F1）。

    **向后兼容**：仍是 2 元组 `(kids, chars_to_recheck)`，旧调用点
    `kids, zero_chars = probe_children(p)` 行为逐字不变；
    新增 `.meta` 承载「全灭 / 未定论」判据，供块级兜底与审计使用。"""

    def __new__(cls, kids, recheck, meta=None):
        obj = super().__new__(cls, (kids, recheck))
        obj.meta = dict(meta or {})
        return obj

    @property
    def kids(self):
        return self[0]

    @property
    def recheck_chars(self):
        return self[1]

    @property
    def all_dead(self):
        return bool(self.meta.get('all_dead'))

    @property
    def incomplete(self):
        return bool(self.meta.get('incomplete'))

    @property
    def failed_chars(self):
        return list(self.meta.get('failed_chars') or [])

    @property
    def zero_chars(self):
        return list(self.meta.get('zero_chars') or [])


def _probe_outcome_from_exc(prefix, e):
    """探测**整体**异常（池级）时构造"全灭"结论：子树的分布未知 ⇒ 未定论，不丢。"""
    _audit_exc(e, 'probe_children.pool', prefix)
    meta = {'prefix': prefix, 'attempted': len(CHARSET), 'kids': 0, 'resolved': 0,
            'zero_chars': [], 'failed_chars': sorted(CHARSET), 'rounds': 0,
            'all_dead': True, 'incomplete': True, 'exc': f'{type(e).__name__}: {e}'}
    audit_event('probe_all_dead', 'block',
                {'prefix': prefix, 'missing_chars': sorted(CHARSET),
                 'attempted': len(CHARSET), 'rounds': 0,
                 'reason': f'probe_exception:{type(e).__name__}'})
    return ProbeOutcome([], sorted(CHARSET), meta)


def probe_children(prefix):
    """一阶子块探测（v9 语义保留：最多 3 轮 + 失败轮后 cooldown/sleep 30s）。

    R3 增补（C2/F1/F2）：
      · 三种结局明确区分：命中(t>0) / **确证为空**(t==0) / **未定论**(None 或抛异常)；
        只有「命中 + 确证为空」算已定论，未定论字符一律留在 failed 集里；
      · 字符级异常兜住（F2：HTTP 200 缺 info 的 KeyError 不再穿出探测池）；
      · 全灭（无任何命中、无任何确证为空、全部未定论）⇒ 写 `probe_all_dead` 审计并打标记，
        由调用方走块级兜底阶梯（重试轮 → 降级 + 修复轮），**不再静默丢子树**；
      · 返回 ProbeOutcome（2 元组兼容 + .meta 判据）。
    """
    results = {}
    pending = set(CHARSET)
    zero_chars = set()
    rounds_done = 0

    def probe(c):
        try:
            return c, get_total(prefix + c), None
        except Exception as e:                       # F2：字符级兜住
            _audit_exc(e, 'probe_children', prefix + c)
            return c, None, f'{type(e).__name__}: {e}'

    for rnd in range(PROBE_ROUNDS):
        if not pending:
            break
        rounds_done = rnd + 1
        to_probe = sorted(pending)
        pending = set()
        failed_now = set()
        with ThreadPoolExecutor(max_workers=effective_workers('probe')) as ex:
            futs = [ex.submit(probe, c) for c in to_probe]
            for fut in as_completed(futs):
                try:
                    c, t, _err = fut.result()
                except Exception as e:               # 池级异常（极端）：整轮视为未定论
                    _audit_exc(e, 'probe_children.pool', prefix)
                    failed_now.update(to_probe)
                    break
                if t is None:
                    failed_now.add(c)
                elif t > 0:
                    results[prefix + c] = t
                else:
                    zero_chars.add(c)
        pending = failed_now
        if pending:
            log(f"    探测失败 {len(pending)} 字符，30s 后重试（轮 {rnd+1}）")
            LIMITER.cooldown(30); time.sleep(30)

    attempted = len(CHARSET)
    all_dead = (not results) and (not zero_chars) and bool(pending)
    incomplete = bool(pending)
    meta = {'prefix': prefix, 'attempted': attempted, 'kids': len(results),
            'resolved': len(results) + len(zero_chars),
            'zero_chars': sorted(zero_chars), 'failed_chars': sorted(pending),
            'rounds': rounds_done, 'all_dead': all_dead, 'incomplete': incomplete,
            'covered': sum(results.values())}
    if all_dead:
        audit_event('probe_all_dead', 'block',
                    {'prefix': prefix, 'missing_chars': sorted(pending),
                     'attempted': attempted, 'rounds': rounds_done,
                     'reason': 'all_chars_unresolved'})
        log(f"    [r3] 探测全灭 {prefix!r}：{len(pending)}/{attempted} 字符全部未定论"
            f"（{rounds_done} 轮）→ 块级兜底阶梯接手")
    elif incomplete:
        audit_event('probe_incomplete', 'block',
                    {'prefix': prefix, 'missing_chars': sorted(pending),
                     'attempted': attempted, 'rounds': rounds_done,
                     'resolved_chars': meta['resolved'], 'kids': meta['kids']})
    return ProbeOutcome(sorted(results.items()), sorted(zero_chars | pending), meta)


def _probe_dead_policy(item, outcome):
    """块级兜底阶梯的**纯决策**部分（两套主循环共用，无 IO）。

    返回 {'action': 'proceed'|'retry'|'degrade', 'retry_n', 'reason', 'delay_s', 'failed_chars'}"""
    if outcome is None or not getattr(outcome, 'incomplete', False):
        return {'action': 'proceed', 'retry_n': int(item.get('probe_dead', 0) or 0),
                'reason': None, 'delay_s': 0.0, 'failed_chars': []}
    n = int(item.get('probe_dead', 0) or 0)
    kind = 'probe_all_dead' if outcome.all_dead else 'probe_incomplete'
    if n < PROBE_DEAD_RETRY_MAX:
        return {'action': 'retry', 'retry_n': n + 1, 'reason': kind,
                'delay_s': PROBE_DEAD_DELAY_S, 'failed_chars': outcome.failed_chars}
    return {'action': 'degrade', 'retry_n': n, 'reason': kind,
            'delay_s': 0.0, 'failed_chars': outcome.failed_chars}


def _find_degraded_gap(prog, prefix):
    """找出该 prefix 已登记的未消解降级缺口（跨 pass/跨进程仍可用，靠 progress.gaps 反查）。"""
    for g in (prog.get('gaps') or []):
        if g.get('p') == prefix and g.get('degraded') and not g.get('resolved'):
            return g
    return None


def _probe_dead_degrade(prog, item, prefix, total, outcome, pre, kids):
    """降级记账（C2）：**不丢子树** —— 未定论字符登记为「未决缺口」进修复轮。

    父块自身页已由"先抓一笔"（数据保险）落盘；已命中的子块照常入队；
    未定论部分无法证明已覆盖 ⇒ 必须留缺口，收敛判据据此拒绝 exit 0（F1）。"""
    fail_chars = outcome.failed_chars or list(CHARSET)
    known = sum(t for _, t in kids)
    reason = 'probe_all_dead' if outcome.all_dead else 'probe_incomplete'
    gap_rec = {'p': prefix, 'total': total, 'covered': known,
               'gap': max(0, int(total) - known), 'chars': fail_chars, 'type': 'branch',
               'probe_dead': int(item.get('probe_dead', 0) or 0),
               'degraded': True, 'repair_attempts': 0,
               'ts': datetime.now().isoformat(), 'reason': reason}
    prog.setdefault('gaps', []).append(gap_rec)
    led = prog.setdefault('probe_dead', [])
    led.append({'p': prefix, 'ts': gap_rec['ts'], 'reason': reason,
                'failed_chars': fail_chars, 'attempts': gap_rec['probe_dead'], 'pre': pre})
    if len(led) > 200:
        del led[:-200]
    _recovery_note(prog, 'probe-dead-degrade', prefix,
                   f"探测未定论 {len(fail_chars)}/{len(CHARSET)} 字符（重试 {gap_rec['probe_dead']} 轮后）"
                   f"→ 降级：保留已命中子块 {len(kids)} 个 + 父块自身页 {pre} 条，"
                   f"未定论子树登记为未决缺口进修复轮")
    audit_event('repair_round_enter', 'block',
                {'reason': reason, 'prefix': prefix,
                 'todo_snapshot': len(prog.get('todo') or []),
                 'missing_chars': fail_chars, 'attempts': gap_rec['probe_dead']})
    return gap_rec


# ---------------------------------------------------------------------------
# R3p4·P4-A —— 「条目级未定论」的统一降级记账（C-1 / C-2 / C-3 的收敛残留门闭环）
#
#   缺陷面（crit-correct §三 C-1/C-2/C-3，均有实跑读数）：
#     · 并发 `_conc_run_item` 用**裸** get_total ⇒ 200-缺-info 的 KeyError 穿到 _conc_worker，
#       `q.put_tail(item, 1.0)` **无尝试预算** ⇒ 无限回队尾活锁（25s 内 25 次，进程永不退出）；
#     · 串行 `_serial_process_queue` 用 _safe_get_total ⇒ None ⇒ `todo.pop(0)` 静默跳过
#       ⇒ rc=0 + 0 条记录 + 打印「全部完成」+ gaps=0 = **假收敛**（基线是响亮的 rc=1 崩溃）；
#     · 两条路径的失败随后都被 exit 0 盖章 `decided=done`（机器判据反向背书）。
#
#   统一语义（**总原则**：任何「单点失败导致子树静默丢失」都必须登记且影响裁决）：
#     ① 预算内（< TOTAL_NONE_RETRY_MAX）：重试轮 —— 条目带计数延迟重排，不丢子树；
#     ② 预算耗尽：块级降级记账 —— 登记未决缺口（gaps[]，type='undetermined'）+ recovery 账本
#        + 审计（契约扩展表事件 probe_incomplete{scope:'item-total'}）⇒ 进修复轮；
#     ③ 修复轮：重新探测 total（可得 ⇒ 回队重排重跑子树；records 已覆盖 ⇒ 消解留档）；
#        预算耗尽仍不可得 ⇒ 缺口标 terminal ⇒ 收敛判据判「未收敛」exit 3。
#
#   审计事件名一律取自 R3p4/refs/audit-contract-v2.md §2（probe_retry/probe_incomplete 均在
#   扩展表内；retry_scheduled/retry_exhausted/error_classified 在核心表内），不新造表外名。
# ---------------------------------------------------------------------------
def _find_undetermined_gap(prog, prefix):
    """该 prefix 已登记的未消解「未定论」缺口（跨 pass/跨进程复用，避免重复登记）。"""
    for g in (prog.get('gaps') or []):
        if g.get('p') == prefix and g.get('type') == 'undetermined' and not g.get('resolved'):
            return g
    return None


def _undetermined_retry(prog, item, prefix, where):
    """① 重试轮：total 未定论但预算未尽 —— 计数 + 审计 + 日志（队列语义由调用方处理）。"""
    n = int(item.get('total_none', 0) or 0) + 1
    item['total_none'] = n
    audit_event('probe_retry', 'block',
                {'prefix': prefix, 'reason': 'total_none', 'attempt': n,
                 'max_attempts': TOTAL_NONE_RETRY_MAX, 'missing_chars': [],
                 'scope': 'item-total', 'where': where, 'delay_s': PROBE_DEAD_DELAY_S,
                 'todo_snapshot': len(prog.get('todo') or [])})
    log(f"    [r3] 条目 total 未定论 {prefix!r}（{where}）→ 重试轮 "
        f"#{n}/{TOTAL_NONE_RETRY_MAX}，{PROBE_DEAD_DELAY_S:.0f}s 后重排（不丢子树）")
    return n


def _undetermined_degrade(prog, item, prefix, total, reason, where, extra=None):
    """② 降级记账：单点失败 ⇒ 子树未定论，**登记缺口 + 审计 + 交修复轮**（不静默丢、不假收敛）。

    调用方负责队列语义（并发：条目出队后不再回队；串行：todo.pop(0)）与加锁
    （并发路径在 q.lock 内调用，保证 progress 快照自洽 I4）。返回缺口 dict。"""
    ts = datetime.now().isoformat()
    attempts = {'total_none': int(item.get('total_none', 0) or 0),
                'item_exc': int(item.get('exc_retry', 0) or 0)}
    g = _find_undetermined_gap(prog, prefix)
    if g is None:
        g = {'p': prefix, 'total': total, 'covered': 0, 'gap': None, 'chars': [],
             'type': 'undetermined', 'undetermined': True, 'degraded': True,
             'repair_attempts': int(item.get('repair_attempts', 0) or 0),
             'ts': ts, 'reason': reason, 'where': where, 'attempts': attempts}
        prog.setdefault('gaps', []).append(g)
    else:
        g['ts'] = ts
        g['reason'] = reason
        g['where'] = where
        g['attempts'] = attempts
    # 「上次降级时刻」：与 require_ts 配对判定「回队重排后是否又降级」——
    # requeued_ts > last_degrade_ts ⇒ 该子树重排后**跑完了**（无二次降级）⇒ 修复轮可消解，
    # 避免把「瞬时失败的子树」永久留在未决缺口里（假未收敛）。
    g['last_degrade_ts'] = round(time.time(), 3)
    if total:                                 # total 已知（条目异常路径）⇒ 记可读缺口量
        g['total'] = int(total)
        g['covered'] = recorded_count(prefix)
        g['gap'] = max(0, int(total) - int(g['covered']))
    led = prog.setdefault('undetermined', [])
    led.append({'p': prefix, 'ts': ts, 'reason': reason, 'where': where,
                'total': total, 'attempts': attempts})
    if len(led) > UNDETERMINED_LEDGER_MAX:
        del led[:-UNDETERMINED_LEDGER_MAX]
    _recovery_note(prog, 'undetermined-degrade', prefix,
                   f"{reason}（{where}）：total={total} 尝试 {attempts} 后仍不可得 "
                   f"→ 降级登记未决缺口（type=undetermined），收敛判据将拒绝 exit 0")
    detail = {'prefix': prefix, 'reason': reason, 'scope': 'item-total', 'where': where,
              'attempted': max(1, attempts['total_none'] + 1),
              'resolved_chars': 0, 'kids': 0, 'missing_chars': [],
              'undetermined': True, 'total': total,
              'covered': g.get('covered'), 'gap': g.get('gap'), 'attempts': attempts,
              'todo_snapshot': len(prog.get('todo') or [])}
    if extra:
        detail.update(extra)
    audit_event('probe_incomplete', 'block', detail)
    log(f"    [r3] 条目未定论降级 {prefix!r}（{reason}/{where}）：子树登记为未决缺口，"
        f"修复轮接手（不再静默跳过、不再无限回队尾）")
    return g


def _keep_resolved_gap(prog, gap, reason):
    """③ 已消解缺口**留档**（L2/C-5）：从 gaps[] 移除前先落 resolved_gaps[]，
    使「容差内补齐/records 已覆盖」的残留缺口在收敛报告与覆盖率审计处仍然可见。"""
    rec = {'p': gap.get('p'), 'type': gap.get('type'), 'total': gap.get('total'),
           'covered': gap.get('covered'), 'got': gap.get('got'), 'gap': gap.get('gap'),
           'degraded': bool(gap.get('degraded')),
           'repair_attempts': int(gap.get('repair_attempts', 0) or 0),
           'resolved_reason': reason, 'resolved_ts': datetime.now().isoformat()}
    lst = prog.setdefault('resolved_gaps', [])
    lst.append(rec)
    if len(lst) > RESOLVED_GAPS_KEEP:
        del lst[:-RESOLVED_GAPS_KEEP]
    return rec


# ---------------------------------------------------------------------------
# F5：停滞判据（水位/进度年龄阈值 → stall_detected 审计；可选超阈值自停）
# ---------------------------------------------------------------------------
def progress_watermark(prog):
    """进度水位（任一维变化即算"有进展"）。

    只用**单调/语义明确**的量：已抓记录数（stats.records 单调增）、完成叶子数、
    done 账本长度、todo 长度（取负号：收缩=进展，分支展开=任务变多不算进展）、
    gaps / recovery 长度。刻意不用 todo 长度本身 —— 分支展开会让它变大。"""
    st = prog.get('stats') or {}
    try:
        return (int(st.get('records', 0) or 0),
                int(st.get('leaves', 0) or 0),
                len(prog.get('done') or []),
                -len(prog.get('todo') or []),
                len(prog.get('gaps') or []),
                len(prog.get('recovery') or []))
    except (TypeError, ValueError):
        return None


def stall_state(prog=None, now=None):
    """当前停滞读数（结构化，测试与哈希读数用）。"""
    now = now if now is not None else time.time()
    last = _PROGRESS_STATE.get('last_change_ts')
    age = None if last is None else max(0.0, now - last)
    return {'age_s': age, 'watermark': _PROGRESS_STATE.get('watermark'),
            'audited': bool(_PROGRESS_STATE.get('stall_audited')),
            'stopped': bool(_PROGRESS_STATE.get('stall_stop_fired')),
            'ticks': _PROGRESS_STATE.get('ticks', 0),
            'threshold_s': STALL_PROGRESS_AGE_S, 'stop_threshold_s': STALL_STOP_AGE_S,
            'action': STALL_ACTION, 'todo': len((prog or {}).get('todo') or [])}


def stall_tick(prog, where='periodic'):
    """停滞检查（幂等、永不抛异常）：水位变化 ⇒ 复位年龄；跨阈值 ⇒ stall_detected。

    默认动作 = 只审计（audit）；SCRAPE_STALL_ACTION=stop 时超过自停阈值执行
    graceful_stop（存 checkpoint + 审计 + exit EXIT_STALL_STOP）。"""
    _PROGRESS_STATE['ticks'] = _PROGRESS_STATE.get('ticks', 0) + 1
    now = time.time()
    wm = progress_watermark(prog)
    prev = _PROGRESS_STATE.get('watermark')
    if _PROGRESS_STATE.get('last_change_ts') is None or (wm is not None and wm != prev):
        _PROGRESS_STATE['watermark'] = wm
        _PROGRESS_STATE['last_change_ts'] = now
        if _PROGRESS_STATE.get('stall_audited') and prev is not None and wm != prev:
            _PROGRESS_STATE['stall_audited'] = False       # 恢复后重新武装
            audit_event('stall_cleared', 'block',
                        {'where': where, 'watermark': list(wm) if wm else None})
        return {'stalled': False, 'age_s': 0.0, 'action': None}
    age = now - _PROGRESS_STATE['last_change_ts']
    st = stall_state(prog, now)
    out = {'stalled': False, 'age_s': round(age, 3), 'action': None}
    if age >= STALL_PROGRESS_AGE_S and not _PROGRESS_STATE.get('stall_audited'):
        _PROGRESS_STATE['stall_audited'] = True
        detail = {'kind': 'progress_age', 'last_progress_age_s': round(age, 3),
                  'where': where, 'threshold_s': STALL_PROGRESS_AGE_S,
                  'action': STALL_ACTION, 'watermark': list(wm) if wm else None,
                  'todo': st['todo'], 'gaps': len(prog.get('gaps') or [])}
        audit_event('stall_detected', 'block', detail)
        log(f"    [r3] 停滞判据命中：进度年龄 {age:.1f}s ≥ {STALL_PROGRESS_AGE_S:.0f}s"
            f"（where={where}, todo={st['todo']}, action={STALL_ACTION}）")
        out = {'stalled': True, 'age_s': round(age, 3), 'action': 'audit'}
    if (STALL_ACTION == 'stop' and age >= STALL_STOP_AGE_S
            and not _PROGRESS_STATE.get('stall_stop_fired')):
        _PROGRESS_STATE['stall_stop_fired'] = True
        audit_event('stall_detected', 'block',
                    {'kind': 'progress_age_stop', 'last_progress_age_s': round(age, 3),
                     'where': where, 'threshold_s': STALL_STOP_AGE_S,
                     'action': 'stop', 'todo': st['todo']})
        graceful_stop(prog, signal_name='stall', exit_code=EXIT_STALL_STOP,
                      extra={'age_s': round(age, 3), 'where': where})
        out = {'stalled': True, 'age_s': round(age, 3), 'action': 'stop'}
    return out


def graceful_stop(prog, signal_name='stall', exit_code=EXIT_STALL_STOP, extra=None):
    """优雅自停：存 checkpoint（progress + 待写 records）→ 审计 → 退出。

    与「进程被杀」的区别：progress.json 与 records.jsonl 都已落盘且相互一致。"""
    try:
        rc_flush = flush_pending_records()
    except Exception:
        rc_flush = -1
    try:
        save_progress(prog)
    except Exception:
        pass
    d = {'signal': signal_name, 'todo': len(prog.get('todo') or []),
         'done': len(prog.get('done') or []), 'exit_code': exit_code,
         'pending_records': rc_flush if isinstance(rc_flush, int) else 'unknown'}
    if extra:
        d.update(extra)
    audit_event('graceful_stop', 'process', d)
    try:
        log(f"[r3] 优雅自停（signal={signal_name}）：checkpoint 已落盘，exit {exit_code} {d}")
    except Exception:
        pass
    try:
        sys.stdout.flush(); sys.stderr.flush()
    except Exception:
        pass
    os._exit(int(exit_code))


def _stall_monitor_loop(prog):
    stop = _STALL_THREAD.get('stop')
    while stop is not None and not stop.wait(STALL_CHECK_EVERY_S):
        try:
            stall_tick(prog, 'monitor')
        except Exception as e:
            try:
                log(f"[r3] 停滞监测异常（已忽略，监测继续）: {type(e).__name__}: {e}")
            except Exception:
                pass


def _stall_monitor_start(prog):
    """独立监测线程：即使全部工作线程阻塞在 fetch 里，停滞判据仍然生效。"""
    if not STALL_MONITOR_ENABLED:
        return None
    if _STALL_THREAD.get('t') is not None and _STALL_THREAD['t'].is_alive():
        return _STALL_THREAD['t']
    _STALL_THREAD['stop'] = threading.Event()
    t = threading.Thread(target=_stall_monitor_loop, args=(prog,),
                         name='r3-stall-monitor', daemon=True)
    _STALL_THREAD['t'] = t
    t.start()
    return t


def _stall_monitor_stop():
    ev = _STALL_THREAD.get('stop')
    if ev is not None:
        ev.set()
    t = _STALL_THREAD.get('t')
    if t is not None and t.is_alive():
        t.join(timeout=1.0)
    _STALL_THREAD['t'] = None


# ---------------------------------------------------------------------------
# F6/F1：收敛判据机器化（结构化报告 + 退出码 + convergence_decision 审计）
# ---------------------------------------------------------------------------
def open_gaps(prog):
    """未决缺口（resolved 标记为真的条目视为已消解）。"""
    return [g for g in (prog.get('gaps') or []) if not g.get('resolved')]


def convergence_decision(prog, round_num, fixed=0, remaining=0):
    """机器化收敛判据。返回 (decided, terminal, exit_code, reason, detail)。

    decided ∈ {'done','continue'}（与 R3 审计 schema 的枚举一致）；
    terminal=True 表示主循环可以结束，此时 exit_code 就是**机器结论**：
      0 = 收敛；EXIT_INCOMPLETE = 未收敛（todo 空但仍有未决缺口且修复预算耗尽）。"""
    todo = len(prog.get('todo') or [])
    gaps = open_gaps(prog)
    degraded = [g for g in gaps if g.get('degraded')]
    terminal_gaps = [g for g in gaps if g.get('terminal')]
    attempts = [int(g.get('repair_attempts', 0) or 0) for g in gaps] or [0]
    detail = {'todo': todo, 'gaps': len(gaps), 'degraded_gaps': len(degraded),
              'undetermined_gaps': sum(1 for g in gaps if g.get('type') == 'undetermined'),
              'terminal_gaps': len(terminal_gaps), 'repair_attempts_max': max(attempts),
              'repair_attempts_budget': GAP_REPAIR_MAX,
              'done': len(prog.get('done') or []),
              'records': int((prog.get('stats') or {}).get('records', 0) or 0),
              'probe_dead': len(prog.get('probe_dead') or []),
              'round': round_num, 'fixed_last_round': fixed,
              'remaining_last_round': remaining,
              'round_max': MAX_ROUNDS}
    if todo > 0:
        return 'continue', False, None, 'todo-pending', detail
    if not gaps:
        return 'done', True, EXIT_CONVERGED, 'no-todo-no-open-gaps', detail
    if fixed > 0:
        return 'continue', False, None, 'repair-progress', detail
    if len(terminal_gaps) == len(gaps):
        return 'continue', True, EXIT_INCOMPLETE, 'repair-budget-exhausted', detail
    return 'continue', False, None, 'repair-budget-left', detail


def write_convergence_report(prog, decided, terminal, exit_code, reason, detail, extra=None):
    """写 OUT/convergence-report.json（原子替换；失败只告警，不影响主流程）。"""
    global LAST_CONVERGENCE
    rep = {'schema': 'r3-convergence-v1', 'ts': round(time.time(), 3),
           'ts_iso': datetime.now().isoformat(timespec='milliseconds'),
           'pid': os.getpid(), 'run': _r3_run_id(),
           'decided': decided, 'terminal': bool(terminal),
           'exit_code': exit_code, 'reason': reason, 'detail': detail,
           'gaps': [{'p': g.get('p'), 'type': g.get('type'), 'total': g.get('total'),
                     'covered': g.get('covered'), 'got': g.get('got'), 'gap': g.get('gap'),
                     'degraded': bool(g.get('degraded')),
                     'repair_attempts': g.get('repair_attempts', 0),
                     'terminal': bool(g.get('terminal')),
                     'missing_chars': len(g.get('chars') or [])}
                    for g in (prog.get('gaps') or [])],
           'probe_dead': [{'p': e.get('p'), 'reason': e.get('reason'),
                           'attempts': e.get('attempts'),
                           'missing_chars': len(e.get('failed_chars') or [])}
                          for e in (prog.get('probe_dead') or [])][-50:],
           'stats': dict(prog.get('stats') or {}),
           'resolved_gaps': [{'p': g.get('p'), 'type': g.get('type'),
                              'total': g.get('total'), 'covered': g.get('covered'),
                              'got': g.get('got'), 'gap': g.get('gap'),
                              'degraded': bool(g.get('degraded')),
                              'repair_attempts': int(g.get('repair_attempts', 0) or 0),
                              'resolved_reason': g.get('resolved_reason'),
                              'resolved_ts': g.get('resolved_ts')}
                             for g in (prog.get('resolved_gaps') or [])],
           'undetermined': [{'p': e.get('p'), 'reason': e.get('reason'),
                             'where': e.get('where'), 'total': e.get('total'),
                             'attempts': e.get('attempts')}
                            for e in (prog.get('undetermined') or [])][-50:],
           'audit_counts': audit_counts(),
           'audit_dropped': _AUDIT_DROPPED,
           'records_deferred': _RECORDS_DEFERRED,
           'recovery_tail': (prog.get('recovery') or [])[-10:]}
    if extra:
        rep.update(extra)
    LAST_CONVERGENCE = rep
    blob = (json.dumps(rep, ensure_ascii=False, indent=1) + '\n').encode('utf-8')
    tmp = CONV_REPORT_FILE.with_name(f"convergence-report.json.tmp.{os.getpid()}")
    try:
        fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            _write_all(fd, blob)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(str(tmp), str(CONV_REPORT_FILE))
    except OSError as e:
        ensure_out_dir('convergence-report')
        try:
            fd = os.open(str(tmp), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
            try:
                _write_all(fd, blob)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.replace(str(tmp), str(CONV_REPORT_FILE))
        except OSError as e2:
            audit_event('convergence_report_failed', 'process',
                        {'err': f'{type(e2).__name__}: {e2}',
                         'first_err': f'{type(e).__name__}: {e}'})
    return rep


def recheck_chars(parent, chars, gap_target, max_chars=15, miss_limit=5, pause=0.5):
    """v9: 每字符的双排序查询并行（去字符内 pause）。返回发现的 (sub,total)。

    R3·slot-converge (F2)：get_total 换成容错包装 _safe_get_total ⇒
    HTTP 200 缺 info 的 KeyError 记为「未定论」（不计命中、不打断复验循环）。"""
    found = []
    misses = 0
    for c in chars:
        if len(found) >= max_chars or gap_target <= 0:
            break
        if misses >= miss_limit:
            break
        sub = parent + c
        with ThreadPoolExecutor(max_workers=2) as ex:
            f1 = ex.submit(_safe_get_total, sub, None, 'recheck')
            f2 = ex.submit(_safe_get_total, sub, "title", 'recheck')
            try:
                t1 = f1.result(); t2 = f2.result()
            except Exception as e:                   # 池级异常兜底（双保险）
                _audit_exc(e, 'recheck.pool', sub)
                t1 = t2 = None
        t = max(t1 or 0, t2 or 0)
        if t > 0:
            found.append((sub, t))
            gap_target -= t
            misses = 0
            log(f"      复验命中 {sub!r}: rank={t1} title={t2}")
        else:
            misses += 1
        time.sleep(pause)
    return found

# ===========================================================================
# R2 · P4-C —— 项级并发调度（预算 / 工作队列 / 条目运行器）
#
# 串行基线：`while todo: item = todo[0]; …一次只推进一个 item…`
# 并发版：c 个 item runner 共享同一队列与**同一个全局 LIMITER**，in-flight 重叠推进。
#
# 四条不变量（与基线逐条对应，验收按此断言）：
#   I1 所有请求仍经 LIMITER.acquire()：并发只改「谁来发」，不改「能不能发」，
#      也不改放行间隔（RateLimiter 实现逐字未改）。
#   I2 冷却仍是**全局**闸门（LIMITER.cooldown 语义不变）：并发下任何一处冷却冻结
#      进程内全部请求 —— 这是 B3 的安全语义，不因并发而放松。
#   I3 同一 prefix 同一时刻只被一个 runner 认领（否则同前缀重复抓取 + done 重复记账）。
#   I4 prog['todo'] / done / gaps / stats / recovery 的全部变更与 save_progress()
#      在同一把锁下完成 ⇒ 磁盘上的 progress.json 永远是一个自洽快照（v10 原子写语义不变）。
#
# 冷却 × 并发的数学（审计 §2.4 的「c 倍放大」）：
#   基线写作 `LIMITER.cooldown(X); time.sleep(X)`。全局闸门本身只冻结 X 秒墙钟，
#   但 sleep 让**触发冷却的那个 runner**在整个 X 内占着工作槽：c=3 时相当于
#   c·X 个工作槽秒被吃掉，闸门解除后 c 个请求还要在 LIMITER 上排队
#   ((c-1)×MIN_INTERVAL 的尾部放大)，池的有效并发度在这段时间跌到 0。
#   本实现的缓解 = **条目级延迟重放**（CONC_DEFER_SLOTS=1）：
#     · 冷却照旧全局生效（I2 不破，B3 不受影响）；
#     · 触发冷却的条目带 next_ts=X 回队尾并**立即交还工作槽** ⇒ 冷却窗口内其它
#       ready 条目（含零请求的反查闸门条目、已完成请求只差收尾的分支条目）可继续推进；
#     · 失败聚集时多个条目共享同一次全局冷却（ΣX → max X），串行基线只能 ΣX。
#   CONC_DEFER_SLOTS=0 可退回「占槽睡觉」的基线形态，用于 A/B 量化该项缓解的收益。
# ===========================================================================

_CONC_LOG_LOCK = threading.Lock()
_CONC_LAST_LOG = 0.0


def _conc_observe_limiter():
    """只读观测：LIMITER 的冷却截止时间是否被**外部**推进过（fetch 的 4xx/5xx/连接异常、
    probe_children 的探测失败）。不包装、不修改 RateLimiter 实现，只读一个 float 属性；
    「自伤冷却」（本模块的延迟重放）按 _SELF_COOLDOWN_UNTIL 排除。返回 True = 一次新错误信号。"""
    global _CONC_LAST_EXT_COOLDOWN, _CONC_ERR_EVENTS
    d = float(getattr(LIMITER, 'cooldown_until', 0.0) or 0.0)
    if d <= _CONC_LAST_EXT_COOLDOWN + 1e-6:
        return False
    _CONC_LAST_EXT_COOLDOWN = d
    if d <= _SELF_COOLDOWN_UNTIL + 1e-6:
        return False                    # 自伤冷却（延迟重放）不是错误信号
    _CONC_ERR_EVENTS += 1
    return True


class _ConcurrencyBudget:
    """显式并发预算：允许同时在飞的 item 数（= Crawlee desiredConcurrency 的等价物）。

    · scaleDown：错误信号到达时立即 −1（下限 W_MIN），**不打断在飞条目**
      （对齐 Crawlee pause() 的「停新不打断在飞」语义）；
    · scaleUp：距最后一次错误信号 ≥ W_SCALE_UP_AFTER_S 且未到上限 → +1
      （对齐 Crawlee scaleUp/scaleDown + autoscaleIntervalSecs）；
    · 与 LIMITER 正交：预算管「同时推进几个 item」，LIMITER 管「请求间隔」，
      两者都不可绕过。w=1 时行为等价于串行（回滚形态）。
    """

    def __init__(self, w_max, w_min):
        self.w_max = max(1, int(w_max))
        self.w_min = max(1, min(int(w_min), self.w_max))
        self.w = self.w_max
        self.inflight = 0
        self.peak_inflight = 0
        self.last_error_ts = 0.0
        self.events = []
        self._cv = threading.Condition()

    def try_enter(self):
        with self._cv:
            if self.inflight >= self.w:
                return False
            self.inflight += 1
            if self.inflight > self.peak_inflight:
                self.peak_inflight = self.inflight
            return True

    def exit(self):
        with self._cv:
            if self.inflight > 0:
                self.inflight -= 1
            self._cv.notify_all()

    def note_error(self, kind):
        with self._cv:
            self.last_error_ts = time.time()
            if not CONC_ADAPTIVE or self.w <= self.w_min:
                return False
            self.w -= 1
            self.events.append({'ts': round(time.time(), 3), 'action': 'scaleDown',
                                'w': self.w, 'kind': kind, 'err_events': _CONC_ERR_EVENTS})
            self._cv.notify_all()
            return True

    def maybe_scale_up(self):
        with self._cv:
            if not CONC_ADAPTIVE or self.w >= self.w_max:
                return False
            if time.time() - self.last_error_ts < W_SCALE_UP_AFTER_S:
                return False
            self.w += 1
            self.events.append({'ts': round(time.time(), 3), 'action': 'scaleUp',
                                'w': self.w, 'kind': 'quiet-window',
                                'err_events': _CONC_ERR_EVENTS})
            self._cv.notify_all()
            return True

    def wait(self, timeout):
        with self._cv:
            self._cv.wait(timeout)

    def snapshot(self):
        with self._cv:
            return {'w': self.w, 'w_max': self.w_max, 'w_min': self.w_min,
                    'inflight': self.inflight, 'peak_inflight': self.peak_inflight,
                    'err_events': _CONC_ERR_EVENTS, 'scale_events': list(self.events)}


class _WorkQueue:
    """prog['todo'] 的并发调度视图（直接操作同一个 list 对象，不复制）。

    · lock 同时保护「prog 的全部变更」与 save_progress() ⇒ progress.json 快照自洽（I4），
      因此**不需要改动 save_progress 本体**；
    · inflight = 已认领 prefix 集合（I3）；
    · ready 判定：前缀未被认领 且 item['next_ts'] 已到期；按队列顺序取最早者
      （保持基线的 FIFO 倾向，不引入优先级反转）。
    """

    def __init__(self, prog):
        self.prog = prog
        self.todo = prog['todo']
        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        self.inflight = set()
        # omni-patch(2026-09-27): 认领窗口崩溃快照 —— prefix -> item（落盘见 take/save）
        self.inflight_items = {}
        self.deferred_events = 0

    def take(self):
        with self.cv:
            now = time.time()
            for i, it in enumerate(self.todo):
                p = it.get('p')
                if p in self.inflight or it.get('next_ts', 0.0) > now:
                    continue
                self.todo.pop(i)
                self.inflight.add(p)
                # omni-patch: 认领即写 inflight 快照并落盘 —— 进程在"条目处理中"任何
                # 时刻被杀（kill -9/断电），重启时凭快照回队，杜绝"条目无痕丢失→假收敛"。
                self.inflight_items[p] = it
                self.prog['_inflight_snapshot'] = [dict(x) for x in self.inflight_items.values()]
                save_progress(self.prog)
                return it
            return None

    def finish(self, item):
        with self.cv:
            self.inflight.discard(item.get('p'))
            self.inflight_items.pop(item.get('p'), None)
            self.prog['_inflight_snapshot'] = [dict(x) for x in self.inflight_items.values()]
            self.cv.notify_all()

    def put_tail(self, item, delay=0.0):
        """回队尾（= 基线 `todo.append(todo.pop(0))`），delay>0 时带 next_ts 延迟重放。"""
        with self.cv:
            if delay > 0:
                item['next_ts'] = time.time() + delay
                self.deferred_events += 1
            self.todo.append(item)
            self.cv.notify_all()

    def put_head(self, item):
        """插入队首（= 基线 `todo.insert(0, …)` 的子块入队）。"""
        with self.cv:
            self.todo.insert(0, item)
            self.cv.notify_all()

    def save(self, ctx=None):
        """持锁保存 ⇒ json.dumps 序列化到的是完整快照（I4）。RLock 可重入。
        omni-patch: 保存前同步 inflight 快照字段。"""
        with self.lock:
            self.prog['_inflight_snapshot'] = [dict(x) for x in self.inflight_items.values()]
            save_progress(self.prog, ctx)

    def idle(self):
        with self.cv:
            return (not self.todo) and (not self.inflight)

    def wait_change(self, timeout):
        with self.cv:
            self.cv.wait(timeout)

    def next_wake(self):
        now = time.time()
        with self.cv:
            pend = [it.get('next_ts', 0.0) for it in self.todo
                    if it.get('next_ts', 0.0) > now]
        return min(pend) if pend else None

    def counts(self):
        now = time.time()
        with self.cv:
            ready = deferred = 0
            for it in self.todo:
                if it.get('next_ts', 0.0) > now:
                    deferred += 1
                elif it.get('p') not in self.inflight:
                    ready += 1
            return {'todo': len(self.todo), 'ready': ready, 'deferred': deferred,
                    'inflight': len(self.inflight)}


def _conc_defer(q, item, delay, ctx):
    """条目级延迟重放：基线的 `LIMITER.cooldown(X); time.sleep(X)`，但冷却期不占用工作槽。

    冷却仍**全局**生效（I2，B3 安全语义不变），条目带 next_ts=X 回队尾并立即交还工作槽；
    CONC_DEFER_SLOTS=0 时退回基线形态（sleep 占槽），供 A/B 量化。"""
    global _SELF_COOLDOWN_UNTIL
    LIMITER.cooldown(delay)
    _SELF_COOLDOWN_UNTIL = max(_SELF_COOLDOWN_UNTIL, time.time() + delay)
    if CONC_DEFER_SLOTS:
        q.put_tail(item, delay)
        q.save(ctx)
    else:
        time.sleep(delay)
        q.put_tail(item, 0.0)
        q.save(ctx)


def _conc_log_periodic(q, budget, wid):
    global _CONC_LAST_LOG
    try:                                   # R3·F5：并发路径也采样进度水位（监测线程之外的补充）
        stall_tick(q.prog, 'conc-periodic')
    except Exception:
        pass
    now = time.time()
    with _CONC_LOG_LOCK:
        if now - _CONC_LAST_LOG < W_LOG_EVERY_S:
            return
        _CONC_LAST_LOG = now
    snap = budget.snapshot()
    c = q.counts()
    log(f"[conc] w={snap['w']}/{snap['w_max']} inflight={snap['inflight']}/{snap['peak_inflight']} "
        f"todo={c['todo']} ready={c['ready']} deferred={c['deferred']} "
        f"err_events={snap['err_events']} dur={len(snap['scale_events'])} peak_w={snap['w_max']} wid={wid}")


def _conc_run_item(prog, q, item, wid):
    """处理一个队列条目 = 基线主循环体的逐行语义（差异仅限 §P4-C 标注的三点）。

    条目状态机（新增字段全部可 JSON 往返，进程重启后仍可用）：
      item['next_ts']     延迟重放到期时刻（冷却期不占工作槽）
      item['retry']       叶子重试次数（基线已有）
      item['stage']       'recheck' = 分支「先抓 ∥ 探测」已完成、等冷却后当场复验
      item['kids'/'zero_chars'/'covered'/'pre'/'t0']  跨阶段携带的分支中间量
    条目在 take() 时已出队（= 基线的 todo.pop(0)），因此对已认领条目的原地修改
    不需要持锁（不在队列里，不会被 save_progress 序列化到）。
    """
    prefix = item['p']
    total = item.get('total')

    # ---- 1) total 解析（基线：total is None / total == 0）----
    if total is None:
        # R3p4·P4-A（C-1/C-2/C-3）：与串行路径**同路由**。
        #   · 用 _safe_get_total（F2 的真实全覆盖）：200-缺-info 的 KeyError 记 error_classified
        #     并返回 None，不再穿到 _conc_worker 的条目异常路径（此前正是活锁入口）；
        #   · 未定论 ⇒ 预算内重试轮 / 预算耗尽 ⇒ 降级记账（登记缺口 + 审计 + 进修复轮）。
        t = _safe_get_total(prefix, None, 'conc-item-total')
        if t is None:
            if int(item.get('total_none', 0) or 0) < TOTAL_NONE_RETRY_MAX:
                _undetermined_retry(prog, item, prefix, 'conc-item-total')
                _conc_defer(q, item, PROBE_DEAD_DELAY_S, prefix)
                return
            with q.lock:                       # prog 变更与 save 同锁（I4 快照自洽）
                _undetermined_degrade(prog, item, prefix, None, 'total_none',
                                      'conc-item-total')
                q.save(prefix)
            return
        total = t
        item['total'] = t
        q.save(prefix)

    if total == 0:
        q.save(prefix)
        return

    # ---- 2) 叶子（基线 L922-951）----
    if total <= LEAF_MAX:
        if is_recorded_done(prefix, total):     # v10: records 反查闸门（不重复抓取）
            cnt = recorded_count(prefix)
            log(f"叶子 {prefix!r} total={total} 跳过（records 反查已含 {cnt} 条互异 mms）")
            with q.lock:
                _tolerance_mark(prog, prefix, cnt, total, '叶子闸门 skip->done')
                mark_leaf_done(prog, prefix, total, cnt, recovered=True)
                _recovery_note(prog, 'skip->done', prefix,
                               f'records 已含 {cnt}/{total}，跳过重复抓取')
                q.save(prefix)
            return
        ok, got = scrape_leaf(prefix, total, deep_retry=True)
        shortfall = total - got
        tol = max(2, int(total * 0.015))
        if shortfall > tol and item.get('retry', 0) < LEAF_RETRY_MAX:
            item['retry'] = item.get('retry', 0) + 1
            log(f"叶子 {prefix!r} 缺 {shortfall}/{total}，延迟重试 #{item['retry']}")
            # 基线：todo 回队尾 + save + cooldown(45) + sleep(45)
            _conc_defer(q, item, LEAF_RETRY_DELAY_S, prefix)
            return
        note = "" if shortfall <= tol else f"  <<< 缺口 {shortfall}"
        if shortfall > 0:
            note += f" [差{shortfall}]"
        log(f"叶子 {prefix!r} total={total} 唯一={got}{note}")
        with q.lock:
            mark_leaf_done(prog, prefix, total, got, recovered=False)
            if shortfall > tol:
                prog['gaps'].append({'p': prefix, 'total': total, 'got': got, 'type': 'leaf'})
            q.save(prefix)
        return

    # ---- 3) 分支（基线 L952-1012）----
    t0 = item.get('t0') or time.time()
    item['t0'] = t0
    degraded_gap = (_find_degraded_gap(prog, prefix) if item.get('degraded') else None)
    # ↑ R3·C2：本条目已被降级过 ⇒ 复用已登记缺口（当场复验 pass 不再重复登记）
    if item.get('stage') == 'recheck':
        # 第二段：冷却窗口已过（期间工作槽已交还），执行「大缺口当场复验」（基线 L983-989）
        kids = item.get('kids') or []
        zero_chars = item.get('zero_chars') or []
        covered = item.get('covered') or 0
        gap = total - covered
        found = recheck_chars(prefix, zero_chars, gap)
        for sub, t in found:
            if sub not in [k for k, _ in kids]:
                kids.append((sub, t))
                covered += t
        gap = total - covered
    else:
        # 第一段：先抓一笔（数据保险）＋ 细分探测 —— 并行执行（基线 L953-979）
        skip_pre = (prefix not in FORCE_REQUEUE) and \
            recorded_count(prefix) >= total - leaf_tol(total)   # v10: 反查已够则免重复
        if skip_pre:
            with q.lock:
                _tolerance_mark(prog, prefix, recorded_count(prefix), total, '分支先抓跳过')
            log(f"    先抓 {prefix!r}: 跳过（records 反查已含 {recorded_count(prefix)}/{total}）")
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_pre = None if skip_pre else ex.submit(scrape_leaf, prefix, total, False)
            f_probe = ex.submit(probe_children, prefix)
            if f_pre is None:
                ok, pre = True, recorded_count(prefix)
            else:
                try:
                    ok, pre = f_pre.result()
                except Exception as e:
                    log(f"    先抓异常 {prefix!r}: {e}")
                    ok, pre = False, 0
            try:
                probe_out = f_probe.result()            # R3: ProbeOutcome（仍是 2 元组）
                kids, zero_chars = probe_out
            except Exception as e:                      # R3·C2: 探测整体异常 ⇒ 未定论，不丢子树
                log(f"    探测异常 {prefix!r}: {e}")
                probe_out = _probe_outcome_from_exc(prefix, e)
                kids, zero_chars = probe_out
        if pre > 0:
            log(f"    先抓 {prefix!r}: {pre} 条落盘")
        # ---- R3·C2/F1：块级兜底阶梯（探测全灭/未定论 ⇒ 重试轮 → 降级 + 修复轮）----
        pol = _probe_dead_policy(item, probe_out)
        if pol['action'] == 'retry':
            item['probe_dead'] = pol['retry_n']
            log(f"    [r3] 探测未定论 {len(pol['failed_chars'])} 字符（{pol['reason']}）"
                f"→ 父块重试轮 #{pol['retry_n']}/{PROBE_DEAD_RETRY_MAX}，"
                f"{pol['delay_s']:.0f}s 后重排（不丢子树）")
            audit_event('probe_retry', 'block',
                        {'prefix': prefix, 'reason': pol['reason'],
                         'attempt': pol['retry_n'], 'max_attempts': PROBE_DEAD_RETRY_MAX,
                         'missing_chars': pol['failed_chars'], 'delay_s': pol['delay_s'],
                         'todo_snapshot': len(prog.get('todo') or [])})
            _conc_defer(q, item, pol['delay_s'], prefix)
            return
        if pol['action'] == 'degrade':
            degraded_gap = _probe_dead_degrade(prog, item, prefix, total, probe_out, pre, kids)
            item['degraded'] = True          # 标记：后续 pass（当场复验）就地更新，不再新增缺口
        covered = sum(t for _, t in kids)
        gap = total - covered
        # 大缺口 → 当场复验。基线在此 cooldown(20)+sleep(20)（占槽）；
        # 并发版改为条目级延迟重放（stage='recheck'）：冷却仍全局生效（I2），工作槽立即交还。
        if gap > max(GAP_TRIGGER, int(total * 0.02)):
            log(f"    缺口 {gap}（大），冷却 {BRANCH_RECHECK_DELAY_S:.0f}s 后当场复验 "
                f"{len(zero_chars)} 字符")
            item['stage'] = 'recheck'
            item['kids'] = kids
            item['zero_chars'] = zero_chars
            item['covered'] = covered
            item['pre'] = pre
            _conc_defer(q, item, BRANCH_RECHECK_DELAY_S, prefix)
            return

    # ---- 4) 公共尾部：日志 / 缺口记账 / 子块入队（基线 L990-1012）----
    dt = time.time() - t0
    kid_info = ", ".join(f"{s}:{t}" for s, t in sorted(kids)[:14])
    log(f"细分 {prefix!r} (total={total}) -> {len(kids)}子块 覆盖{covered} 缺口{gap} "
        f"[{dt:.0f}s] {kid_info}")
    item['children'] = [s for s, t in kids]
    item['covered'] = covered
    item['gap'] = gap
    item.pop('stage', None)
    with q.lock:
        if gap > max(GAP_TRIGGER, int(total * 0.02)):
            if degraded_gap is not None:
                # R3·C2：降级缺口已在 _probe_dead_degrade 登记（含 failed_chars 与预算计数）；
                # 当场复验后的覆盖数就地更新，避免同一 prefix 出现两条缺口（重复计数）。
                degraded_gap['covered'] = covered
                degraded_gap['gap'] = gap
            else:
                prog['gaps'].append({'p': prefix, 'total': total, 'covered': covered,
                                     'gap': gap, 'chars': zero_chars, 'type': 'branch',
                                     'repair_attempts': 0})
        skipped_kids = 0
        for s, t in reversed(kids):
            if is_recorded_done(s, t):        # v10: 该子叶已落盘 → 不入队，避免重复抓取
                skipped_kids += 1
                _tolerance_mark(prog, s, recorded_count(s), t, '子块跳过')   # v10-fix: 容差留痕
                log(f"    子块 {s!r} total={t} 已在 records 中完整 → 不入队"
                    f"（{recorded_count(s)} 条互异 mms）")
                continue
            q.put_head({'p': s, 'total': t})
        if skipped_kids:
            _recovery_note(prog, 'kids-skipped', prefix,
                           f'{skipped_kids} 个子块已由 records 反查证明完整，未入队')
        q.save(prefix)


def _conc_worker(prog, q, budget, wid):
    """一个 item runner：预算许可 → 取 ready 条目 → 执行 → 交还许可（+ 观测/自适应）。"""
    while True:
        if not budget.try_enter():
            budget.wait(0.2)
            budget.maybe_scale_up()
            _conc_log_periodic(q, budget, wid)
            continue
        item = None
        try:
            item = q.take()
            if item is None:
                if q.idle():
                    break                       # 队列空且无在飞 ⇒ 收敛退出
                nxt = q.next_wake()
                timeout = 0.2 if nxt is None else max(0.02, min(0.5, nxt - time.time()))
                q.wait_change(timeout)
                continue
            try:
                _conc_run_item(prog, q, item, wid)
            except Exception as e:
                # R3p4·P4-A（C-2）：条目异常**必须有尝试预算** —— 无预算的回队尾在
                # 「注入恒定」时是活锁（实测 25s 内 25 次、进程永不退出、永不写收敛裁决）。
                #   预算内：分类审计 + retry_scheduled + 延迟回队尾；
                #   预算耗尽：error_classified + retry_exhausted + 降级记账（登记缺口进修复轮）
                #             ⇒ 收敛判据判「未收敛」exit 3，进程有界退出。
                prefix = item.get('p')
                cls = _audit_exc(e, 'conc-item', prefix)          # request 级 error_classified
                n = int(item.get('exc_retry', 0) or 0) + 1
                item['exc_retry'] = n
                if n <= ITEM_EXC_RETRY_MAX:
                    audit_event('retry_scheduled', 'request',
                                {'where': 'conc-item', 'prefix': prefix, 'attempt': n,
                                 'attempts_max': ITEM_EXC_RETRY_MAX, 'class': cls,
                                 'wait_s': ITEM_EXC_DELAY_S, 'q': prefix,
                                 'status_or_exc': f'{type(e).__name__}: {e}'})
                    log(f"    [conc] 条目异常 {prefix!r}: {type(e).__name__}: {e} "
                        f"→ 回队尾（{n}/{ITEM_EXC_RETRY_MAX}）")
                    q.put_tail(item, ITEM_EXC_DELAY_S)
                else:
                    audit_event('retry_exhausted', 'request',
                                {'where': 'conc-item', 'prefix': prefix, 'attempts': n,
                                 'budget': ITEM_EXC_RETRY_MAX, 'class': cls,
                                 'q': prefix, 'last_error': f'{type(e).__name__}: {e}'})
                    log(f"    [conc] 条目异常预算耗尽 {prefix!r}（{n}/{ITEM_EXC_RETRY_MAX}）："
                        f"{type(e).__name__}: {e} → 降级记账（不再无限回队尾）")
                    with q.lock:
                        _undetermined_degrade(prog, item, prefix, item.get('total'),
                                              'item_exc_budget_exhausted', 'conc-item',
                                              extra={'class': cls, 'attempts': n,
                                                     'budget': ITEM_EXC_RETRY_MAX,
                                                     'exc': f'{type(e).__name__}: {e}'})
                        q.save(prefix)
        finally:
            if item is not None:
                q.finish(item)
            budget.exit()
        try:    # 观测/自适应不得因异常打死 runner（丢工作 = 不可接受）
            if _conc_observe_limiter() and budget.note_error('external-cooldown'):
                log(f"[conc] scaleDown → w={budget.w}（外部冷却/错误信号 #{_CONC_ERR_EVENTS}）")
            budget.maybe_scale_up()
            _conc_log_periodic(q, budget, wid)
        except Exception as e:
            log(f"[conc] 观测/自适应异常（已忽略，runner 继续）: {type(e).__name__}: {e}")


def process_queue(prog):
    """项级并发主循环（R2·P4-C）。与 v10.1 串行版逐条对应，差异只有三处：
       (1) 多个 item 在飞（并发预算 W_MAX）；
       (2) 冷却期不占用工作槽（条目级延迟重放，全局冷却语义不变）；
       (3) 条目内 progress 变更与 save_progress 同锁（快照自洽）。
    SCRAPE_CONC=0（或 W_MAX<=1）时走 `_serial_process_queue` 基线形态（回滚）。"""
    if not CONC_ENABLED or W_MAX <= 1:
        return _serial_process_queue(prog)
    q = _WorkQueue(prog)
    budget = _ConcurrencyBudget(W_MAX, W_MIN)
    log(f"[conc] 项级并发启动：w_max={W_MAX} w_min={W_MIN} adaptive={CONC_ADAPTIVE} "
        f"defer_slots={CONC_DEFER_SLOTS} leaf_retry={LEAF_RETRY_DELAY_S:.0f}s "
        f"branch_recheck={BRANCH_RECHECK_DELAY_S:.0f}s todo={len(q.todo)}")
    threads = [threading.Thread(target=_conc_worker, args=(prog, q, budget, i),
                                name=f"item-runner-{i}", daemon=True)
               for i in range(W_MAX)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if any(t.is_alive() for t in threads):          # 不应发生（daemon 线程，防御性告警）
        log("[conc] 警告：仍有 runner 未退出")
    snap = budget.snapshot()
    ev = snap['scale_events']
    log(f"[conc] 项级并发结束：peak_inflight={snap['peak_inflight']} err_events={snap['err_events']} "
        f"dur={len(ev)} 末态 w={snap['w']} deferred={q.deferred_events} "
        f"scale_events={ev}")


def _serial_process_queue(prog):
    """基线串行主循环（v10.1 逐行保留）：SCRAPE_CONC=0 时使用的回滚/对照形态。

    R3·slot-converge 在本路径的改动与并发路径**逐条对应**：
      (1) total 解析用 _safe_get_total（F2）；(2) 块级兜底阶梯（C2/F1）；
      (3) 停滞水位采样（F5）。"""
    todo = prog['todo']
    while todo:
        try:                                   # R3·F5：串行路径也采样进度水位
            stall_tick(prog, 'serial')
        except Exception:
            pass
        item = todo[0]
        prefix = item['p']
        total = item.get('total')
        if total is None:
            t = _safe_get_total(prefix, None, 'serial-item-total')      # R3·F2
            if t is None:
                # R3p4·P4-A（C-1/C-3）：**绝不静默 `todo.pop(0)` 跳过** —— 那会把
                # 「条目 total 解析失败」变成 rc=0 + 0 条记录 + 「全部完成」+ gaps=0 的
                # 假收敛（相对基线的 rc=1 崩溃是失败语义倒退）。改为与并发路径同路由：
                # 预算内重试轮 / 预算耗尽 ⇒ 降级记账（登记缺口 + 审计 + 进修复轮）。
                if int(item.get('total_none', 0) or 0) < TOTAL_NONE_RETRY_MAX:
                    _undetermined_retry(prog, item, prefix, 'serial-item-total')
                    todo.append(todo.pop(0))                  # = 基线回队尾
                    save_progress(prog, prefix)
                    LIMITER.cooldown(PROBE_DEAD_DELAY_S); time.sleep(PROBE_DEAD_DELAY_S)
                    continue
                _undetermined_degrade(prog, item, prefix, None, 'total_none',
                                      'serial-item-total')
                todo.pop(0); save_progress(prog, prefix); continue
            total = t
            item['total'] = t
            save_progress(prog, prefix)

        if total == 0:
            todo.pop(0); save_progress(prog, prefix); continue

        if total <= LEAF_MAX:
            if is_recorded_done(prefix, total):     # v10: records 反查闸门（不重复抓取）
                cnt = recorded_count(prefix)
                log(f"叶子 {prefix!r} total={total} 跳过（records 反查已含 {cnt} 条互异 mms）")
                _tolerance_mark(prog, prefix, cnt, total, '叶子闸门 skip->done')   # v10-fix: 容差留痕
                mark_leaf_done(prog, prefix, total, cnt, recovered=True)
                _recovery_note(prog, 'skip->done', prefix,
                               f'records 已含 {cnt}/{total}，跳过重复抓取')
                todo.pop(0)
                save_progress(prog, prefix)
                continue
            ok, got = scrape_leaf(prefix, total, deep_retry=True)
            shortfall = total - got
            tol = max(2, int(total * 0.015))
            if shortfall > tol and item.get('retry', 0) < LEAF_RETRY_MAX:
                item['retry'] = item.get('retry', 0) + 1
                log(f"叶子 {prefix!r} 缺 {shortfall}/{total}，延迟重试 #{item['retry']}")
                todo.append(todo.pop(0))
                save_progress(prog, prefix)
                LIMITER.cooldown(45); time.sleep(45)
                continue
            note = "" if shortfall <= tol else f"  <<< 缺口 {shortfall}"
            if shortfall > 0:
                note += f" [差{shortfall}]"
            log(f"叶子 {prefix!r} total={total} 唯一={got}{note}")
            mark_leaf_done(prog, prefix, total, got, recovered=False)
            if shortfall > tol:
                prog['gaps'].append({'p': prefix, 'total': total, 'got': got, 'type': 'leaf'})
            todo.pop(0)
            save_progress(prog, prefix)
        else:
            # 1) 先抓一笔（数据保险）＋ 2) 细分探测  —— v9: 并行执行
            t0 = time.time()
            skip_pre = (prefix not in FORCE_REQUEUE) and \
                recorded_count(prefix) >= total - leaf_tol(total)   # v10: 反查已够则免重复
            if skip_pre:
                _tolerance_mark(prog, prefix, recorded_count(prefix), total, '分支先抓跳过')
                log(f"    先抓 {prefix!r}: 跳过（records 反查已含 {recorded_count(prefix)}/{total}）")
            with ThreadPoolExecutor(max_workers=2) as ex:
                f_pre = None if skip_pre else ex.submit(scrape_leaf, prefix, total, False)
                f_probe = ex.submit(probe_children, prefix)
                if f_pre is None:
                    ok, pre = True, recorded_count(prefix)
                else:
                    try:
                        ok, pre = f_pre.result()
                    except Exception as e:
                        log(f"    先抓异常 {prefix!r}: {e}")
                        ok, pre = False, 0
                try:
                    probe_out = f_probe.result()        # R3: ProbeOutcome（仍是 2 元组）
                    kids, zero_chars = probe_out
                except Exception as e:                  # R3·C2: 探测整体异常 ⇒ 未定论，不丢子树
                    log(f"    探测异常 {prefix!r}: {e}")
                    probe_out = _probe_outcome_from_exc(prefix, e)
                    kids, zero_chars = probe_out
            if pre > 0:
                log(f"    先抓 {prefix!r}: {pre} 条落盘")
            # ---- R3·C2/F1：块级兜底阶梯（探测全灭/未定论 ⇒ 重试轮 → 降级 + 修复轮）----
            pol = _probe_dead_policy(item, probe_out)
            if pol['action'] == 'retry':
                item['probe_dead'] = pol['retry_n']
                log(f"    [r3] 探测未定论 {len(pol['failed_chars'])} 字符（{pol['reason']}）"
                    f"→ 父块重试轮 #{pol['retry_n']}/{PROBE_DEAD_RETRY_MAX}，"
                    f"{pol['delay_s']:.0f}s 后重排（不丢子树）")
                audit_event('probe_retry', 'block',
                            {'prefix': prefix, 'reason': pol['reason'],
                             'attempt': pol['retry_n'], 'max_attempts': PROBE_DEAD_RETRY_MAX,
                             'missing_chars': pol['failed_chars'], 'delay_s': pol['delay_s'],
                             'todo_snapshot': len(prog.get('todo') or [])})
                todo.append(todo.pop(0))            # = 基线回队尾
                save_progress(prog, prefix)
                LIMITER.cooldown(pol['delay_s']); time.sleep(pol['delay_s'])
                continue
            degraded_gap = None
            if pol['action'] == 'degrade':
                degraded_gap = _probe_dead_degrade(prog, item, prefix, total, probe_out, pre, kids)
                item['degraded'] = True
            covered = sum(t for _, t in kids)
            gap = total - covered
            # 3) 大缺口 → 当场复验
            if gap > max(GAP_TRIGGER, int(total * 0.02)):
                log(f"    缺口 {gap}（大），冷却 20s 后当场复验 {len(zero_chars)} 字符")
                LIMITER.cooldown(20); time.sleep(20)
                found = recheck_chars(prefix, zero_chars, gap)
                for sub, t in found:
                    if sub not in [k for k, _ in kids]:
                        kids.append((sub, t))
                        covered += t
                gap = total - covered
            dt = time.time() - t0
            kid_info = ", ".join(f"{s}:{t}" for s, t in sorted(kids)[:14])
            log(f"细分 {prefix!r} (total={total}) -> {len(kids)}子块 覆盖{covered} 缺口{gap} [{dt:.0f}s] {kid_info}")
            item['children'] = [s for s, t in kids]
            item['covered'] = covered
            item['gap'] = gap
            if gap > max(GAP_TRIGGER, int(total * 0.02)):
                if degraded_gap is not None:
                    degraded_gap['covered'] = covered       # R3·C2：就地更新，不重复登记
                    degraded_gap['gap'] = gap
                else:
                    prog['gaps'].append({'p': prefix, 'total': total, 'covered': covered,
                                         'gap': gap, 'chars': zero_chars, 'type': 'branch',
                                         'repair_attempts': 0})
            todo.pop(0)
            skipped_kids = 0
            for s, t in reversed(kids):
                if is_recorded_done(s, t):        # v10: 该子叶已落盘 → 不入队，避免重复抓取
                    skipped_kids += 1
                    _tolerance_mark(prog, s, recorded_count(s), t, '子块跳过')   # v10-fix: 容差留痕
                    log(f"    子块 {s!r} total={t} 已在 records 中完整 → 不入队"
                        f"（{recorded_count(s)} 条互异 mms）")
                    continue
                todo.insert(0, {'p': s, 'total': t})
            if skipped_kids:
                _recovery_note(prog, 'kids-skipped', prefix,
                               f'{skipped_kids} 个子块已由 records 反查证明完整，未入队')
            save_progress(prog, prefix)

def final_recheck_round(prog):
    """终局修复轮：对全部遗留 gaps 慢速重修。返回 (修复数, 仍缺数)。

    R3·slot-converge（F1/C2）：
      · 每个缺口带 repair_attempts 预算（GAP_REPAIR_MAX）；预算用尽且仍未补齐 ⇒ 标
        terminal=True 并**保留在 gaps[]**（绝不静默丢弃 ⇒ 收敛判据会判"未收敛"）；
      · 进入/退出修复轮写 repair_round_enter / repair_round_result / repair_exhausted 审计；
      · 修复结果结构化落到 LAST_REPAIR_SUMMARY（供收敛报告与测试读数）。"""
    global LAST_REPAIR_SUMMARY
    gaps = prog.get('gaps', [])
    if not gaps:
        LAST_REPAIR_SUMMARY = {'entered': False, 'gaps_in': 0, 'fixed': 0,
                               'remaining': 0, 'terminal': 0}
        return 0, 0
    fixed = 0
    remaining = []
    terminal = 0
    log(f"=== 终局修复轮：{len(gaps)} 个遗留缺口 ===")
    audit_event('repair_round_enter', 'block',
                {'reason': 'final-recheck', 'todo_snapshot': len(prog.get('todo') or []),
                 'gaps': len(gaps),
                 'degraded': sum(1 for g in gaps if g.get('degraded')),
                 'attempts_max': max([int(g.get('repair_attempts', 0) or 0) for g in gaps] or [0])})
    for gap in gaps:
        p = gap['p']
        total = gap.get('total')
        if gap.get('terminal'):
            # 预算已耗尽：不再重修，但**保留**在 gaps[]（收敛判据据此判未收敛）
            remaining.append(gap)
            terminal += 1
            continue
        # 本轮尝试计数（三种类型共用；下面的预算判定据此标 terminal）
        gap['repair_attempts'] = int(gap.get('repair_attempts', 0) or 0) + 1
        if gap.get('type') == 'undetermined':
            # R3p4·P4-A：未定论子树（条目 total 解析失败 / 条目异常预算耗尽）的修复轮。
            #   · total 不可得 ⇒ 重新探测（瞬时故障在此自愈）；
            #   · total 可得且 records 已证明覆盖 ⇒ 消解（留档进 resolved_gaps[]）；
            #   · total 可得但本轮未证明覆盖 ⇒ 回队重排重跑该子树，缺口留档（预算继续累计）。
            tp = total
            if not tp:
                tp = _safe_get_total(p, None, 'repair-undetermined')
                if tp:
                    gap['total'] = tp
                    log(f"  终局修复未定论块 {p!r}: total 重新探测成功={tp}")
            # (a) 上次「回队重排」之后没有再次降级 ⇒ 子树已跑完 ⇒ 消解（不留假未收敛）
            if float(gap.get('requeued_ts', 0) or 0) > float(gap.get('last_degrade_ts', 0) or 0):
                gap['resolved'] = True
                gap['resolved_reason'] = 'requeued-completed'
                _keep_resolved_gap(prog, gap, 'requeued-completed')
                fixed += 1
                log(f"  终局修复未定论块 {p!r}: 回队重排后子树已完成"
                    f"（records={recorded_count(p)}）✓（消解留档）")
                continue
            if tp and is_recorded_done(p, tp):
                gap['resolved'] = True
                gap['resolved_reason'] = 'records-covered'
                _keep_resolved_gap(prog, gap, 'undetermined-covered')
                fixed += 1
                log(f"  终局修复未定论块 {p!r}: records 反查 {recorded_count(p)}/{tp} ✓（消解留档）")
                continue
            if tp:
                prog['todo'].append({'p': p, 'total': tp})
                gap['requeued_ts'] = round(time.time(), 3)
                fixed += 1
                log(f"  终局修复未定论块 {p!r}: total={tp} → 回队重排（本轮未证明覆盖，缺口留档）")
            else:
                log(f"  终局修复未定论块 {p!r}: total 仍不可得 → 保留未决缺口")
            remaining.append(gap)
            if int(gap.get('repair_attempts', 0) or 0) >= GAP_REPAIR_MAX:
                gap['terminal'] = True
                gap['terminal_reason'] = 'repair-budget-exhausted'
                audit_event('repair_exhausted', 'block',
                            {'prefix': p, 'type': 'undetermined',
                             'attempts': gap.get('repair_attempts'),
                             'budget': GAP_REPAIR_MAX, 'total': gap.get('total'),
                             'covered': gap.get('covered'), 'got': gap.get('got'),
                             'degraded': True, 'reason': gap.get('reason'),
                             'missing_chars': 0})
                log(f"  [r3] 未定论缺口 {p!r} 修复预算用尽"
                    f"（{gap['repair_attempts']}/{GAP_REPAIR_MAX}）→ 标 terminal："
                    f"收敛判据判「未收敛」（不谎报完成）")
            continue
        tol = max(2, int(total * 0.015)) if total else 2
        kept = False                       # 本轮该缺口是否仍未补齐（留在 remaining）
        if gap.get('type') == 'leaf':
            if is_recorded_done(p, total):        # v10: 反查已够 → 免重修
                fixed += 1
                _tolerance_mark(prog, p, recorded_count(p), total, '终局修复跳过')   # v10-fix
                _keep_resolved_gap(prog, gap, 'records-covered')                     # L2：留档
                log(f"  终局修复叶子 {p!r}: records 反查 {recorded_count(p)}/{total} ✓（跳过重修）")
                continue
            ok, got = scrape_leaf(p, total, deep_retry=True)
            if got >= total - tol:
                fixed += 1
                _keep_resolved_gap(prog, gap, 'repair-covered')                      # L2：留档
                log(f"  终局修复叶子 {p!r}: {got}/{total} ✓")
            else:
                gap['got'] = max(gap.get('got', 0), got)
                log(f"  终局修复叶子 {p!r} 仍缺: {got}/{total}")
                remaining.append(gap)
                kept = True
        else:  # branch
            chars = gap.get('chars', CHARSET)
            if not chars:
                chars = CHARSET
            g = gap.get('gap')
            if g is None:  # 兼容无 gap 字段的遗留条目（如 AT2/AT3/AT4/AT5 仅有 total/got）
                g = max(0, total - gap.get('covered', gap.get('got', 0)))
            found = recheck_chars(p, chars, g, max_chars=20, miss_limit=6, pause=0.6)
            if found:
                fixed += len(found)
                for sub, t in reversed(found):
                    prog['todo'].append({'p': sub, 'total': t})
                cov = gap.get('covered', 0) + sum(t for _, t in found)
                gap['covered'] = cov
                if total - cov > max(GAP_TRIGGER, int(total * 0.02)):
                    remaining.append(gap)
                    kept = True
                else:
                    gap['resolved'] = True
                    gap['resolved_reason'] = 'tolerance-covered'
                    _keep_resolved_gap(prog, gap, 'tolerance-covered')   # L2：留档，不静默消失
                    log(f"  终局修复分支 {p!r} 已补齐（{cov}/{total}）→ 消解留档 resolved_gaps[]")
            else:
                remaining.append(gap)
                kept = True
        # 预算判定：本缺口已重修 GAP_REPAIR_MAX 次仍未补齐 ⇒ terminal（不再空转）
        if kept and not gap.get('resolved') \
                and int(gap.get('repair_attempts', 0) or 0) >= GAP_REPAIR_MAX:
            gap['terminal'] = True
            gap['terminal_reason'] = 'repair-budget-exhausted'
            audit_event('repair_exhausted', 'block',
                        {'prefix': gap.get('p'), 'type': gap.get('type'),
                         'attempts': gap.get('repair_attempts'),
                         'budget': GAP_REPAIR_MAX, 'total': total,
                         'covered': gap.get('covered'), 'got': gap.get('got'),
                         'degraded': bool(gap.get('degraded')),
                         'missing_chars': len(gap.get('chars') or [])})
            log(f"  [r3] 缺口 {gap.get('p')!r} 修复预算用尽"
                f"（{gap['repair_attempts']}/{GAP_REPAIR_MAX}）→ 标 terminal："
                f"收敛判据将判「未收敛」（不再空转，也不谎报完成）")
    prog['gaps'] = remaining
    terminal = sum(1 for g in remaining if g.get('terminal'))
    save_progress(prog)
    LAST_REPAIR_SUMMARY = {'entered': True, 'gaps_in': len(gaps), 'fixed': fixed,
                           'remaining': len(remaining), 'terminal': terminal}
    audit_event('repair_round_result', 'block',
                {'fixed': fixed, 'remaining': len(remaining), 'terminal': terminal,
                 'gaps_in': len(gaps)})
    log(f"  终局轮：修复 {fixed}，仍缺 {len(remaining)}（其中预算耗尽 terminal={terminal}）")
    return fixed, len(remaining)

# ===========================================================================
# R2·slot-mem 内存守卫接线（BEGIN —— 本 slot 独占区间，其余 slot 请勿改动这三处）
#   · effective_workers(kind)：**读函数**，返回内存守卫当前生效的并发度
#       NORMAL/WARN → 基线；PRESSURE → 基线×0.5（下限 1）；CRITICAL/SAFE_MODE → 1。
#     守卫未启动（import 失败/MEMGUARD_ENABLED=0）时返回基线常量 ⇒ 行为等价 v10.1。
#   · 调用点仅两处：scrape_leaf 的叶子补抓池、probe_children 的探测池。
#   · 「任务闸门暂停新工作」由 memguard.install_gate() 在 main() 里于运行时包装
#     scrape_leaf / probe_children 的入口实现（可逆、不改这两个函数的函数体）。
#   · memguard 用函数内 import（不改文件头 import 区）。
# ===========================================================================
def effective_workers(kind):
    """内存守卫生效并发度（纯读、无阻塞、无 IO）。"""
    try:
        import memguard
        return memguard.effective_workers(kind)
    except Exception:
        return {'leaf': LEAF_WORKERS, 'probe': PROBE_WORKERS}.get(kind, 1)


def main():
    args = sys.argv[1:]
    try:                                                # R2·mem: 守卫缺席/异常都不阻断抓取（fail-open）
        import memguard                                 # 函数内 import（不动文件头 import 区）
        memguard.start(audit_path=OUT / "memguard.jsonl",   # 结构化内存审计（jsonl，按大小轮转）
                       base_workers={'leaf': LEAF_WORKERS, 'probe': PROBE_WORKERS},
                       tag='scrape-v10.1', log_fn=log)      # WARN+ 同时写 scrape 日志
        memguard.install_gate(sys.modules[__name__])    # 闸门：CRITICAL 时暂停新工作单元
    except Exception as _mg_err:                        # 缺模块/权限/任何异常 ⇒ 等价 v10.1 运行
        log(f"[memguard] 内存守卫未启用（{type(_mg_err).__name__}: {_mg_err}），"
            f"并发度退回基线常量 LEAF_WORKERS={LEAF_WORKERS} PROBE_WORKERS={PROBE_WORKERS}")
    prog = load_progress()
    if prog is None:
        seeds = args if args else list(string.ascii_uppercase)
        if RECORDS_FILE.exists() and RECORDS_FILE.stat().st_size > 0:
            prog = rebuild_from_records(seeds, args)     # 兜底：全量反查 records 重建队列
        else:
            log(f"初始化队列，种子：{seeds}")
            baseline = _safe_get_total("", None, 'baseline')     # R3·F2：200-缺-info 不再崩启动
            log(f"全量基线: {baseline}")
            prog = {'todo': [{'p': s, 'total': None} for s in seeds],
                    'stats': {'leaves': 0, 'records': 0},
                    'gaps': [], 'done': [], 'recovery': [],
                    'baseline': baseline,
                    'seeds': list(seeds),
                    TAINT_KEY: [],
                    'started': datetime.now().isoformat()}
    else:
        prog.setdefault('done', [])
        prog.setdefault('recovery', [])
        prog.setdefault(TAINT_KEY, [])
        prog.setdefault('probe_dead', [])
        for _g in (prog.get('gaps') or []):              # R3：老 progress 兼容（补预算计数）
            _g.setdefault('repair_attempts', 0)
        prog.setdefault('seeds', list(args) if args else list(string.ascii_uppercase))

    # omni-patch(2026-09-27): inflight 快照回队 —— 崩溃窗口 = 条目被认领处理中
    # （缺此恢复：条目 take 后从磁盘 todo 移除、被杀即无痕丢失 → 重启假收敛 exit 0）
    _snap = prog.pop('_inflight_snapshot', None) or []
    if _snap:
        _todo_ps = {x.get('p') for x in (prog.get('todo') or [])}
        _done_ps = set()
        for _d in (prog.get('done') or []):
            _done_ps.add(_d.get('p') if isinstance(_d, dict) else _d)
        _requeue = []
        for _it in _snap:
            _p = (_it or {}).get('p') if isinstance(_it, dict) else None
            if _p and _p not in _todo_ps and _p not in _done_ps:
                _requeue.append(_it)
                _todo_ps.add(_p)
        if _requeue:
            prog['todo'] = list(_requeue) + (prog.get('todo') or [])
            log(f"[omni] inflight 快照回队 {len(_requeue)} 项: "
                f"{[x.get('p') for x in _requeue]}")
            try:
                audit_event('inflight_requeue', 'process',
                            {'count': len(_requeue),
                             'items': [x.get('p') for x in _requeue]})
            except Exception:
                pass
    reconcile(prog)                                     # v10: 启动先做一致性自愈
    save_progress(prog)
    log(f"=== scrape v10 长程加固版启动（MIN_INTERVAL={MIN_INTERVAL} PROBE_WORKERS={PROBE_WORKERS}）===")
    # R2·P4-C：并发配置单行可审计（与上面那行分开写，避免与其它 slot 的 patch 冲突）
    log(f"[conc] 配置 CONC_ENABLED={CONC_ENABLED} W_MAX={W_MAX} W_MIN={W_MIN} "
        f"ADAPTIVE={CONC_ADAPTIVE} SCALE_UP_AFTER={W_SCALE_UP_AFTER_S:.0f}s "
        f"DEFER_SLOTS={CONC_DEFER_SLOTS}")
    # R3·slot-converge：自愈/收敛判据配置单行可审计
    log(f"[r3] 配置 audit={AUDIT_FILE.name} 探测重试≤{PROBE_DEAD_RETRY_MAX}({PROBE_DEAD_DELAY_S:.0f}s) "
        f"total未定论重试≤{TOTAL_NONE_RETRY_MAX} ITEM_EXC_RETRY_MAX={ITEM_EXC_RETRY_MAX} "
        f"缺口修复预算={GAP_REPAIR_MAX} 停滞阈值={STALL_PROGRESS_AGE_S:.0f}s/{STALL_STOP_AGE_S:.0f}s"
        f"(action={STALL_ACTION}) 退出码 收敛={EXIT_CONVERGED} 未收敛={EXIT_INCOMPLETE} "
        f"停滞自停={EXIT_STALL_STOP} 轮数上限={MAX_ROUNDS}")
    audit_event('run_start', 'process',
                {'seeds': prog.get('seeds'), 'todo': len(prog.get('todo') or []),
                 'gaps': len(prog.get('gaps') or []), 'argv': sys.argv[1:],
                 'out': str(OUT), 'baseline': prog.get('baseline'),
                 'conc': {'enabled': CONC_ENABLED, 'w_max': W_MAX},
                 'exit_codes': {'converged': EXIT_CONVERGED, 'incomplete': EXIT_INCOMPLETE,
                                'stall_stop': EXIT_STALL_STOP}})
    _stall_monitor_start(prog)                          # R3·F5：独立停滞监测线程
    round_num = 0
    rc = EXIT_CONVERGED
    try:
        while True:
            round_num += 1
            log(f"--- 主循环第 {round_num} 轮 ---")
            stall_tick(prog, 'round')                   # R3·F5：每轮至少采样一次水位
            process_queue(prog)
            fixed, remaining = final_recheck_round(prog)
            decided, terminal, rc_c, reason, detail = convergence_decision(
                prog, round_num, fixed, remaining)
            audit_event('convergence_decision', 'block',
                        dict(detail, decided=decided, reason=reason,
                             terminal=bool(terminal), exit_code=rc_c))
            write_convergence_report(prog, decided, terminal, rc_c, reason, detail)
            if terminal:
                rc = EXIT_CONVERGED if rc_c is None else rc_c
                log(f"[r3] 收敛判据：decided={decided} terminal={terminal} reason={reason} "
                    f"todo={detail['todo']} gaps={detail['gaps']} "
                    f"terminal_gaps={detail['terminal_gaps']} → exit {rc}")
                break
            if round_num >= MAX_ROUNDS:
                # R3p4·P4-A（C-5）：rc 同时看 **todo** 与未决缺口 —— 防御性缺口闭合：
                # 只要还有未做完的条目，就不是收敛（此前只看 gaps_open，todo>0 时会误判 exit 0）。
                gaps_open = len(open_gaps(prog))
                todo_left = len(prog.get('todo') or [])
                rc = EXIT_INCOMPLETE if (gaps_open or todo_left) else EXIT_CONVERGED
                detail2 = dict(detail, reason='max-rounds', max_rounds=MAX_ROUNDS,
                               todo=todo_left)
                audit_event('convergence_decision', 'block',
                            dict(detail2, decided='continue', terminal=True, exit_code=rc))
                write_convergence_report(prog, 'continue', True, rc, 'max-rounds', detail2)
                log(f"[r3] 达最大轮数（{MAX_ROUNDS}）：未决缺口 {gaps_open} 未完成条目 {todo_left} "
                    f"→ exit {rc}")
                break
    finally:
        _stall_monitor_stop()

    try:                                                # R3·F3：收尾冲刷待写 records
        left = flush_pending_records()
    except Exception:
        left = -1
    if isinstance(left, int) and left > 0:
        log(f"[r3] 收尾：仍有 {left} 条 records 未落盘（已审计 records_write_deferred）")
        rc = EXIT_INCOMPLETE if rc == EXIT_CONVERGED else rc
    gaps_left = open_gaps(prog)
    resolved_arch = list(prog.get('resolved_gaps') or [])
    if rc == EXIT_CONVERGED:
        # 收敛：保留原「全部完成」字样（keeper-v2 的 JOB_COMPPAT 依赖），但结论以退出码为准
        # R3p4·P4-A（C-5/L1）：文案用**真实值**，不再把 `todo=0 gaps=0` 硬编码进字符串。
        log(f"全部完成！stats={prog['stats']} 收敛判据=exit {EXIT_CONVERGED}"
            f"（todo={len(prog.get('todo') or [])} gaps={len(gaps_left)} "
            f"resolved_gaps={len(resolved_arch)}，报告={CONV_REPORT_FILE.name}）")
    else:
        # 未收敛：**刻意不打印「全部完成」** —— 字符串判据正是 F1/F6 要消灭的东西
        log(f"未收敛（exit {rc}）：todo={len(prog['todo'])} 未决缺口={len(gaps_left)} "
            f"stats={prog['stats']} → 详见 {CONV_REPORT_FILE.name} 与 {AUDIT_FILE.name}")
    if prog['gaps']:
        log(f"=== 最终遗留缺口 ({len(prog['gaps'])}) ===")
        for g in prog['gaps']:
            log(f"  {g}")
    if resolved_arch:
        log(f"=== 已消解缺口留档 resolved_gaps ({len(resolved_arch)}) ==="
            f"（覆盖率审计可见，不再静默消失）")
    audit_event('run_end', 'process',
                {'exit_code': rc, 'todo': len(prog.get('todo') or []),
                 'gaps': len(gaps_left), 'done': len(prog.get('done') or []),
                 'undetermined': len(prog.get('undetermined') or []),
                 'resolved_gaps': len(resolved_arch),
                 'stats': dict(prog.get('stats') or {}),
                 'audit_counts': audit_counts(), 'audit_dropped': _AUDIT_DROPPED,
                 'records_deferred': _RECORDS_DEFERRED})
    return rc

def _cli():
    """--reconcile-only：只做 records 修复 + 反查重建，不抓取（供测试/运维离线使用）。
    R2: --throttle=auto|static 已在上方 _r2_throttle_mode() 解析并从 argv 摘除，
        此处把过滤后的 argv 交回 main()（main 用 sys.argv[1:] 取种子，需看到过滤结果）。"""
    _snap = LIMITER.snapshot()
    log(f"{_LOG_PREFIX} mode={_snap['mode']} interval={_snap['interval']:.4f}s "
        f"rate={_snap['rate_req_s']:.3f}req/s 下界={_snap['min_interval']:.4f}s"
        f"(窗口上限={_snap['rate_window_max']}req/{THROTTLE_RATE_WINDOW:.1f}s) "
        f"上界={_snap['max_interval']:.2f}s 窗口={THROTTLE_WINDOW}")
    sys.argv = [sys.argv[0]] + list(_MAIN_ARGV)      # R2: 摘除 --throttle 后交还 main()
    if '--reconcile-only' in sys.argv[1:]:
        prog = load_progress()
        if prog is None:
            print(json.dumps({'ok': False, 'reason': 'no usable progress.json/.bak'},
                             ensure_ascii=False))
            return 2
        prog.setdefault('done', [])
        prog.setdefault('recovery', [])
        prog.setdefault(TAINT_KEY, [])
        st = reconcile(prog)
        save_progress(prog)
        print(json.dumps({'ok': True, **st}, ensure_ascii=False))
        return 0
    # R3·F6：main() 的返回码即收敛判据（0=收敛 / 3=未收敛 / 4=停滞自停），原样上抛
    rc = main()
    return rc if isinstance(rc, int) else 0

if __name__ == '__main__':
    sys.exit(_cli())
