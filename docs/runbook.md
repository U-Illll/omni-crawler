# RUNBOOK — omni-crawler 运维手册

> 版本：v0.1（2026-09-27，P1+P2 落地时点）

## 1. 快速开始

### 1.1 框架自检
```bash
cd ~/go/omni-crawler
bash tests/run_acceptance.sh     # 框架红队（11 套件）
bash deploy/acceptance.sh        # 总验收（红队 + adapter 清单 + 部署物）
```

### 1.2 跑 social（社媒）
```bash
# 框架托管循环（推荐；先小规模冒烟）
python3 omni/adapters/social/main.py run --rounds 1 --max-pages 10 --max-seed 0
# 原 sm-recon 命令（保真转发）
python3 omni/adapters/social/main.py loop --max-iter 50
```
现场数据在 `omni/adapters/social/runs/`（items/records/progress）。

### 1.3 跑 library（图书馆，托管模式）
```bash
# 状态
python3 omni/adapters/library/main.py status --site /tmp/library-scrape
# 单轮（检查→必要时启动 scrape.py 子进程）
python3 omni/adapters/library/main.py managed --once --site /tmp/library-scrape
# 长程托管（= keeper 内嵌；收敛后休整再退出，由 keeper 重拉）
python3 omni/adapters/library/main.py managed --loop --site /tmp/library-scrape
# 场景：
#   本机隔离测试现场: --site /tmp/omni-lib-test --seeds Z
#   生产切换: 指向既有现场（见 §4 迁移切换）
```
**注意**：托管模式不要同时开 keeper-v3.sh（二选一，避免双守护）。

### 1.4 keeper 看护（任意 adapter）
```bash
export OMNI_HOME=~/go/omni-crawler
export MAIN_CMD="python3 -u omni/adapters/library/main.py managed --loop --site /tmp/library-scrape"
bash deploy/keeper.sh watch      # 常驻（flock 单例）
bash deploy/keeper.sh status
bash deploy/keeper.sh stop
```
- STALE_LIMIT 必须 > 引擎休整（REST_BETWEEN_ROUNDS=900s），默认 1200s。

## 2. 如何写一个新 adapter

1. 建 `omni/adapters/<name>/`，参考 `omni/adapters/base.py` 实现：
   ```python
   class MyAdapter(Adapter):
       name = "my"
       def root(self): ...          # runs/logs 所在地
       def iter_once(self, ctx):    # 一个迭代单元；返回是否有进展
           r = ctx.fetcher.get(url, env="CN")   # 用框架 fetch 栈
           ctx.store.add_record("mykind", key, value, src=url)
           return True
       def converged(self, ctx):    # 机器判据（非字符串匹配）
           return ctx.store.summary()["pending"] == 0
   ```
2. `main.py`：Engine(adapter, args).run_once()/loop()。
3. 跑 `bash tests/run_acceptance.sh` 确保框架完好；自建 acceptance。

**通道声明**：`ctx.fetcher.get(url, env="LAN"|"CN"|"INTL"|"AUTO")`。
- LAN=南科大内网；CN=国内公网；INTL=外网（走 mihomo）。
- 出口表见 `omni/channel/manager.py`（可注入替换）。
- **ISP 住宅通道（2026-09-27 实测定稿）**：默认表未含——按需注入
  `egress_table={"isp-us": {7894}, "isp-jp": {7895}}`。出口敏感目标优先 ISP：
  groq 实测 7890（机房段）403 / 7894（US 住宅）·7895（JP 东京）200。

**升级链**：`omni/fetch/escalate.py::fetch_smart(http, browser, url)` 自动 L0→L0+→L1。
- L0+ = 协议层 TLS 指纹（curl_cffi，`http.get_impersonated()`）；空壳页（SPA）跳过 L0+ 直上 L1。
- L1 = Playwright Chromium（2026-09-27 本机实测链路全通：groq 登录页 browser-escalated 200）。

## 3. 双端部署

```bash
bash deploy/deploy.sh aliyun     # rsync 到 root@YOUR_SERVER_IP:/root/omni-crawler/
bash deploy/deploy.sh /tmp/test  # 本地演练
```
排除项：runs/ logs/ site/ *.pid test-results/ 等（防互踩）。

云端依赖：python3（requests）；浏览器路径需 playwright（云端 sm-recon venv 或另装）。

## 4. 迁移切换（library 生产现场）

现况：云上 `/tmp/library-scrape/`（R3 v2.1 部署件，就绪未启动）。
切换步骤（切后有回滚点）：
1. `bash deploy/deploy.sh aliyun`（同步框架）
2. 云端自检：`cd /root/omni-crawler && python3 tests/smoke_imports.py`
3. 先 status 观察：`python3 omni/adapters/library/main.py status --site /tmp/library-scrape`
4. 停旧守护（若 keeper-v3 在跑）：`bash /tmp/library-scrape/keeper-v3.sh stop`
5. 以托管模式起：`MAIN_CMD="python3 -u /root/omni-crawler/omni/adapters/library/main.py managed --loop --site /tmp/library-scrape" OMNI_HOME=/root/omni-crawler bash /root/omni-crawler/deploy/keeper.sh watch`
6. 回滚：停 omni keeper，重新启动原 keeper-v3 流程。

**social 迁移**：云上 sm-recon 现在由原 keeper 巡检。切换 = 把其中 `/tmp/sm-recon`（保持路径不变）的 MAIN_CMD 改为 omni 版入口，或直接把 keeper 的 BASE 指到 omni 部署。切换前先用本机跑通小规模；切换窗口选在收敛休整期。

## 5. 逆向桥（模式二工具接入）

流程（详见 architecture.md §6）：
1. 运行态出现签名壁垒 → `escalate.write_reverse_ticket()` 落盘任务单（runs/reverse-tickets/）
2. Agent 会话接单 → 用 frx-director（需用户先启动 Firefox Reverse 浏览器，marionette 3928）
   → 产出《param-blueprint.md》
3. 实现签名插件 → `omni/anticrawl/signers/<target>/`（Node 或 Python，纯本地）
4. adapter 声明使用 → 回归验证 → 记账

**当前阻塞**：frx 浏览器未启动（用户操作项）；Camoufox 已下载在位但**本机 Ubuntu 26.04 启动卡死**（2026-09-27 两版本实锤，见 §6 #2）；L1 现用 Playwright Chromium（实测可用）。

## 6. 已知事项

| # | 事项 | 说明 |
|---|---|---|
| 1 | library 补丁 | **omni-inflight-snapshot.patch**（修复 inflight 窗口崩溃丢失条目→假收敛；test_converge 血缘断言对其报 2 FAIL 属预期，行为类全 PASS；详见 `omni/adapters/library/OMNI-PATCHES.md`）|
| 2 | 浏览器路径依赖 | L1 用 `~/go/camoufox-venv/bin/python`（playwright 1.63 + **chromium-1243**，2026-09-27 实测 groq 200）；**camoufox/Firefox 本机 Ubuntu 26.04 不可用**（135/152 两版 headless 启动卡死实锤：glxtest 缺失+SWGL 失败+juggler 不握手；待 Firefox 修复后可切回）；系统 python 无 playwright。核心 HTTP 路径不受影响 |
| 3 | 云端 python | 云上 scrape.py 已打 py3.10 兼容层；omni 框架代码兼容 |
| 4 | 双守护互斥 | library 托管模式下不开 keeper-v3.sh |
| 5 | test-results/ | drill/e2e 的运行产物，不入库 |
| 6 | 域名键 | 限速/熔断键=hostname（去端口） |
| 7 | UA 注入 | HTTP 出口默认浏览器 UA（否则 WAF 秒拒 403，2026-09-27 实证教训） |
| 8 | 出口敏感目标 | groq 家族（console.groq.com 等）：**按出口 IP 判定**（非指纹）——机房段 403 / ISP 住宅 200；矩阵读数与接入方式见 §2 通道声明 |
