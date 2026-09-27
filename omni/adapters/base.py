#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""omni.adapters.base — 适配器协议。

每个爬虫任务 = 一个 adapter 包（omni/adapters/<name>/）：
- adapter.py 定义 Adapter 类（实现 iter_once / converged / report）
- main.py    CLI 入口（argparse → Engine）
- 可选：acceptance.json / 目标卡 etc.

记录 schema（框架最小约定）：
- store.add_item(key, ...)    处理单元（URL 或任务键）——幂等
- store.add_record(kind, key, value, src, evidence, ...) 产出——幂等按 (kind,key)
"""


class Adapter:
    name = "adapter"

    def root(self):
        """运行根目录（放 runs/ logs/ 的地方）。默认：实例目录。"""
        raise NotImplementedError

    def iter_once(self, ctx):
        """执行一个迭代单元。返回 True 表示有进展。"""
        raise NotImplementedError

    def converged(self, ctx):
        """收敛判定（所有任务完成 + pending 空）。"""
        raise NotImplementedError

    def report(self, ctx):
        """收尾报告（收敛后调用；写 runs/report.md 等）。"""
        pass
