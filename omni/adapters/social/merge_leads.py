#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""merge_leads — 合并本机与云端的 leads.jsonl（幂等，输出 merged.jsonl + 覆盖写回）"""
import json
import os
import subprocess
import sys

RUNS = "/tmp/sm-recon/runs"
# 云端主机与 SSH 私钥从环境变量读取（不入库）
CLOUD_HOST = os.environ.get("SM_RECON_CLOUD_HOST", "user@your-server")
SSH_KEY = os.path.expanduser(os.environ.get("SM_RECON_SSH_KEY", "~/.ssh/id_ed25519"))
SSHOPT = (f"-F /dev/null -i {SSH_KEY} -o StrictHostKeyChecking=no "
          "-o UserKnownHostsFile=/tmp/kh_aliyun -o ConnectTimeout=8")


def load(path):
    out = []
    if not os.path.exists(path):
        return out
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out


def pull_cloud():
    tmp = "/tmp/sm-recon/runs/cloud-leads.jsonl"
    cmd = f"rsync -a -e 'ssh {SSHOPT}' {CLOUD_HOST}:/tmp/sm-recon/runs/leads.jsonl {tmp}"
    subprocess.run(cmd, shell=True, timeout=60, capture_output=True)
    return load(tmp)


def main():
    local = load(os.path.join(RUNS, "leads.jsonl"))
    cloud = pull_cloud()
    print(f"本机 {len(local)} 条 | 云端 {len(cloud)} 条")

    # 已知误报黑名单（社区成员数等）
    bad = {"18247"}

    seen = set()
    merged = []
    for r in local + cloud:
        if str(r.get("value")) in bad:
            continue
        key = (r.get("kind"), str(r.get("value")), (r.get("src") or "")[:120])
        if key in seen:
            continue
        seen.add(key)
        merged.append(r)

    # 排序：kind, -conf
    merged.sort(key=lambda r: (r.get("kind", ""), -(r.get("conf") or 0)))

    # 安全输出：merged-leads.jsonl（不覆盖正在运行的 leads.jsonl）
    out = os.path.join(RUNS, "merged-leads.jsonl")
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in merged:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, out)
    print(f"合并完成: {len(merged)} 条 → {out}")

    # 分类合并（本机 + 云端）
    cmds = f"rsync -a -e 'ssh {SSHOPT}' {CLOUD_HOST}:/tmp/sm-recon/runs/classified.jsonl /tmp/sm-recon/runs/cloud-classified.jsonl"
    subprocess.run(cmds, shell=True, timeout=60, capture_output=True)
    c2 = load("/tmp/sm-recon/runs/cloud-classified.jsonl")
    byval = {}
    for r in load(os.path.join(RUNS, "classified.jsonl")) + c2:
        byval[r["value"]] = r
    cpath = os.path.join(RUNS, "classified.jsonl")
    tmp = cpath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for r in byval.values():
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    os.replace(tmp, cpath)
    print(f"分类合并: {len(byval)} 条（含云端 {len(c2)}）")

    # 顺带统计
    from collections import Counter
    ks = Counter(r.get("kind") for r in merged)
    print("kinds:", dict(ks))


if __name__ == "__main__":
    main()
