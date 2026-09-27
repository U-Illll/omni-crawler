#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""sm_sources — 查询矩阵与种子站（v0.1）"""

# 四轮搜索查询矩阵
ROUNDS = {
    1: [  # 核心留学生向
        "南方科技大学 留学生 群",
        "南方科技大学 留学生 QQ群",
        "南方科技大学 国际学生 群",
        "南科大 留学生 微信群",
        "南科大 国际学生 微信群",
        "SUSTech 留学生 群",
        "SUSTech international students 微信群",
        "南方科技大学 交换生 群",
        "南科大 学伴 群",
        "南方科技大学 外国学生 群",
        "南科大 国际生 加群",
        "南方科技大学 留学生会",
        "南科大 留学生 招生群",
        "SUSTech wechat group",
        "SUSTech QQ group international",
        "南方科技大学 留学生 2026",
    ],
    2: [  # 留学申请向（准备出国的南科大学生）
        "南科大 留学 交流群",
        "南方科技大学 留学申请 群",
        "南科大 飞跃 群",
        "南科大 留学 微信群",
        "南科大 出国 交流群",
        "南方科技大学 海外 群",
        "南科大 交换 群",
        "南科大 留学 学长 群",
        "南方科技大学 飞跃手册",
        "南科大 暑研 群",
    ],
    3: [  # 新生/招生向
        "南方科技大学 新生群 2026",
        "南方科技大学 新生QQ群",
        "南方科技大学 官方招生群",
        "南方科技大学 招生咨询群",
        "南科大 2026 新生 微信群",
        "南方科技大学 家长群",
        "南科大 国际招生 群",
        "南方科技大学 研究生新生群",
        "南方科技大学 综评 群",
        "南方科技大学 招生 群 号",
    ],
    4: [  # 国别/来源地向
        "南科大 巴基斯坦 学生",
        "南科大 韩国 学生 群",
        "南科大 俄罗斯 学生",
        "南科大 非洲 学生 群",
        "南方科技大学 印尼 学生",
        "南方科技大学 泰国 学生",
        "SUSTech international students QQ",
        "SUSTech Korean students",
        "SUSTech Pakistani students group",
        "SUSTech African students",
        "南方科技大学 越南 学生 群",
        "南方科技大学 马来西亚 学生",
    ],
    5: [  # 补充维度（第三批 2026-09-23：港澳台/校友/预科/语言生）
        "南方科技大学 港澳台 学生 群",
        "南科大 台湾 学生 群",
        "南科大 香港 学生 群",
        "南科大 澳门 学生",
        "南方科技大学 校友 群",
        "南科大 国际校友",
        "SUSTech alumni group chat",
        "南方科技大学 预科 学生",
        "南科大 语言生 群",
        "南方科技大学 进修 学生",
        "SUSTech foundation program students",
        "南科大 国际 学生 会 微信群",
    ],
}

# 学校官方种子站（深挖起点）
SEED_URLS = [
    ("https://www.sustech.edu.cn/zh/index2.html", "sustech_site"),
    ("https://www.sustech.edu.cn/en/", "sustech_site"),
    ("https://global.sustech.edu.cn/international_students", "global_site"),
    ("https://global.sustech.edu.cn/news/announcements", "global_site"),
    ("https://global.sustech.edu.cn/news/news", "global_site"),
    ("https://www.sustech.edu.cn/en/students.html", "sustech_site"),
    ("https://infoadmin.sustech.edu.cn/index", "infoadmin"),
]

# 微信通道搜索词（搜狗微信文章搜索）
WECHAT_QUERIES = [
    "南方科技大学 留学生",
    "南科大 留学生 群",
    "南方科技大学 国际学生",
    "南科大 学伴",
    "南方科技大学 留学生会",
    "南科大 迎新 国际学生",
    "南方科技大学 国际招生",
    "南科大 交换生",
    "南方科技大学 留学 申请",
    "南科大 飞跃",
    "南方科技大学 新生 群",
    "南科大 加群",
    # 第二轮（针对性深挖 2026-09-23）
    "南方科技大学 国际学生 迎新",
    "南科大 留学生 报到",
    "南方科技大学 留学生 活动",
    "南科大 国际学生 中秋",
    "南方科技大学 国际 学生 春节",
    "南方科技大学 留学生 毕业",
    "南科大 国际学生学者",
    "南方科技大学 国际 学生 新生",
    # 第三轮（英文/国际维度 2026-09-23）
    "SUSTech international students",
    "南方科技大学 international students",
    "SUSTech orientation",
    "南科大 国际学者",
    "南方科技大学 国际合作 学生",
    "SUSTech global",
]

# 站内深挖：允许深挖的域名与预算
SEED_DOMAINS = ["sustech.edu.cn", "global.sustech.edu.cn", "newshub.sustech.edu.cn"]
SEED_MAX_PAGES_PER_DOMAIN = 40
SEED_DEPTH = 2


def queries_for(round_no, only=None):
    qs = ROUNDS.get(round_no, [])
    if only:
        qs = [q for q in qs if only in q]
    return qs
