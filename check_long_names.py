#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查是否存在5字以上的长名字"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"

df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)

# 筛选已处理的数据
processed = df[df['状态'] == '已处理(终极版)']
processed['name_len'] = processed['AI提取姓名'].str.len()

print("=" * 80)
print("姓名长度分布检查")
print("=" * 80)

print(f"\n总提取数: {len(processed):,}")
print("\n长度分布:")
print(processed['name_len'].value_counts().sort_index())

# 检查5字以上
long_names = processed[processed['name_len'] >= 5]
print(f"\n5字以上姓名: {len(long_names)} 条")

if len(long_names) > 0:
    print("\n样例:")
    for i, (idx, row) in enumerate(long_names.head(10).iterrows(), 1):
        print(f"{i}. '{row['AI提取姓名']}' (长度{row['name_len']}) - {row['角色']}")
else:
    print("✓ 无5字以上姓名，长度控制有效！")
