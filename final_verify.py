#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""最终验证"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("最终验证")
print("=" * 80)

result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)

print(f"\n总记录数: {len(result_df):,}")

# 统计各种来源
sources = result_df['来源'].value_counts()
print(f"\n来源统计:")
for source, count in sources.head(10).items():
    print(f"  {source}: {count:,}")

# 检查N/A记录
na_records = result_df[result_df['来源'] == 'N/A']
print(f"\nN/A记录: {len(na_records):,}")

if len(na_records) > 0:
    print(f"\nN/A记录样例（前5条）:")
    for _, row in na_records.head(5).iterrows():
        print(f"  序号{row['序号']}, 案号: {row['案号']}")

# 检查二轮规则
erlun = result_df[result_df['来源'] == '二轮规则']
print(f"\n二轮规则记录: {len(erlun):,}")

# 检查是否还有旧标记
old_marks = result_df[result_df['来源'] == '规则(终极版)']
print(f"\n规则(终极版)记录: {len(old_marks):,} {'✓' if len(old_marks) == 0 else '✗'}")

# 检查AI标记
ai_marks = result_df[result_df['来源'].str.contains('需要AI处理:', na=False)]
print(f"需要AI处理标记: {len(ai_marks):,} {'✓' if len(ai_marks) == 0 else '✗'}")

print("\n" + "=" * 80)
