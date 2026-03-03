#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证修复效果"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("验证修复效果")
print("=" * 80)

# 读取数据
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)

print(f"\n1. 检查ai_queue中是否还有'已处理'状态的记录...")
processed = queue_df[queue_df['状态'].str.contains('已处理', na=False)]
print(f"   结果: {len(processed)} 条 {'✓ 正确' if len(processed) == 0 else '✗ 错误'}")

print(f"\n2. 检查是否有脱敏标记(N/A)...")
na_records = queue_df[queue_df['状态'] == 'N/A(脱敏)']
print(f"   N/A(脱敏)记录: {len(na_records)} 条")
if len(na_records) > 0:
    print(f"   样例:")
    for _, row in na_records.head(5).iterrows():
        print(f"     序号{row['序号']}, {row['角色']}: {row['片段'][:50]}...")

print(f"\n3. 检查result.csv中是否还有'需要AI处理'标记...")
has_ai_marker = result_df[result_df['来源'].str.contains('需要AI处理:', na=False)]
print(f"   仍需AI处理的记录: {len(has_ai_marker)} 条")
if len(has_ai_marker) > 0:
    print(f"   样例（前5条）:")
    for _, row in has_ai_marker.head(5).iterrows():
        print(f"     序号{row['序号']}: {row['来源']}")

print(f"\n4. 检查是否还有问题提取（第二、米吉提、官担任等）...")
problem_names = ['第二', '米吉提', '官担任', '哈斯苏', '简易程']
found_problems = []
for name in problem_names:
    matches = result_df.apply(lambda row: any(name in str(row[col]) for col in result_df.columns if '审判' in col or '书记' in col), axis=1)
    if matches.any():
        found_problems.append((name, matches.sum()))

if found_problems:
    print(f"   发现问题提取:")
    for name, count in found_problems:
        print(f"     '{name}': {count} 次")
else:
    print(f"   ✓ 未发现已知问题提取")

print(f"\n5. 队列统计...")
print(f"   总片段: {len(queue_df):,}")
print(f"   待处理: {len(queue_df[queue_df['状态'] == '待处理']):,}")
print(f"   N/A(脱敏): {len(queue_df[queue_df['状态'] == 'N/A(脱敏)']):,}")

print("\n" + "=" * 80)
