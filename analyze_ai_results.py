#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析AI提取结果，检查"(无)"的合理性
"""
import sys
import io
import pandas as pd
from pathlib import Path
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"

print("=" * 80)
print("AI提取结果分析")
print("=" * 80)

# 读取数据
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)

print(f"\n总片段数: {len(queue_df):,}")

# 统计各种状态
status_counts = queue_df['状态'].value_counts()
print(f"\n状态统计:")
for status, count in status_counts.items():
    print(f"  {status}: {count:,}")

# 统计AI提取结果
ai_results = queue_df['AI提取姓名'].value_counts()
print(f"\nAI提取结果统计:")
for result, count in ai_results.head(10).items():
    print(f"  '{result}': {count:,}")

# 分析"(无)"的片段
no_result = queue_df[queue_df['AI提取姓名'] == '(无)']
print(f"\n\n{'='*80}")
print(f"'(无)'结果分析 - 总数: {len(no_result):,}")
print(f"{'='*80}")

# 按角色分类
print(f"\n按角色分类:")
role_counts = no_result['角色'].value_counts()
for role, count in role_counts.items():
    print(f"  {role}: {count:,}")

# 分析片段特征
print(f"\n\n片段特征分析（前30个'(无)'样例）:")
print(f"{'-'*80}")

categories = {
    '法律条文': [],
    '程序性描述': [],
    '姓名不完整': [],
    '可能有姓名': []
}

for idx, row in no_result.head(30).iterrows():
    snippet = row['片段']
    role = row['角色']
    
    # 判断类别
    if '法律' in snippet or '条文' in snippet or '第' in snippet[:10]:
        categories['法律条文'].append((role, snippet[:60]))
    elif '宣布' in snippet or '休庭' in snippet or '评议' in snippet or '署名' in snippet:
        categories['程序性描述'].append((role, snippet[:60]))
    elif re.search(r'[\u4e00-\u9fa5]{2,4}(?=审判|书记)', snippet):
        categories['可能有姓名'].append((role, snippet[:60]))
    else:
        categories['姓名不完整'].append((role, snippet[:60]))

for category, items in categories.items():
    if items:
        print(f"\n【{category}】({len(items)}个):")
        for role, snippet in items[:5]:
            print(f"  {role}: {snippet}...")

# 检查有成功提取的片段
success = queue_df[queue_df['AI提取姓名'].notna() & (queue_df['AI提取姓名'] != '(无)') & (queue_df['AI提取姓名'] != '')]
print(f"\n\n{'='*80}")
print(f"成功提取分析 - 总数: {len(success):,}")
print(f"{'='*80}")

print(f"\n成功提取样例（前20个）:")
for idx, row in success.head(20).iterrows():
    print(f"  {row['角色']}: {row['AI提取姓名']} | 片段: {row['片段'][:50]}...")

# 计算成功率
total_processed = len(queue_df[queue_df['状态'] == '已处理'])
total_success = len(success)
success_rate = (total_success / total_processed * 100) if total_processed > 0 else 0

print(f"\n\n{'='*80}")
print(f"总体统计")
print(f"{'='*80}")
print(f"已处理片段: {total_processed:,}")
print(f"成功提取: {total_success:,}")
print(f"无法提取: {len(no_result):,}")
print(f"成功率: {success_rate:.1f}%")
print(f"无法提取率: {len(no_result)/total_processed*100:.1f}%")

print("\n" + "=" * 80)
