#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证修改效果"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("验证修改效果")
print("=" * 80)

# 读取数据
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)

print(f"\n1. 检查来源是否使用'二轮规则'...")
erlun_count = (result_df['来源'] == '二轮规则').sum()
print(f"   '二轮规则'记录: {erlun_count:,} 条")

print(f"\n2. 检查是否还有'规则(终极版)'...")
old_source = result_df[result_df['来源'] == '规则(终极版)']
print(f"   '规则(终极版)'记录: {len(old_source):,} 条 {'✓ 正确' if len(old_source) == 0 else '需要清理'}")

print(f"\n3. 检查N/A记录...")
na_records = result_df[result_df['来源'] == 'N/A']
print(f"   N/A记录: {len(na_records):,} 条")

# 验证N/A记录确实没有任何角色
role_columns = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']
if len(na_records) > 0:
    na_with_names = na_records[na_records[role_columns].notna().any(axis=1)]
    print(f"   N/A但有人名的记录: {len(na_with_names):,} 条 {'✓ 正确' if len(na_with_names) == 0 else '✗ 错误'}")

print(f"\n4. 检查是否还有'需要AI处理'标记...")
has_ai = result_df[result_df['来源'].str.contains('需要AI处理:', na=False)]
print(f"   仍有'需要AI处理'标记: {len(has_ai):,} 条")
if len(has_ai) > 0:
    print(f"   样例（前5条）:")
    for _, row in has_ai.head(5).iterrows():
        print(f"     序号{row['序号']}: {row['来源']}")

print(f"\n5. ai_queue统计...")
print(f"   总记录: {len(queue_df):,}")
print(f"   待处理: {len(queue_df[queue_df['状态'] == '待处理']):,}")
print(f"   N/A(脱敏): {len(queue_df[queue_df['状态'] == 'N/A(脱敏)']):,}")
print(f"   已处理: {len(queue_df[queue_df['状态'].str.contains('已处理', na=False)]):,}")

print("\n" + "=" * 80)
print("验证完成")
print("=" * 80)
