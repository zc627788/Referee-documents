#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量更新旧的来源标记：
1. "规则(终极版)" -> "二轮规则"
2. 清理所有"需要AI处理"标记（如果该行有任何人名）
3. 设置完全空行为N/A
"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("批量更新result.csv中的来源标记")
print("=" * 80)

# 读取数据
print("\n读取result.csv...")
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)
print(f"总记录数: {len(result_df):,}")

# 角色列
role_columns = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']

# ============================================================
# 步骤1: 更新"规则(终极版)" -> "二轮规则"
# ============================================================
print("\n步骤1: 更新'规则(终极版)' -> '二轮规则'...")
old_source_mask = result_df['来源'] == '规则(终极版)'
old_count = old_source_mask.sum()

if old_count > 0:
    print(f"  发现 {old_count:,} 条'规则(终极版)'记录")
    result_df.loc[old_source_mask, '来源'] = '二轮规则'
    print(f"  ✓ 已更新为'二轮规则'")
else:
    print(f"  ✓ 未发现需要更新的记录")

# ============================================================
# 步骤2: 清理"需要AI处理"标记（向量化操作）
# ============================================================
print("\n步骤2: 清理'需要AI处理'标记...")
ai_marker_mask = result_df['来源'].str.contains('需要AI处理:', na=False)
ai_count = ai_marker_mask.sum()

if ai_count > 0:
    print(f"  发现 {ai_count:,} 条带'需要AI处理'标记的记录")
    
    # 向量化检查：该行是否有任何人名
    has_any_name = (result_df[role_columns].notna().any(axis=1)) & (result_df[role_columns] != '').any(axis=1)
    
    # 需要清理的：有AI标记且有人名
    clean_mask = ai_marker_mask & has_any_name
    
    if clean_mask.sum() > 0:
        # 向量化去除"需要AI处理:"部分
        result_df.loc[clean_mask, '来源'] = result_df.loc[clean_mask, '来源'].str.split('需要AI处理:').str[0].str.strip()
        print(f"  ✓ 清理了 {clean_mask.sum():,} 条记录的AI标记")
else:
    print(f"  ✓ 未发现需要清理的记录")

# ============================================================
# 步骤3: 设置完全空行为N/A
# ============================================================
print("\n步骤3: 设置完全空行为N/A...")
empty_mask = result_df[role_columns].isna().all(axis=1) | (result_df[role_columns] == '').all(axis=1)
empty_with_source = empty_mask & (result_df['来源'].notna()) & (result_df['来源'] != 'N/A')
empty_count = empty_with_source.sum()

if empty_count > 0:
    print(f"  发现 {empty_count:,} 条空记录需要设为N/A")
    result_df.loc[empty_with_source, '来源'] = 'N/A'
    print(f"  ✓ 已设置为N/A")
else:
    print(f"  ✓ 未发现需要设置的记录")

# ============================================================
# 保存
# ============================================================
print("\n保存文件...")
result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
print(f"  ✓ {RESULT_PATH}")

print("\n" + "=" * 80)
print("更新完成")
print("=" * 80)

# 最终统计
print(f"\n最终统计:")
print(f"  总记录: {len(result_df):,}")
print(f"  二轮规则: {(result_df['来源'] == '二轮规则').sum():,}")
print(f"  N/A: {(result_df['来源'] == 'N/A').sum():,}")
print(f"  仍有AI标记: {result_df['来源'].str.contains('需要AI处理:', na=False).sum():,}")
