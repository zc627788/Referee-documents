#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
恢复N/A记录：检查所有空记录并设置为N/A
"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("恢复N/A记录")
print("=" * 80)

# 读取数据
print("\n读取result.csv...")
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)
print(f"总记录数: {len(result_df):,}")

# 角色列
role_columns = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']

print("\n检查空记录（向量化操作）...")

# 向量化：找出所有角色列都为空且flag也为空的记录
empty_roles = (result_df[role_columns].isna() | (result_df[role_columns] == '')).all(axis=1)
empty_flag = result_df['flag'].isna() | (result_df['flag'] == '')

# 完全空的记录（角色为空且flag为空）
completely_empty = empty_roles & empty_flag

print(f"  完全空的记录（角色+flag都空）: {completely_empty.sum():,}")

# 检查当前N/A记录数
current_na = (result_df['来源'] == 'N/A').sum()
print(f"  当前N/A记录: {current_na:,}")

# 需要设置为N/A的记录（完全空但来源不是N/A）
need_na = completely_empty & (result_df['来源'] != 'N/A')
need_na_count = need_na.sum()

if need_na_count > 0:
    print(f"\n发现 {need_na_count:,} 条需要设置为N/A的记录")
    
    # 显示样例
    print("\n样例（前10条）:")
    for _, row in result_df[need_na].head(10).iterrows():
        print(f"  序号{row['序号']}, 案号{row['案号']}, 来源: {row['来源']}")
    
    # 设置为N/A
    result_df.loc[need_na, '来源'] = 'N/A'
    
    # 保存
    result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
    print(f"\n✓ 已设置 {need_na_count:,} 条记录为N/A")
    print(f"✓ 已保存: {RESULT_PATH}")
else:
    print(f"\n✓ 所有空记录都已正确标记为N/A")

print("\n" + "=" * 80)
print("恢复完成")
print("=" * 80)

# 最终统计
final_na = (result_df['来源'] == 'N/A').sum()
print(f"\n最终N/A记录数: {final_na:,}")
