#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复N/A记录：
1. result.csv中空记录的来源改为N/A
2. 同步ai_queue中的N/A记录到result
3. 从ai_queue中删除N/A记录
"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("修复N/A记录")
print("=" * 80)

# 读取数据
print("\n读取数据...")
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)

print(f"ai_queue总数: {len(queue_df):,}")
print(f"result总数: {len(result_df):,}")

# ============================================================
# 步骤1: 修复result.csv中的空记录
# ============================================================
print("\n步骤1: 修复result.csv中的空记录...")

# 角色列（根据实际result.csv的列名）
role_columns = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']

# 找出所有角色列都为空的记录
empty_mask = result_df[role_columns].isna().all(axis=1) | (result_df[role_columns] == '').all(axis=1)

# 这些记录的来源应该是N/A
empty_with_source = empty_mask & (result_df['来源'].notna()) & (result_df['来源'] != 'N/A')
empty_count = empty_with_source.sum()

if empty_count > 0:
    print(f"  发现 {empty_count:,} 条空记录需要修改来源为N/A")
    result_df.loc[empty_with_source, '来源'] = 'N/A'
else:
    print(f"  ✓ 未发现需要修改的空记录")

# ============================================================
# 步骤2: 同步ai_queue中的N/A记录到result
# ============================================================
print("\n步骤2: 同步ai_queue中的N/A记录到result...")

# 找出ai_queue中的N/A记录
na_mask = queue_df['状态'] == 'N/A(脱敏)'
na_records = queue_df[na_mask].copy()

print(f"  ai_queue中的N/A记录: {len(na_records):,} 条")

if len(na_records) > 0:
    # 提取序号和角色
    na_records['row_num'] = na_records['snippet_id'].str.split('_').str[0].astype(int) + 1
    na_records['row_num'] = na_records['row_num'].astype(str)
    
    # 按序号分组，收集所有N/A的角色
    na_by_row = na_records.groupby('row_num')['角色'].apply(lambda x: ', '.join(x.unique())).to_dict()
    
    # 更新result表
    result_df_indexed = result_df.set_index('序号')
    update_count = 0
    
    for row_num, roles in na_by_row.items():
        if row_num in result_df_indexed.index:
            current_source = result_df_indexed.loc[row_num, '来源']
            
            # 如果来源中已有"需要AI处理"标记，移除这些N/A角色
            if pd.notna(current_source) and '需要AI处理:' in str(current_source):
                parts = str(current_source).split('需要AI处理:')
                if len(parts) == 2:
                    base_source = parts[0].strip()
                    ai_roles = parts[1].strip()
                    
                    # 移除N/A角色
                    na_role_set = set(r.strip() for r in roles.split(','))
                    remaining_roles = [r.strip() for r in ai_roles.split(',') if r.strip() not in na_role_set]
                    
                    if remaining_roles:
                        result_df_indexed.loc[row_num, '来源'] = f"{base_source} 需要AI处理: {', '.join(remaining_roles)}"
                    else:
                        result_df_indexed.loc[row_num, '来源'] = base_source
                    
                    update_count += 1
    
    result_df = result_df_indexed.reset_index()
    
    if update_count > 0:
        print(f"  ✓ 更新了 {update_count} 行的AI标记")
    else:
        print(f"  ✓ 无需更新AI标记")

# ============================================================
# 步骤3: 从ai_queue中删除N/A记录
# ============================================================
print("\n步骤3: 从ai_queue中删除N/A记录...")

before_count = len(queue_df)
queue_df = queue_df[queue_df['状态'] != 'N/A(脱敏)'].copy()
removed_count = before_count - len(queue_df)

print(f"  删除 {removed_count:,} 条N/A记录")
print(f"  剩余 {len(queue_df):,} 条记录")

# ============================================================
# 保存
# ============================================================
print("\n保存文件...")
result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
queue_df.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')

print(f"  ✓ {RESULT_PATH}")
print(f"  ✓ {QUEUE_PATH}")

print("\n" + "=" * 80)
print("修复完成")
print("=" * 80)

# 最终统计
print(f"\n最终统计:")
print(f"  result.csv: {len(result_df):,} 条")
print(f"  ai_queue.csv: {len(queue_df):,} 条")
print(f"  result中N/A来源: {(result_df['来源'] == 'N/A').sum():,} 条")
