#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复5-6字的多姓名连接问题
将"张三李四"拆分为"张三"
"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.rule_extractor import COMMON_SURNAMES, COMPOUND_SURNAMES

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

print("=" * 80)
print("修复多姓名连接问题")
print("=" * 80)

# 读取数据
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)

# 筛选5-6字的姓名
jieba_mask = queue_df['状态'] == '已处理(jieba)'
queue_df['name_length'] = queue_df['AI提取姓名'].str.len()
long_mask = jieba_mask & (queue_df['name_length'] >= 5) & (queue_df['name_length'] <= 6)

long_names = queue_df[long_mask].copy()
print(f"\n找到5-6字姓名: {len(long_names):,} 条")

def split_multi_name(name: str) -> str:
    """拆分多个姓名，只保留第一个"""
    if not name or len(name) < 5:
        return name
    
    # 尝试找第二个姓氏的位置
    for i in range(2, len(name)):
        if name[i] in COMMON_SURNAMES or name[i:i+2] in COMPOUND_SURNAMES:
            # 找到第二个姓氏，提取第一个姓名
            first_name = name[:i]
            if 2 <= len(first_name) <= 4:
                return first_name
    
    # 没找到第二个姓氏，返回空（拒绝）
    return ''

# 向量化拆分
print("\n拆分多姓名...")
split_names = long_names['AI提取姓名'].apply(split_multi_name)

# 统计
valid_mask = split_names != ''
valid_count = valid_mask.sum()
invalid_count = (~valid_mask).sum()

print(f"成功拆分: {valid_count:,} 条")
print(f"无法拆分（删除）: {invalid_count:,} 条")

# 更新queue
queue_df.loc[long_names[valid_mask].index, 'AI提取姓名'] = split_names[valid_mask].values
queue_df.loc[long_names[~valid_mask].index, '状态'] = '待处理'
queue_df.loc[long_names[~valid_mask].index, 'AI提取姓名'] = ''

# 更新result
print("\n更新result...")
roles = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']

updated_count = 0
deleted_count = 0

for idx, row in long_names.iterrows():
    snippet_id = row['snippet_id']
    role = row['角色']
    old_name = row['AI提取姓名']
    new_name = split_names.loc[idx]
    
    row_num = int(snippet_id.split('_')[0]) + 1
    result_mask = result_df['序号'] == str(row_num)
    
    if result_mask.any():
        if new_name:
            # 更新为拆分后的姓名
            result_df.loc[result_mask, role] = new_name
            updated_count += 1
        else:
            # 删除无效姓名
            result_df.loc[result_mask, role] = ''
            deleted_count += 1

print(f"更新: {updated_count:,} 行")
print(f"删除: {deleted_count:,} 行")

# 保存
print("\n保存结果...")
queue_df.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')
result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')

# 显示样例
print(f"\n拆分样例（前20个）:")
for i, (idx, row) in enumerate(long_names[valid_mask].head(20).iterrows(), 1):
    old_name = row['AI提取姓名']
    new_name = split_names.loc[idx]
    print(f"{i}. {old_name} → {new_name}")

print(f"\n{'='*80}")
print(f"修复完成")
print(f"{'='*80}\n")

# 重新统计
remaining = len(queue_df[(queue_df['AI提取姓名'] == '(无)') | (queue_df['AI提取姓名'] == '')])
success = len(queue_df[queue_df['状态'] == '已处理(jieba)'])

print(f"jieba提取总数: {success:,} 条")
print(f"剩余待处理: {remaining:,} 条")
print(f"成功率: {success/len(queue_df)*100:.1f}%")
