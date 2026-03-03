#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
恢复合法姓名并清理真正的脏数据
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

print("=" * 80)
print("智能清理：只删除真正的脏数据，保留合法姓名")
print("=" * 80)

# 读取数据
print("\n读取ai_queue.csv...")
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
print(f"总片段数: {len(queue_df):,}")

# 定义真正的脏数据模式
invalid_patterns = ['简易程', '普通程', '特别程', '程序', '序由', '序公', '序独']

# 查找脏数据
dirty_mask = pd.Series([False] * len(queue_df))
for pattern in invalid_patterns:
    dirty_mask |= queue_df['AI提取姓名'].str.contains(pattern, na=False)

dirty_count = dirty_mask.sum()
print(f"\n发现包含非法模式的提取结果: {dirty_count:,} 条")

if dirty_count > 0:
    # 显示样例
    print("\n样例（前20条）:")
    samples = queue_df[dirty_mask][['序号', '角色', 'AI提取姓名']].head(20)
    for _, row in samples.iterrows():
        print(f"  序号{row['序号']}, {row['角色']}: {row['AI提取姓名']}")
    
    # 清理
    print(f"\n清理中...")
    queue_df.loc[dirty_mask, 'AI提取姓名'] = ''
    queue_df.loc[dirty_mask, '状态'] = '待处理'
    
    # 保存
    queue_df.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')
    print(f"✓ 已清理 {dirty_count:,} 条脏数据")
    print(f"✓ 已保存: {QUEUE_PATH}")
    
    # 统计保留的合法姓名（姓程、姓序的）
    valid_cheng = queue_df[queue_df['AI提取姓名'].str.startswith('程', na=False)]['AI提取姓名'].nunique()
    valid_xu = queue_df[queue_df['AI提取姓名'].str.startswith('序', na=False)]['AI提取姓名'].nunique()
    if valid_cheng > 0 or valid_xu > 0:
        print(f"\n✓ 保留的合法姓名: 姓程({valid_cheng}种), 姓序({valid_xu}种)")
else:
    print("✓ 未发现脏数据")

print("\n" + "=" * 80)
print("清理完成")
print("=" * 80)
