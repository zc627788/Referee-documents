#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
专门清理脏数据：删除所有包含'程'或'序'的提取结果
"""
import sys
import io
import pandas as pd
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent / "src"))

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"

print("=" * 80)
print("清理脏数据：删除包含'程'或'序'的错误提取")
print("=" * 80)

# 读取数据
print("\n读取ai_queue.csv...")
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
print(f"总片段数: {len(queue_df):,}")

# 统计当前脏数据
dirty_mask = queue_df['AI提取姓名'].notna() & (
    queue_df['AI提取姓名'].str.contains('程', na=False) | 
    queue_df['AI提取姓名'].str.contains('序', na=False)
)
dirty_count = dirty_mask.sum()
print(f"\n发现包含'程'或'序'的提取结果: {dirty_count:,} 条")

if dirty_count > 0:
    # 显示样例
    print("\n样例（前10条）:")
    samples = queue_df[dirty_mask][['序号', '角色', 'AI提取姓名']].head(10)
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
else:
    print("✓ 未发现脏数据")

print("\n" + "=" * 80)
print("清理完成")
print("=" * 80)
