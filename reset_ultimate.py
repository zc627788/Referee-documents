#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
from pathlib import Path

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)

# 重置终极版提取的记录
ultimate_mask = queue_df['状态'] == '已处理(终极版)'
queue_df.loc[ultimate_mask, '状态'] = '待处理'
queue_df.loc[ultimate_mask, 'AI提取姓名'] = ''

# 重置result
result_ultimate_mask = result_df['来源'] == '规则(终极版)'
roles = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']

for idx in result_df[result_ultimate_mask].index:
    for role in roles:
        if role in result_df.columns:
            result_df.at[idx, role] = ''
    result_df.at[idx, '来源'] = '规则+增强'

queue_df.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')
result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')

print(f"已重置 {ultimate_mask.sum()} 条终极版记录")
