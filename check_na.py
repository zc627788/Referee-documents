#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import io
import pandas as pd

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

df = pd.read_csv(r'e:\Referee-documents\data\output\2016年01月裁判文书数据_result.csv', 
                 encoding='utf-8-sig', dtype=str)

# 检查序号323277
row_323277 = df[df['序号'] == '323277']
if len(row_323277) > 0:
    source = row_323277.iloc[0]['来源']
    print(f"序号323277的来源: [{source}]")
    print(f"类型: {type(source)}")
    print(f"是否为NaN: {pd.isna(source)}")
else:
    print("未找到序号323277")

# 统计来源
print(f"\n总记录数: {len(df)}")
print(f"来源为'N/A'的记录: {(df['来源'] == 'N/A').sum()}")
print(f"来源为NaN的记录: {df['来源'].isna().sum()}")
print(f"来源为空字符串的记录: {(df['来源'] == '').sum()}")

# 检查所有唯一的来源值
unique_sources = df['来源'].value_counts()
print(f"\n所有来源类型（前15个）:")
for src, count in unique_sources.head(15).items():
    print(f"  '{src}': {count:,}")
