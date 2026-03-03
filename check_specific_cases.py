#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""检查具体案例"""
import sys
import io
import pandas as pd
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 要检查的snippet_id
check_ids = ['450_5644', '564_1456', '773_2814', '783_677']

queue_df = pd.read_csv(r'e:\Referee-documents\data\output\2016年01月裁判文书数据_ai_queue.csv', 
                       encoding='utf-8-sig', dtype=str)

print("=" * 80)
print("具体案例分析")
print("=" * 80)

for sid in check_ids:
    row = queue_df[queue_df['snippet_id'] == sid]
    if len(row) > 0:
        row = row.iloc[0]
        print(f"\n【案例 {sid}】")
        print(f"角色: {row['角色']}")
        print(f"片段: {row['片段']}")
        print(f"AI结果: {row['AI提取姓名']}")
        
        # 分析片段
        snippet = row['片段']
        role = row['角色']
        
        # 检查是否包含角色词
        if role in snippet:
            print(f"✓ 片段中包含角色词'{role}'")
            # 提取角色词后的内容
            parts = snippet.split(role)
            if len(parts) > 1:
                after_role = parts[1][:30]
                print(f"  角色词后内容: {after_role}")
                
                # 检查是否有可能的姓名
                name_pattern = r'[\u4e00-\u9fa5]{2,4}'
                matches = re.findall(name_pattern, after_role[:20])
                if matches:
                    print(f"  可能的姓名: {matches}")
        else:
            print(f"✗ 片段中不包含角色词'{role}'")
        
        # 判断类型
        if '法律' in snippet or '条文' in snippet or '第' in snippet[:15]:
            print(f"  类型: 法律条文")
        elif '署名' in snippet or '加盖' in snippet or '印章' in snippet:
            print(f"  类型: 程序性描述")
        elif '移送' in snippet or '执行' in snippet[:20]:
            print(f"  类型: 程序性描述")
        else:
            print(f"  类型: 其他")
        
        print("-" * 80)
