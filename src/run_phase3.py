#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase 3: AI失败结果补漏提取
用于处理经过AI提取（Phase 2）后依然返回"(无)"或空白的数据。
通过更具弹性的正则表达式（如允许单字名、处理错别字干扰等）进行最后一轮尝试提取。
"""

import pandas as pd
import re
from pathlib import Path
import sys

def fallback_extract(role, snippet):
    """
    针对AI遗漏的姓名特征进行专门正则提取。
    """
    if not isinstance(snippet, str):
        return None
        
    role_pattern = role.replace('代理', '(?:代理|代)?')
    
    # Pattern 1: Role matching, optional spaces/colons, then extract name.
    # Deals with strange spaces (e.g. 瞿 峥), html entities (&middot;), trailing unwanted words
    pat1 = re.compile(rf"{role_pattern}[\s:：]*(?P<name>[\u4e00-\u9fa5&;A-Za-z\s]+?)(?:\s|二〇|一九|等|关注|附|\\|\(|（|《|0|1|2|3|4|5|6|7|8|9|$)", re.UNICODE)
    m = pat1.search(snippet)
    
    if m:
        # Clean up the name
        n = m.group('name')
        n = n.replace('&middot;', '·').replace(' ', '')
        # Remove common verbs/nouns falsely caught at the start
        n = re.sub(r'^(担任|由|是由|记录|法官)', '', n)
        # It's an issue if it catches pure legal boilerplate
        if '署名' in n or '裁定' in n or '移送' in n or '宣布' in n:
            return None
        if len(n) >= 1 and len(n) <= 15:
            return n.strip()
            
    # Pattern 2: Name before the role (e.g., 由瞿德雄担任审判长)
    pat2 = re.compile(rf"(?:由|是由人员)?(?P<name>[\u4e00-\u9fa5&;A-Za-z]+)(?:同志)?担任{role_pattern}", re.UNICODE)
    m2 = pat2.search(snippet)
    if m2:
        n = m2.group('name').replace('&middot;', '·').replace(' ', '')
        if len(n) >= 1 and len(n) <= 15:
            return n.strip()
            
    return None


def run_phase3():
    data_dir = Path(r'e:\Referee-documents\data\output')
    queue_path = data_dir / '2016年01月裁判文书数据_ai_queue.csv'
    result_path = data_dir / '2016年01月裁判文书数据_result.csv'

    print("Phase 3: 正在读取数据...")
    qdf = pd.read_csv(queue_path, encoding='utf-8-sig', dtype=str)
    rdf = pd.read_csv(result_path, encoding='utf-8-sig', dtype=str)

    # 找到AI没能处理出来(或者处理失败)的数据
    mask = qdf['AI提取姓名'].isin(['(无)', '', 'NaN']) | qdf['AI提取姓名'].isna()
    failed_df = qdf[mask]

    print(f"检测到 {len(failed_df)} 条AI未提取出姓名的记录，开始补漏提取...")

    updates = {}
    for idx, row in failed_df.iterrows():
        role = str(row['角色'])
        snippet = str(row['片段'])
        sid = str(row['snippet_id'])
        
        extracted = fallback_extract(role, snippet)
        if extracted:
            updates[sid] = {'name': extracted, 'role': role}

    print(f"Phase 3 成功补漏提取到 {len(updates)} 个人名！")

    if updates:
        # 修改 Queue 表
        print("正在写回 ai_queue.csv ...")
        for idx in qdf.index:
            sid = str(qdf.at[idx, 'snippet_id'])
            if sid in updates:
                qdf.at[idx, '状态'] = '已处理(Phase3)'
                qdf.at[idx, 'AI提取姓名'] = updates[sid]['name']
        qdf.to_csv(queue_path, index=False, encoding='utf-8-sig')
        
        # 修改 Result 表
        print("正在合并至 result.csv ...")
        
        # 建立序号的快速索引
        idx_map = {}
        for df_idx, df_row in rdf.iterrows():
            idx_map[str(df_row['序号'])] = df_idx
            
        merged_count = 0
        from collections import defaultdict
        
        class Person:
            def __init__(self, name, role):
                self.name = name
                self.role = role
                
        # 将相同序号(不同角色片段)汇聚
        row_updates = defaultdict(list)
        for sid, info in updates.items():
            # sid格式为: rowIdx_snippetPosition
            ridx = str(int(sid.split('_')[0]) + 1)
            row_updates[ridx].append(Person(info['name'], info['role']))
            
        for ridx, persons in row_updates.items():
            if ridx in idx_map:
                df_idx = idx_map[ridx]
                updated_flag = False
                
                for p in persons:
                    if p.role in rdf.columns:
                        old_val = str(rdf.at[df_idx, p.role]) if pd.notna(rdf.at[df_idx, p.role]) else ''
                        old_names = [n for n in old_val.split(';') if n] if old_val else []
                        if p.name not in old_names:
                            old_names.append(p.name)
                            rdf.at[df_idx, p.role] = ';'.join(old_names)
                            updated_flag = True
                            
                if updated_flag:
                    # 更改来源
                    old_source = str(rdf.at[df_idx, '来源']) if pd.notna(rdf.at[df_idx, '来源']) else ''
                    if '补漏' not in old_source:
                        rdf.at[df_idx, '来源'] = old_source + '+补漏' if old_source else '补漏'
                    merged_count += 1
                
        rdf.to_csv(result_path, index=False, encoding='utf-8-sig', na_rep='')
        print(f"成功将 {merged_count} 条数据的遗漏信息更新至 result.csv！")

if __name__ == '__main__':
    run_phase3()
