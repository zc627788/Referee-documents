#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整流程：阶段1（流式读取） + 阶段1.1（铁血版） + 自动合并
特点：
1. 秒启动（读一块算一块）
2. 内存占用极低（只存当前处理的几万行）
3. 实时落盘，断电不丢数据
4. 自动执行阶段1.1并合并结果
"""
import sys
import io
import pandas as pd
import numpy as np
import re
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import subprocess
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent / "src"))

BASE = Path(__file__).parent

# 配置
INPUT_CSV = BASE / "input" / "2016年裁判文书数据" / "2016年裁判文书数据_马克数据网" / "2016年01月裁判文书数据.csv"
OUTPUT_DIR = BASE / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

QUEUE_PATH = OUTPUT_DIR / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = OUTPUT_DIR / "2016年01月裁判文书数据_result.csv"

READ_CHUNK_SIZE = 50000  # 每次读取5万行
ROLES = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员', '人民陪审员']

def process_chunk_data(args):
    """处理一个小切片的数据"""
    df_chunk, start_idx = args
    extracted_rows = []
    
    df_chunk['全文'] = df_chunk['全文'].fillna('')
    df_chunk['案号'] = df_chunk['案号'].fillna('')
    
    for role in ROLES:
        # 预筛选
        mask = df_chunk['全文'].str.contains(role, regex=False)
        if not mask.any():
            continue
            
        target_rows = df_chunk[mask]
        pattern = f"(.{{0,50}})({re.escape(role)})(.{{0,50}})"
        matches = target_rows['全文'].str.extractall(pattern)
        
        if len(matches) > 0:
            snippets = matches[0] + matches[1] + matches[2]
            orig_indices = matches.index.get_level_values(0)
            match_rank = matches.index.get_level_values(1)
            
            temp_res = pd.DataFrame({
                '文件': '201601',
                '序号': orig_indices + 1,
                '案号': df_chunk.loc[orig_indices, '案号'].values,
                '角色': role,
                '片段': snippets.values,
                '位置': 0,
                'snippet_id': orig_indices.astype(str) + '_' + match_rank.astype(str),
                '状态': '待处理',
                'AI提取姓名': ''
            })
            extracted_rows.append(temp_res)
            
    if extracted_rows:
        return pd.concat(extracted_rows)
    return None

if __name__ == '__main__':
    print("=" * 80)
    print("完整流程：阶段1（流式） + 阶段1.1（铁血版） + 自动合并")
    print("=" * 80)
    print(f"\n输入: {INPUT_CSV}")
    print(f"输出: {QUEUE_PATH}")
    print(f"      {RESULT_PATH}\n")
    
    # ============================================================
    # 阶段1：流式读取 + 多进程处理
    # ============================================================
    print("=" * 80)
    print("阶段1：流式读取 + 实时处理")
    print("=" * 80)
    
    start_time = time.time()
    
    # 1. 初始化输出文件
    print(f"\n[1/4] 初始化输出文件...")
    cols = ['文件', '序号', '案号', '角色', '片段', '位置', 'snippet_id', '状态', 'AI提取姓名']
    pd.DataFrame(columns=cols).to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')
    
    # 2. 启动流式处理
    print(f"\n[2/4] 启动流水线处理（CPU: {cpu_count()}核）...")
    
    reader = pd.read_csv(INPUT_CSV, encoding='utf-8', usecols=['全文', '案号'], chunksize=READ_CHUNK_SIZE)
    
    total_extracted = 0
    
    with Pool(processes=max(1, cpu_count() - 1)) as pool:
        tasks = []
        for i, chunk in enumerate(reader):
            tasks.append((chunk, i * READ_CHUNK_SIZE))
        
        print(f"  数据已切分为 {len(tasks)} 块，开始并行计算...")
        
        with tqdm(total=len(tasks), desc="流式处理进度") as pbar:
            for res_df in pool.imap_unordered(process_chunk_data, tasks):
                if res_df is not None and not res_df.empty:
                    res_df.to_csv(QUEUE_PATH, mode='a', index=False, header=False, encoding='utf-8-sig')
                    total_extracted += len(res_df)
                pbar.update(1)
    
    print(f"\n  提取完成！共写入 {total_extracted:,} 条片段")
    
    # 3. 生成Result框架
    print(f"\n[3/4] 生成Result表框架（仅读取案号）...")
    df_case = pd.read_csv(INPUT_CSV, encoding='utf-8', usecols=['案号'])
    result_df = pd.DataFrame({
        '文件': '201601',
        '序号': np.arange(1, len(df_case) + 1),
        '案号': df_case['案号'].fillna(''),
        '来源': '待处理'
    })
    for role in ROLES:
        result_df[role] = ''
    result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
    
    end_time = time.time()
    print(f"\n阶段1完成！总耗时: {end_time - start_time:.2f} 秒")
    print(f"处理速度: {len(df_case) / (end_time - start_time):.0f} 行/秒")
    
    # ============================================================
    # 阶段1.1：执行铁血版规则提取
    # ============================================================
    print("\n" + "=" * 80)
    print("阶段1.1：执行铁血版规则提取")
    print("=" * 80)
    
    result = subprocess.run(
        [sys.executable, str(BASE / "run_phase1_1_ultimate.py")],
        cwd=str(BASE)
    )
    
    if result.returncode != 0:
        print("\n❌ 阶段1.1执行失败")
        sys.exit(1)
    
    # ============================================================
    # 阶段1.2：自动合并结果到result.csv
    # ============================================================
    print("\n" + "=" * 80)
    print("阶段1.2：自动合并结果到result.csv")
    print("=" * 80)
    
    print("\n读取处理结果...")
    queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
    result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)
    
    processed = queue_df[queue_df['状态'] == '已处理(终极版)'].copy()
    print(f"已处理: {len(processed):,} 条")
    
    if len(processed) > 0:
        print("\n合并到result表...")
        
        # 按序号和角色分组，合并姓名（同角色多名用分号分隔）
        grouped = processed.groupby(['序号', '角色'])['AI提取姓名'].apply(
            lambda x: ';'.join(x.dropna().unique())
        ).reset_index()
        
        print(f"合并后: {len(grouped):,} 个唯一(序号,角色)组合")
        
        # 更新result表
        for role in tqdm(ROLES, desc="更新角色"):
            role_data = grouped[grouped['角色'] == role]
            if len(role_data) == 0:
                continue
            
            name_map = dict(zip(role_data['序号'].astype(int), role_data['AI提取姓名']))
            result_df[role] = result_df['序号'].astype(int).map(name_map).fillna(result_df[role])
        
        # 更新来源
        processed_rows = grouped['序号'].astype(int).unique()
        result_df.loc[result_df['序号'].astype(int).isin(processed_rows), '来源'] = '规则(终极版)'
        
        result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
        print(f"\n已更新: {RESULT_PATH}")
        
        filled_count = (result_df[ROLES] != '').sum().sum()
        print(f"\n统计:")
        print(f"  总行数: {len(result_df):,}")
        print(f"  已填充姓名: {filled_count:,}")
        print(f"  填充率: {filled_count / (len(result_df) * len(ROLES)) * 100:.1f}%")
    
    # ============================================================
    # 最终统计
    # ============================================================
    print("\n" + "=" * 80)
    print("✅ 完整流程执行完成")
    print("=" * 80)
    
    total_snippets = len(queue_df)
    processed_snippets = len(queue_df[queue_df['状态'] == '已处理(终极版)'])
    pending_snippets = len(queue_df[queue_df['状态'] == '待处理'])
    
    print(f"""
总片段数: {total_snippets:,}
  已处理: {processed_snippets:,} ({processed_snippets/total_snippets*100:.1f}%)
  待处理: {pending_snippets:,} ({pending_snippets/total_snippets*100:.1f}%)

结果文件:
  {RESULT_PATH}
  {QUEUE_PATH}

下一步:
  - 查看result.csv中的提取结果
  - 待处理数据可交由阶段2（AI）处理
""")
