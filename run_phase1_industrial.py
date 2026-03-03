#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段1 工业级并发版：多进程 + RuleExtractor直接提取
- 调用 RuleExtractor.extract_fulltext() 提取确定的名字 → 直接写入result.csv
- 不确定的片段 → 写入ai_queue.csv给阶段1.1处理
- 多进程加速处理
"""
import sys
import io
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.rule_extractor import RuleExtractor
from models import Person

BASE = Path(__file__).parent

# ============================================================
# 配置
# ============================================================
INPUT_CSV = BASE / "input" / "2016年裁判文书数据" / "2016年裁判文书数据_马克数据网" / "2016年01月裁判文书数据.csv"
OUTPUT_DIR = BASE / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

QUEUE_PATH = OUTPUT_DIR / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = OUTPUT_DIR / "2016年01月裁判文书数据_result.csv"

ROLES = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员', '人民陪审员']

def process_chunk(chunk_data):
    """
    单个进程的处理函数：
    接收一个DataFrame切片，返回提取后的结果
    """
    chunk_df, start_idx = chunk_data
    
    result_rows = []
    queue_rows = []
    
    for idx, row in chunk_df.iterrows():
        row_num = idx + 1
        full_text = str(row.get('全文', '')).strip()
        case_no = str(row.get('案号', ''))
        
        if not full_text or full_text == 'nan':
            # 空文本，创建空结果行
            result_row = {
                '文件': '201601',
                '序号': row_num,
                '案号': case_no,
                '来源': '规则(无内容)'
            }
            for role in ROLES:
                result_row[role] = ''
            result_rows.append(result_row)
            continue
        
        # 调用 RuleExtractor 提取
        certain_persons, uncertain_snippets = RuleExtractor.extract_fulltext(full_text)
        
        # 构建result行
        result_row = {
            '文件': '201601',
            '序号': row_num,
            '案号': case_no,
            '来源': '规则' if certain_persons else '待处理'
        }
        
        # 按角色分组确定的名字
        role_names = {role: [] for role in ROLES}
        for person in certain_persons:
            if person.role in role_names:
                role_names[person.role].append(person.name)
        
        # 填充角色列（同角色多名用分号分隔）
        for role in ROLES:
            result_row[role] = ';'.join(role_names[role]) if role_names[role] else ''
        
        result_rows.append(result_row)
        
        # 不确定的片段加入AI队列
        for snippet in uncertain_snippets:
            queue_row = {
                '文件': '201601',
                '序号': row_num,
                '案号': case_no,
                '角色': snippet.role,
                '片段': snippet.snippet,
                '位置': snippet.position,
                'snippet_id': f"{row_num}_{snippet.position}",
                '状态': '待处理',
                'AI提取姓名': ''
            }
            queue_rows.append(queue_row)
    
    result_df = pd.DataFrame(result_rows) if result_rows else None
    queue_df = pd.DataFrame(queue_rows) if queue_rows else None
    
    return result_df, queue_df

if __name__ == '__main__':
    print("=" * 80)
    print("阶段1：工业级并发版（RuleExtractor + 多进程）")
    print("=" * 80)
    print(f"\n输入: {INPUT_CSV}")
    print(f"输出: {QUEUE_PATH}")
    print(f"      {RESULT_PATH}\n")
    
    start_time = time.time()
    
    # 1. 读取数据
    print("\n[1/4] 正在读取CSV...")
    try:
        df = pd.read_csv(INPUT_CSV, encoding='utf-8', usecols=['全文', '案号'], engine='pyarrow')
        print("  使用pyarrow引擎加速读取")
    except:
        df = pd.read_csv(INPUT_CSV, encoding='utf-8', usecols=['全文', '案号'])
        print("  使用默认引擎读取")
    
    df['全文'] = df['全文'].fillna('')
    df['案号'] = df['案号'].fillna('')
    
    total_rows = len(df)
    print(f"  数据加载完成，共 {total_rows:,} 行")
    
    # 2. 多进程切分
    num_cores = max(1, cpu_count() - 1)
    chunk_size = int(np.ceil(total_rows / num_cores))
    
    print(f"\n[2/4] 启动多进程提取（{num_cores}核并发）...")
    print(f"  每块约 {chunk_size:,} 行")
    
    chunks = []
    for i in range(num_cores):
        start_i = i * chunk_size
        end_i = min((i + 1) * chunk_size, total_rows)
        if start_i >= total_rows:
            break
        chunks.append((df.iloc[start_i:end_i].copy(), start_i))
    
    # 3. 并行执行
    print(f"\n[3/4] 并行处理中...")
    result_parts = []
    queue_parts = []
    
    with Pool(processes=num_cores) as pool:
        for result_df, queue_df in tqdm(pool.imap(process_chunk, chunks), total=len(chunks), desc="并发进度"):
            if result_df is not None:
                result_parts.append(result_df)
            if queue_df is not None:
                queue_parts.append(queue_df)
    
    # 4. 合并结果
    print(f"\n[4/4] 合并并保存结果...")
    
    if result_parts:
        final_result = pd.concat(result_parts, ignore_index=True)
        final_result = final_result.sort_values('序号').reset_index(drop=True)
    else:
        final_result = pd.DataFrame(columns=['文件', '序号', '案号', '来源'] + ROLES)
    
    if queue_parts:
        final_queue = pd.concat(queue_parts, ignore_index=True)
    else:
        final_queue = pd.DataFrame(columns=['文件', '序号', '案号', '角色', '片段', '位置', 'snippet_id', '状态', 'AI提取姓名'])
    
    # 保存文件
    final_result.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
    final_queue.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')
    
    end_time = time.time()
    
    # 统计
    print("\n" + "=" * 80)
    print("✅ 阶段1完成")
    print("=" * 80)
    
    filled_count = (final_result[ROLES] != '').sum().sum()
    rule_extracted = len(final_result[final_result['来源'] == '规则'])
    pending_count = len(final_result[final_result['来源'] == '待处理'])
    
    print(f"""
总耗时: {end_time - start_time:.2f} 秒
处理速度: {total_rows / (end_time - start_time):.0f} 行/秒

Result表统计:
  总行数: {len(final_result):,}
  规则提取成功: {rule_extracted:,} ({rule_extracted/total_rows*100:.1f}%)
  待AI处理: {pending_count:,} ({pending_count/total_rows*100:.1f}%)
  已填充姓名数: {filled_count:,}

AI队列统计:
  待处理片段: {len(final_queue):,}

输出文件:
  {RESULT_PATH}
  {QUEUE_PATH}

下一步:
  运行阶段1.1处理AI队列:
  python run_phase1_1_ultimate.py
""")
