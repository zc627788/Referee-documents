#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段1快速版：向量化批量处理原始CSV
从原始CSV直接生成ai_queue和result，速度提升10倍
"""
import sys
import io
import pandas as pd
import re
from pathlib import Path
from tqdm import tqdm

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.rule_extractor import RuleExtractor, ROLE_KEYWORDS

BASE = Path(__file__).parent

print("=" * 80)
print("阶段1快速版：向量化批量处理")
print("=" * 80)

# 输入输出路径
INPUT_CSV = BASE / "input" / "2016年裁判文书数据" / "2016年裁判文书数据_马克数据网" / "2016年01月裁判文书数据.csv"
OUTPUT_DIR = BASE / "data" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

QUEUE_PATH = OUTPUT_DIR / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = OUTPUT_DIR / "2016年01月裁判文书数据_result.csv"

print(f"\n输入: {INPUT_CSV}")
print(f"输出: {QUEUE_PATH}")
print(f"      {RESULT_PATH}")

# ============================================================
# 步骤1: 读取原始数据
# ============================================================
print("\n步骤1: 读取原始数据...")
df = pd.read_csv(INPUT_CSV, encoding='utf-8', usecols=['全文', '案号', '裁判日期', '法院'])
print(f"总行数: {len(df):,}")

# ============================================================
# 步骤2: 向量化提取所有角色片段
# ============================================================
print("\n步骤2: 向量化提取角色片段...")

# 为每个角色构建正则模式
ROLES = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员', '人民陪审员']

all_snippets = []

for role in tqdm(ROLES, desc="提取角色片段"):
    # 构建角色关键词模式
    role_pattern = f"(?:{role}|{'|'.join(ROLE_KEYWORDS.get(role, [role]))})"
    
    # 向量化提取：找到包含角色关键词的片段
    # 简化版：提取角色词前后各50字
    pattern = f"(.{{0,50}}){role_pattern}(.{{0,50}})"
    
    matches = df['全文'].str.extractall(pattern)
    
    if len(matches) > 0:
        for idx, match in matches.iterrows():
            row_idx = idx[0]  # 原始行号
            snippet = match[0] + role + match[1]  # 拼接片段
            
            all_snippets.append({
                '文件': '201601',
                '序号': row_idx + 1,
                '案号': df.at[row_idx, '案号'] if '案号' in df.columns else '',
                '角色': role,
                '片段': snippet.strip(),
                '位置': 0,
                'snippet_id': f"{row_idx}_{len(all_snippets)}",
                '状态': '待处理',
                'AI提取姓名': '',
            })

print(f"提取到 {len(all_snippets):,} 个角色片段")

# ============================================================
# 步骤3: 生成ai_queue
# ============================================================
print("\n步骤3: 生成ai_queue...")
queue_df = pd.DataFrame(all_snippets)
queue_df.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')
print(f"已保存: {QUEUE_PATH}")

# ============================================================
# 步骤4: 生成result（空框架）
# ============================================================
print("\n步骤4: 生成result框架...")

# 创建result表结构
result_data = []
for idx in range(len(df)):
    row = {
        '文件': '201601',
        '序号': idx + 1,
        '案号': df.at[idx, '案号'] if '案号' in df.columns else '',
        '来源': '待处理',
    }
    # 添加所有角色列
    for role in ROLES:
        row[role] = ''
    
    result_data.append(row)

result_df = pd.DataFrame(result_data)
result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig')
print(f"已保存: {RESULT_PATH}")

print("\n" + "=" * 80)
print("阶段1快速版完成")
print("=" * 80)
print(f"""
总行数: {len(df):,}
提取片段: {len(all_snippets):,}

下一步：
  python run_phase1_1_ultimate.py  # 执行阶段1.1提取姓名
""")
