#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""清理无用的旧脚本"""
import sys
import io
import os
from pathlib import Path

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE = Path(__file__).parent

# 定义要删除的无用脚本
SCRIPTS_TO_DELETE = [
    # 旧版本的阶段1.1脚本
    "src/run_phase1_1.py",
    "src/run_phase1_1_clean.py",
    "src/run_phase1_1_multiprocess.py",
    "src/run_phase1_1_parallel.py",
    "run_phase1_1_final.py",
    
    # 测试和分析脚本（已完成任务）
    "analyze_name_lengths.py",
    "analyze_pending.py",
    "analyze_ultimate_results.py",
    "check_fixes.py",
    "view_errors.py",
    "find_invalid_names.py",
    "test_ironclad_cleaning.py",
    "test_unified_vars.py",
    "ironclad_validation_report.py",
    "patch_1_3_report.py",
    "final_quality_report.py",
    "final_summary.py",
    "final_summary_all_phases.py",
    
    # 重置脚本（保留reset_ultimate.py作为通用重置）
    "reset_jieba_quick.py",
    
    # 临时文件
    "invalid_names_report.csv",
    
    # 旧的清理脚本
    "cleanup_unused_scripts.py",
]

print("=" * 80)
print("清理无用脚本")
print("=" * 80)

deleted_count = 0
not_found_count = 0

for script in SCRIPTS_TO_DELETE:
    file_path = BASE / script
    if file_path.exists():
        try:
            os.remove(file_path)
            print(f"✓ 已删除: {script}")
            deleted_count += 1
        except Exception as e:
            print(f"✗ 删除失败: {script} - {e}")
    else:
        print(f"- 不存在: {script}")
        not_found_count += 1

print("\n" + "=" * 80)
print("清理完成")
print("=" * 80)
print(f"""
已删除: {deleted_count} 个文件
不存在: {not_found_count} 个文件

保留的核心脚本:
  - run_phase1.py (阶段1主脚本)
  - run_phase1_1_ultimate.py (阶段1.1实现)
  - reset_ultimate.py (重置脚本)
  - fix_multi_names.py (多名字修复)
""")
