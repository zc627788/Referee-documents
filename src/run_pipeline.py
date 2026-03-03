#!/usr/bin/env python3
"""
裁判文书角色提取 v6.0 — Phase 1 / Phase 2 / Retry 分离

用法：
  # 阶段一：规则提取（全文扫描 + 边界判断）
  python src/run_pipeline.py phase1 --input <CSV路径> [--output-dir data/output] [--limit N]

  # 阶段二：AI精修（处理 ai_queue 中的不确定片段）
  python src/run_pipeline.py phase2 --output-dir data/output

  # 重试：仅处理阶段二中失败的片段
  python src/run_pipeline.py retry --output-dir data/output

  # 查看进度
  python src/run_pipeline.py status --output-dir data/output
"""
import argparse
import sys
import json
import time
import re
from pathlib import Path
from typing import List, Dict
from collections import defaultdict

import pandas as pd
from tqdm import tqdm

# 把 src 目录加入路径
sys.path.insert(0, str(Path(__file__).parent))

# 确保控制台输出使用 UTF-8（Windows 终端兼容）
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from core import (
    Config, RuleExtractor,
    persons_to_wide_row, IncrementalCSVWriter, ROLE_COLUMNS,
)
from core.rule_extractor import UncertainSnippet
from core.enhanced_rule_extractor import EnhancedRuleExtractor
from models import Person

# GLM4Extractor 延迟导入（只在Phase2需要时导入）


# ─────────────────────────────────────────────────────────────
# 辅助函数
# ─────────────────────────────────────────────────────────────
def _extract_file_label(filename: str) -> str:
    """从文件名提取简短标识，如 '2016年01月裁判文书数据' -> '201601'"""
    m = re.search(r'(\d{4}).*?(\d{1,2})月', filename)
    if m:
        return f"{m.group(1)}{int(m.group(2)):02d}"
    digits = re.findall(r'\d+', filename)
    return ''.join(digits[:2]) if digits else filename[:10]


def _find_output_files(output_dir: Path):
    """在输出目录中查找 result / ai_queue 文件"""
    results = list(output_dir.glob('*_result.csv'))
    queues = list(output_dir.glob('*_ai_queue.csv'))
    if not results:
        print(f'[ERROR] 输出目录 {output_dir} 中未找到 *_result.csv')
        print('  请先运行 phase1 生成结果文件')
        sys.exit(1)
    if len(results) > 1:
        print(f'[INFO] 发现多个 result 文件：')
        for i, r in enumerate(results, 1):
            print(f'  {i}. {r.name}')
        print(f'  使用最新的: {results[-1].name}')
    return results[-1], queues[-1] if queues else None


# ─────────────────────────────────────────────────────────────
# AI 批量提取（token 打包 + 并发）
# ─────────────────────────────────────────────────────────────
def _ai_batch_extract(snippets: List[Dict], config: Config, log_file: Path = None, response_dir: Path = None) -> Dict[str, Dict]:
    """
    使用GLM-4批量提取（一次请求最多500条）
    
    Args:
        snippets: 待处理的片段列表 [{'id': str, 'role': str, 'text': str}, ...]
        config: 配置对象
        log_file: AI处理日志文件路径
        response_dir: 原始响应对话存储目录
    
    Returns:
        提取结果映射 {snippet_id: {'name': str, 'role': str}}
    """
    if not snippets:
        return {}
    
    import datetime
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 延迟导入GLM4Extractor
    from core.glm4_extractor import GLM4Extractor
    extractor = GLM4Extractor(config)
    
    # 批量处理 (修复 Bug: 传入 log_file 和 response_dir)
    result_map = extractor.extract_batch(snippets, log_file=log_file, response_dir=response_dir)
    
    return result_map


def _merge_ai_results(result_csv: Path, queue_df: pd.DataFrame,
                      ai_map: Dict[str, Dict], file_label: str):
    """
    将 AI 结果合并回 result.csv，更新 ai_queue 的状态列。
    返回: (merged_count, new_count, failed_ids)
    """
    # 读取现有 result
    existing_df = None
    if result_csv.exists() and result_csv.stat().st_size > 0:
        try:
            # 增加 low_memory=False 解决 DtypeWarning，统一转为字符串处理
            existing_df = pd.read_csv(str(result_csv), encoding='utf-8-sig', low_memory=False)
        except:
            existing_df = None

    # 按 row_idx 分组 AI 结果
    ai_by_row: Dict[int, List[Person]] = defaultdict(list)
    processed_ids = set()
    for sid, info in ai_map.items():
        parts = sid.split('_')
        ridx = int(parts[0])
        ai_by_row[ridx].append(Person(name=info['name'], role=info['role']))
        processed_ids.add(sid)

    merged_count = 0
    new_count = 0

    if existing_df is not None and len(existing_df) > 0:
        idx_map = {}
        for df_idx, df_row in existing_df.iterrows():
            idx_map[int(df_row['序号'])] = df_idx

        for ridx, ai_persons in ai_by_row.items():
            row_num = ridx + 1
            if row_num in idx_map:
                df_idx = idx_map[row_num]
                for p in ai_persons:
                    if p.role in existing_df.columns:
                        old_val = str(existing_df.at[df_idx, p.role]) if pd.notna(existing_df.at[df_idx, p.role]) else ''
                        # 保持有序列表去重（避免 set 乱序，保证输出稳定）
                        old_names = [n for n in old_val.split(';') if n] if old_val else []
                        if p.name not in old_names:
                            old_names.append(p.name)
                            existing_df.at[df_idx, p.role] = ';'.join(old_names)
                existing_df.at[df_idx, '来源'] = '规则+ai'
                curr_flag = str(existing_df.at[df_idx, 'flag']) if pd.notna(existing_df.at[df_idx, 'flag']) else ''
                
                # 当前处理的角色集合
                processed_roles = {p.role for p in ai_persons}
                
                if '需要AI处理:' in curr_flag:
                    # 解析当前的待处理和已处理
                    parts = curr_flag.split('已用ai处理:')
                    needs_ai_part = parts[0].replace('需要AI处理:', '').strip()
                    done_ai_part = parts[1].strip() if len(parts) > 1 else ''
                    
                    needs_roles = [r.strip() for r in needs_ai_part.split(',') if r.strip()]
                    done_roles = [r.strip() for r in done_ai_part.split(',') if r.strip()]
                    
                    # 移除已处理的角色，加入已完成集合
                    remaining_needs = [r for r in needs_roles if r not in processed_roles]
                    new_done_roles = list(set(done_roles + list(processed_roles)))
                    
                    new_flag = ''
                    if remaining_needs:
                        new_flag += f"需要AI处理: {', '.join(remaining_needs)}"
                    if new_done_roles:
                        if new_flag: new_flag += " "
                        new_flag += f"已用ai处理: {', '.join(new_done_roles)}"
                        
                    existing_df.at[df_idx, 'flag'] = new_flag.strip()
                else:
                    existing_df.at[df_idx, 'flag'] = f"已用ai处理: {', '.join(processed_roles)}"
                
                # 最终校验：如果合并后 7 个角色均为空，强制 NA
                role_cols = [c for c in existing_df.columns if c not in ['文件', '序号', '案号', 'flag', '来源']]
                vals = [str(existing_df.at[df_idx, c]) for c in role_cols if pd.notna(existing_df.at[df_idx, c])]
                non_empty_vals = [v for v in vals if v.strip() and v.lower() != 'nan']
                if not non_empty_vals:
                    existing_df.at[df_idx, 'flag'] = 'NA'
                
                merged_count += 1
            else:
                case_no_mask = queue_df[queue_df['snippet_id'].apply(
                    lambda x: str(x).startswith(f'{ridx}_'))]
                case_no = str(case_no_mask.iloc[0]['案号']) if len(case_no_mask) > 0 else ''
                new_row = persons_to_wide_row(ai_persons, file_label, row_num, case_no, source='规则+ai')
                new_row['flag'] = '已用ai处理'
                existing_df = pd.concat([existing_df, pd.DataFrame([new_row])], ignore_index=True)
                new_count += 1

        existing_df.to_csv(str(result_csv), index=False, encoding='utf-8-sig')
    else:
        # 没有 result 文件，创建新的
        rows = []
        for ridx, ai_persons in ai_by_row.items():
            case_no_mask = queue_df[queue_df['snippet_id'].apply(
                lambda x: str(x).startswith(f'{ridx}_'))]
            case_no = str(case_no_mask.iloc[0]['案号']) if len(case_no_mask) > 0 else ''
            new_row = persons_to_wide_row(ai_persons, file_label, ridx + 1, case_no, source='规则+ai')
            new_row['flag'] = '已用ai处理'
            rows.append(new_row)
            new_count += 1
        if rows:
            pd.DataFrame(rows).to_csv(str(result_csv), index=False, encoding='utf-8-sig')

    return merged_count, new_count, processed_ids


# ─────────────────────────────────────────────────────────────
# 子命令：phase1 — 规则提取
# ─────────────────────────────────────────────────────────────
def cmd_phase1(args):
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] 文件不存在: {input_path}")
        sys.exit(1)

    file_name = input_path.stem
    file_label = _extract_file_label(file_name)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_csv = output_dir / f'{file_name}_result.csv'

    config     = Config()
    chunk_size = config.get('processing.chunk_size', 5000)
    # 输出文件存在则覆盖（不再提供SQLite断点续跑）
    ai_queue_csv = output_dir / f'{file_name}_ai_queue.csv'
    if output_csv.exists():
        output_csv.unlink()
    if ai_queue_csv.exists():
        ai_queue_csv.unlink()

    writer = IncrementalCSVWriter(str(output_csv), ROLE_COLUMNS)

    # AI队列 — 带状态列
    AI_QUEUE_COLS = ['文件', '序号', '案号', '角色', '片段', '位置', 'snippet_id', '状态']
    ai_writer = IncrementalCSVWriter(str(ai_queue_csv), AI_QUEUE_COLS)

    stats = dict(read=0, rule_ok=0, uncertain=0, no_role=0, empty_text=0,
                 enhanced_extracted=0, enhanced_filtered=0)
    seen_snippet_ids = set()

    print(f"\n{'='*60}")
    print(f"  Phase 1: 规则提取（全文扫描 + 边界判断）")
    print(f"{'='*60}")
    print(f"  输入     : {input_path}")
    print(f"  提取结果 : {output_csv}")
    print(f"  AI队列   : {ai_queue_csv}")
    print("  断点续传 : 已关闭(SQLite未启用)\n")

    try:
        reader = pd.read_csv(
            input_path,
            chunksize=chunk_size,
            encoding='utf-8',
            on_bad_lines='skip',
            usecols=lambda c: c in ['全文', '案号', '裁判日期', '法院'],
            low_memory=False # 统一读取，防止 DtypeWarning
        )
        pbar = tqdm(desc='Phase 1 扫描', unit='条', mininterval=2.0)

        for chunk in reader:
            result_rows = []
            queue_rows = []
            for row in chunk.itertuples(index=True):
                row_idx = row.Index
                pbar.update(1)
                stats['read'] += 1

                if args.limit and stats['read'] > args.limit:
                    raise StopIteration

                full_text_raw = getattr(row, '全文', None)
                case_no = str(getattr(row, '案号', '') or '')

                # 空文本
                if full_text_raw is None or (isinstance(full_text_raw, float) and str(full_text_raw) == 'nan'):
                    stats['empty_text'] += 1
                    result = persons_to_wide_row([], file_label, row_idx + 1, case_no, source='规则')
                    result_rows.append(result)
                    continue

                full_text = str(full_text_raw).strip()
                if not full_text or full_text == 'nan':
                    stats['empty_text'] += 1
                    result = persons_to_wide_row([], file_label, row_idx + 1, case_no, source='规则')
                    result_rows.append(result)
                    continue

                # 全文扫描提取
                certain, uncertain = RuleExtractor.extract_fulltext(full_text)
                has_any_role_terms = bool(certain) or bool(uncertain)
                
                # 先用增强规则处理不确定片段
                enhanced_extracted = []  # 增强规则提取的人员
                still_uncertain = []     # 仍需AI处理的片段
                
                if uncertain:
                    stats['uncertain'] += len(uncertain)
                    
                    for u in uncertain:
                        # 尝试用增强规则提取
                        name = EnhancedRuleExtractor.try_extract(u.snippet, u.role)
                        
                        if name is None:
                            # 仍不确定，稍后加入AI队列
                            still_uncertain.append(u)
                        elif name != "":
                            # 增强规则提取到姓名
                            enhanced_extracted.append(Person(name=name, role=u.role))
                            stats['enhanced_extracted'] += 1
                        else:
                            # name == "" 表示确定无姓名
                            stats['enhanced_filtered'] += 1
                
                # 合并所有提取到的人员
                all_persons = certain + enhanced_extracted
                ai_roles = [u.role for u in still_uncertain] if still_uncertain else None
                
                # 写入result
                if all_persons or still_uncertain:
                    if certain:
                        stats['rule_ok'] += 1
                    source = '规则+增强' if enhanced_extracted else '规则'
                    result = persons_to_wide_row(all_persons, file_label, row_idx + 1, case_no, source=source, ai_roles=ai_roles)
                    result_rows.append(result)
                else:
                    stats['no_role'] += 1
                    result = persons_to_wide_row([], file_label, row_idx + 1, case_no, source='规则')
                    result_rows.append(result)
                
                # 收集仍需AI处理的片段
                if still_uncertain:
                    for u in still_uncertain:
                        snippet_id = f"{row_idx}_{u.position}"
                        if snippet_id in seen_snippet_ids:
                            continue
                        seen_snippet_ids.add(snippet_id)
                        cleaned_snippet = RuleExtractor._clean_ai_snippet(u.snippet)
                        queue_rows.append({
                            '文件': file_label, '序号': row_idx + 1,
                            '案号': case_no, '角色': u.role,
                            '片段': cleaned_snippet, '位置': u.position,
                            'snippet_id': snippet_id, '状态': '待处理'
                        })
            if result_rows:
                writer.append_many(result_rows)
            if queue_rows:
                ai_writer.append_many(queue_rows)
        pbar.close()

    except StopIteration:
        pbar.close()
        print(f"\n[已达 limit={args.limit}，停止]")
    except KeyboardInterrupt:
        pbar.close()
        print('\n[中断] 未启用断点续传，请重新运行任务')

    # 统计
    processed = stats['read']
    print('\n' + '-' * 60)
    print('Phase 1 完成')
    print('-' * 60)
    print(f'  读取总数   : {stats["read"]:>10,}')
    print(f'  本次处理   : {processed:>10,}')
    if processed:
        print(f'  确定提取   : {stats["rule_ok"]:>10,}  ({stats["rule_ok"]/processed*100:.1f}%)')
        print(f'  不确定片段 : {stats["uncertain"]:>10,}')
        if stats["uncertain"] > 0:
            print(f'    - 增强规则提取: {stats["enhanced_extracted"]:>6,}  ({stats["enhanced_extracted"]/stats["uncertain"]*100:.1f}%)')
            print(f'    - 过滤无姓名  : {stats["enhanced_filtered"]:>6,}  ({stats["enhanced_filtered"]/stats["uncertain"]*100:.1f}%)')
        print(f'  空文本     : {stats["empty_text"]:>10,}  ({stats["empty_text"]/processed*100:.1f}%)')
        print(f'  无角色词   : {stats["no_role"]:>10,}  ({stats["no_role"]/processed*100:.1f}%)')
    print(f'  结果文件   : {output_csv}')
    
    # 统计AI队列实际数量
    actual_ai_queue = 0
    if ai_queue_csv.exists():
        try:
            queue_df = pd.read_csv(str(ai_queue_csv), encoding='utf-8-sig', low_memory=False)
            actual_ai_queue = len(queue_df)
        except:
            pass
    
    if stats["uncertain"] > 0:
        saved_pct = (stats["uncertain"] - actual_ai_queue) / stats["uncertain"] * 100
        print(f'  AI队列     : {ai_queue_csv}')
        print(f'    - 原始不确定: {stats["uncertain"]:>6,}')
        print(f'    - 最终AI队列: {actual_ai_queue:>6,}')
        print(f'    - 优化节省  : {stats["uncertain"] - actual_ai_queue:>6,}  (节省{saved_pct:.1f}%)')
    else:
        print(f'  AI队列     : 无待处理片段')
    print('-' * 60)
    if stats['uncertain'] > 0:
        print(f'\n[下一步] 运行 phase2 处理不确定片段:')
        print(f'  python src/run_pipeline.py phase2 --output-dir {args.output_dir}')


# ─────────────────────────────────────────────────────────────
# 子命令：phase2 — AI 精修
# ─────────────────────────────────────────────────────────────
def cmd_phase2(args):
    output_dir = Path(args.output_dir)
    result_csv, queue_csv = _find_output_files(output_dir)

    if queue_csv is None or not queue_csv.exists():
        print('[ERROR] 未找到 AI 队列文件 (*_ai_queue.csv)')
        print('  请先运行 phase1')
        sys.exit(1)

    # 读取 AI 队列（明确指定AI提取姓名为字符串类型）
    queue_df = pd.read_csv(str(queue_csv), encoding='utf-8-sig', dtype={'AI提取姓名': str}, low_memory=False)
    if '状态' not in queue_df.columns:
        queue_df['状态'] = '待处理'
    if 'snippet_id' not in queue_df.columns:
        # 兼容旧格式：用行号+位置生成 snippet_id
        queue_df['snippet_id'] = queue_df.apply(
            lambda r: f"{int(r['序号'])-1}_{int(r['位置'])}", axis=1)

    # 筛选待处理的
    pending = queue_df[queue_df['状态'] == '待处理']
    if len(pending) == 0:
        print('[完成] 没有待处理的片段')
        # 检查失败的
        failed = queue_df[queue_df['状态'] == '失败']
        if len(failed) > 0:
            print(f'  但有 {len(failed)} 条失败片段，可运行 retry 重试:')
            print(f'  python src/run_pipeline.py retry --output-dir {args.output_dir}')
        return

    file_label = str(pending.iloc[0]['文件'])
    
    # 创建AI处理日志文件
    log_file = queue_csv.parent / f"{queue_csv.stem}_ai_log.txt"

    print(f"\n{'='*60}")
    print(f"  Phase 2: AI 精修")
    print(f"{'='*60}")
    print(f"  队列文件  : {queue_csv}")
    print(f"  结果文件  : {result_csv}")
    print(f"  日志文件  : {log_file}")
    print(f"  待处理    : {len(pending)} 条\n")

    # 构建 snippets
    snippets = []
    for _, row in pending.iterrows():
        snippets.append({
            'id': str(row['snippet_id']),
            'role': str(row['角色']),
            'text': str(row['片段']),
        })

    config = Config()
    
    # 响应详情输出目录
    response_dir = log_file.parent / "ai_responses"
    
    ai_map = _ai_batch_extract(snippets, config, log_file, response_dir)

    # 更新队列状态和AI提取姓名
    processed_ids = set(ai_map.keys())
    all_pending_ids = set(pending['snippet_id'].astype(str))
    
    # 确保'AI提取姓名'列存在
    if 'AI提取姓名' not in queue_df.columns:
        queue_df['AI提取姓名'] = ''

    for idx, row in queue_df.iterrows():
        sid = str(row['snippet_id'])
        if sid in processed_ids:
            queue_df.at[idx, '状态'] = '已处理'
            # 记录AI提取的姓名
            queue_df.at[idx, 'AI提取姓名'] = ai_map[sid]['name']
        elif sid in all_pending_ids and sid not in processed_ids:
            # 发给AI了但没返回结果 — 可能AI认为不是人名(null)或批次失败
            # 如果该 snippet 的批次没失败，说明AI返回了null → 标记为已处理
            # 如果批次失败了 → 标记为失败
            queue_df.at[idx, '状态'] = '已处理'  # 默认已处理(AI认为无人名)
            queue_df.at[idx, 'AI提取姓名'] = '(无)'
            # 加入 ai_map 以便后续清理 result.csv 中的 flag
            ai_map[sid] = {'name': '', 'role': row['角色']}

    # 保存更新后的队列
    queue_df.to_csv(str(queue_csv), index=False, encoding='utf-8-sig')

    # 合并到 result.csv
    if ai_map:
        merged, new, _ = _merge_ai_results(result_csv, queue_df, ai_map, file_label)
        print(f'\n  合并完成: 更新 {merged} 行，新增 {new} 行')

    # 统计
    updated_queue = pd.read_csv(str(queue_csv), encoding='utf-8-sig', low_memory=False)
    total = len(updated_queue)
    done = len(updated_queue[updated_queue['状态'] == '已处理'])
    failed = len(updated_queue[updated_queue['状态'] == '失败'])
    still_pending = len(updated_queue[updated_queue['状态'] == '待处理'])

    print(f'\n{"-"*60}')
    print(f'Phase 2 完成')
    print(f'{"-"*60}')
    print(f'  总片段     : {total:>8,}')
    print(f'  已处理     : {done:>8,}')
    print(f'  失败       : {failed:>8,}')
    print(f'  待处理     : {still_pending:>8,}')
    print(f'{"-"*60}')

    if failed > 0:
        print(f'\n[提示] 有 {failed} 条失败，可运行 retry 重试:')
        print(f'  python src/run_pipeline.py retry --output-dir {args.output_dir}')


# ─────────────────────────────────────────────────────────────
# 子命令：retry — 重试失败项
# ─────────────────────────────────────────────────────────────
def cmd_retry(args):
    output_dir = Path(args.output_dir)
    result_csv, queue_csv = _find_output_files(output_dir)

    if queue_csv is None or not queue_csv.exists():
        print('[ERROR] 未找到 AI 队列文件')
        sys.exit(1)

    queue_df = pd.read_csv(str(queue_csv), encoding='utf-8-sig', dtype={'AI提取姓名': str}, low_memory=False)
    failed = queue_df[queue_df['状态'] == '失败']

    if len(failed) == 0:
        print('[完成] 没有失败的片段需要重试')
        pending = queue_df[queue_df['状态'] == '待处理']
        if len(pending) > 0:
            print(f'  但有 {len(pending)} 条待处理片段，请运行 phase2')
        return

    file_label = str(failed.iloc[0]['文件'])
    
    # 使用同一个日志文件
    log_file = queue_csv.parent / f"{queue_csv.stem}_ai_log.txt"

    print(f"\n{'='*60}")
    print(f"  Retry: 重试失败片段")
    print(f"{'='*60}")
    print(f"  队列文件 : {queue_csv}")
    print(f"  结果文件 : {result_csv}")
    print(f"  日志文件 : {log_file}")
    print(f"  失败片段 : {len(failed)} 条\n")

    snippets = []
    for _, row in failed.iterrows():
        snippets.append({
            'id': str(row['snippet_id']),
            'role': str(row['角色']),
            'text': str(row['片段']),
        })

    config = Config()
    
    # 响应详情输出目录
    response_dir = log_file.parent / "ai_responses"
    
    ai_map = _ai_batch_extract(snippets, config, log_file, response_dir)

    # 更新状态和AI提取姓名
    processed_ids = set(ai_map.keys())
    
    # 确保'AI提取姓名'列存在
    if 'AI提取姓名' not in queue_df.columns:
        queue_df['AI提取姓名'] = ''
    
    for idx, row in queue_df.iterrows():
        sid = str(row['snippet_id'])
        if row['状态'] == '失败':
            if sid in processed_ids:
                queue_df.at[idx, '状态'] = '已处理'
                queue_df.at[idx, 'AI提取姓名'] = ai_map[sid]['name']
            else:
                queue_df.at[idx, '状态'] = '已处理'  # AI返回null也算处理了
                queue_df.at[idx, 'AI提取姓名'] = '(无)'

    queue_df.to_csv(str(queue_csv), index=False, encoding='utf-8-sig')

    if ai_map:
        merged, new, _ = _merge_ai_results(result_csv, queue_df, ai_map, file_label)
        print(f'\n  合并完成: 更新 {merged} 行，新增 {new} 行')

    still_failed = len(queue_df[queue_df['状态'] == '失败'])
    print(f'\n  重试后仍失败: {still_failed} 条')


# ─────────────────────────────────────────────────────────────
# 子命令：status
# ─────────────────────────────────────────────────────────────
def cmd_status(args):
    output_dir = Path(args.output_dir)
    if not output_dir.exists():
        print(f'[ERROR] 输出目录不存在: {output_dir}')
        return

    print(f'\n{"="*60}')
    print(f'  处理状态总览')
    print(f'{"="*60}')

    # Phase 1 进度（基于 CSV 统计）
    results = list(output_dir.glob('*_result.csv'))
    if results:
        print(f'\n  Phase 1 (规则提取):')
        for r in results:
            df = pd.read_csv(str(r), encoding='utf-8-sig', low_memory=False)
            total = len(df)
            pending_ai = 0
            no_signature = 0
            if 'flag' in df.columns:
                flags = df['flag'].fillna('')
                pending_ai = flags.str.startswith('需要AI处理').sum()
                no_signature = (flags == 'NA').sum()
            done = total - pending_ai - no_signature
            print(f'    {r.name}:')
            print(f'      已完成   : {done:>8,}')
            print(f'      待AI处理 : {pending_ai:>8,}')
            print(f'      无角色词 : {no_signature:>8,}')
            print(f'      合计     : {total:>8,}')
    else:
        print(f'\n  Phase 1 (规则提取): 未找到结果文件')

    # Result CSV
    for r in results:
        df = pd.read_csv(str(r), encoding='utf-8-sig', low_memory=False)
        if '来源' in df.columns:
            src_dist = df['来源'].value_counts()
            print(f'\n  结果文件: {r.name} ({len(df)} 行)')
            for src, cnt in src_dist.items():
                print(f'    {src:>6} : {cnt:>8,}')
        else:
            print(f'\n  结果文件: {r.name} ({len(df)} 行)')

    # AI Queue
    queues = list(output_dir.glob('*_ai_queue.csv'))
    for q in queues:
        qdf = pd.read_csv(str(q), encoding='utf-8-sig', low_memory=False)
        print(f'\n  AI队列: {q.name} ({len(qdf)} 条)')
        if '状态' in qdf.columns:
            st_dist = qdf['状态'].value_counts()
            for st, cnt in st_dist.items():
                print(f'    {st:>6} : {cnt:>8,}')
        else:
            print(f'    (旧格式，无状态列)')

    print(f'\n{"="*60}')


# ─────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='裁判文书角色提取系统 v6.0',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    # phase1
    p1 = sub.add_parser('phase1', help='阶段一：规则提取（全文扫描 + 边界判断）')
    p1.add_argument('--input',      required=True,            help='输入 CSV 文件路径')
    p1.add_argument('--output-dir', default='data/output',    help='输出目录')
    p1.add_argument('--limit',      type=int, default=None,   help='处理行数上限（测试用）')

    # phase2
    p2 = sub.add_parser('phase2', help='阶段二：AI精修（处理不确定片段）')
    p2.add_argument('--output-dir', default='data/output',    help='输出目录')

    # retry
    p3 = sub.add_parser('retry', help='重试：仅处理阶段二中失败的片段')
    p3.add_argument('--output-dir', default='data/output',    help='输出目录')

    # status
    p4 = sub.add_parser('status', help='查看处理状态')
    p4.add_argument('--output-dir', default='data/output',    help='输出目录')

    args = parser.parse_args()

    if args.command == 'phase1':
        cmd_phase1(args)
    elif args.command == 'phase2':
        cmd_phase2(args)
    elif args.command == 'retry':
        cmd_retry(args)
    elif args.command == 'status':
        cmd_status(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
