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

from core import (
    Config, RuleExtractor,
    ProgressDB, persons_to_wide_row, IncrementalCSVWriter, ROLE_COLUMNS,
)
from core.rule_extractor import UncertainSnippet
from models import Person


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
def _ai_batch_extract(snippets: List[Dict], config: Config) -> Dict[str, Dict]:
    """
    按 token 估算将片段打包成请求，返回 {snippet_id: {'name': '...', 'role': '...'}}
    失败的片段不会出现在返回值中。
    """
    import httpx
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    api_key = config.get('api.api_key', '')
    if not api_key:
        print('[WARN] API key 未配置，跳过 AI 精修')
        return {}

    base_url = config.get('api.base_url', 'https://vectorengine.ai/v1')
    model = config.get('api.model', 'glm-4')
    timeout = config.get('api.timeout', 120)
    concurrency = config.get('api.concurrency', 5)
    max_retries = config.get('api.max_retries', 3)

    MAX_INPUT_TOKENS = 80000
    MAX_ITEMS_PER_REQUEST = 500
    PROMPT_OVERHEAD = 200
    PER_ITEM_OUTPUT_TOKENS = 25

    def estimate_tokens(text: str) -> int:
        cn = sum(1 for c in text if '\u4e00' <= c <= '\u9fff')
        return int(cn * 2 + (len(text) - cn) * 0.5)

    # 按 token + 条数上限分组
    mega_batches = []
    current_batch, current_tokens = [], PROMPT_OVERHEAD
    for s in snippets:
        item_text = f'[ID:{s["id"]}] 角色:{s["role"]} 文本:"{s["text"]}"'
        item_tokens = estimate_tokens(item_text) + PER_ITEM_OUTPUT_TOKENS
        if (current_tokens + item_tokens > MAX_INPUT_TOKENS or len(current_batch) >= MAX_ITEMS_PER_REQUEST) and current_batch:
            mega_batches.append(current_batch)
            current_batch, current_tokens = [], PROMPT_OVERHEAD
        current_batch.append(s)
        current_tokens += item_tokens
    if current_batch:
        mega_batches.append(current_batch)

    print(f'  打包: {len(snippets)} 条 -> {len(mega_batches)} 个API请求 '
          f'(每请求 ~{len(snippets)//max(len(mega_batches),1)} 条)')

    result_map = {}  # id -> {'name': ..., 'role': ...}
    lock = threading.Lock()
    errors = [0]
    pbar = tqdm(total=len(snippets), desc='AI精修', unit='条', mininterval=1.0)

    def process_mega_batch(batch):
        items_text = '\n'.join([
            f'{i+1}. [ID:{s["id"]}] 角色:{s["role"]} 文本:"{s["text"]}"'
            for i, s in enumerate(batch)
        ])
        prompt = (
            "以下是中国法律文书中的文本片段，每个片段包含一个角色关键词。\n"
            "请提取该角色对应的人名（仅中国人姓名，2-4个汉字），也有可能包含少数民族姓名。\n"
            "如果没有有效人名（如后续是机构名、动词等），返回null。\n\n"
            f"{items_text}\n\n"
            "严格按JSON数组返回，每条对应一个输入，不要输出任何其他文字：\n"
            '[{"id":"对应ID","name":"姓名或null"}]'
        )
        max_out_tokens = len(batch) * PER_ITEM_OUTPUT_TOKENS + 100

        for attempt in range(max_retries):
            try:
                with httpx.Client(timeout=timeout) as client:
                    resp = client.post(
                        f'{base_url}/chat/completions',
                        headers={'Authorization': f'Bearer {api_key}',
                                 'Content-Type': 'application/json'},
                        json={
                            'model': model,
                            'messages': [{'role': 'user', 'content': prompt}],
                            'temperature': 0.1,
                            'max_tokens': max_out_tokens,
                        }
                    )
                    resp.raise_for_status()
                    content = resp.json()['choices'][0]['message']['content']
                    json_match = re.search(r'\[.*\]', content, re.DOTALL)
                    if json_match:
                        ai_results = json.loads(json_match.group())
                        id_to_role = {s['id']: s['role'] for s in batch}
                        with lock:
                            for r in ai_results:
                                name = r.get('name')
                                if name and name != 'null' and name != 'None' and name.strip():
                                    sid = r.get('id', '')
                                    result_map[sid] = {'name': name.strip(), 'role': id_to_role.get(sid, '')}
                            pbar.update(len(batch))
                        return True
                    else:
                        with lock:
                            pbar.update(len(batch))
                        return True  # 没有JSON但不算错误
            except Exception as e:
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
                else:
                    with lock:
                        errors[0] += 1
                        pbar.update(len(batch))
                    print(f'  [ERROR] 批次失败({len(batch)}条): {type(e).__name__}: {e}')
                    return False
        return False

    if len(mega_batches) == 1:
        process_mega_batch(mega_batches[0])
    else:
        with ThreadPoolExecutor(max_workers=min(concurrency, len(mega_batches))) as executor:
            futures = [executor.submit(process_mega_batch, b) for b in mega_batches]
            for f in as_completed(futures):
                pass

    pbar.close()
    if errors[0]:
        print(f'  [WARN] {errors[0]} 个请求失败')
    print(f'  AI 提取到 {len(result_map)} 个有效人名')
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
            existing_df = pd.read_csv(str(result_csv), encoding='utf-8-sig')
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
                        old_names = set(old_val.split(';')) if old_val else set()
                        if p.name not in old_names:
                            old_names.discard('')
                            old_names.add(p.name)
                            existing_df.at[df_idx, p.role] = ';'.join(old_names)
                existing_df.at[df_idx, '来源'] = 'AI'
                merged_count += 1
            else:
                case_no_mask = queue_df[queue_df['snippet_id'].apply(
                    lambda x: str(x).startswith(f'{ridx}_'))]
                case_no = str(case_no_mask.iloc[0]['案号']) if len(case_no_mask) > 0 else ''
                result = persons_to_wide_row(ai_persons, file_label, row_num, case_no, source='AI')
                existing_df = pd.concat([existing_df, pd.DataFrame([result])], ignore_index=True)
                new_count += 1

        existing_df.to_csv(str(result_csv), index=False, encoding='utf-8-sig')
    else:
        # 没有 result 文件，创建新的
        rows = []
        for ridx, ai_persons in ai_by_row.items():
            case_no_mask = queue_df[queue_df['snippet_id'].apply(
                lambda x: str(x).startswith(f'{ridx}_'))]
            case_no = str(case_no_mask.iloc[0]['案号']) if len(case_no_mask) > 0 else ''
            rows.append(persons_to_wide_row(ai_persons, file_label, ridx + 1, case_no, source='AI'))
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

    db_path    = output_dir / 'progress.sqlite'
    output_csv = output_dir / f'{file_name}_result.csv'

    config     = Config()
    chunk_size = config.get('processing.chunk_size', 5000)
    db = ProgressDB(str(db_path))

    # 续跑检测
    existing = db.get_stats()
    if existing['total'] > 0:
        print(f"[续跑] 已有进度 -> done:{existing['done']}  "
              f"pending_ai:{existing['pending_ai']}  "
              f"no_signature:{existing['no_signature']}")

    writer = IncrementalCSVWriter(str(output_csv), ROLE_COLUMNS)

    exception_csv = output_dir / f'{file_name}_exceptions.csv'
    exc_writer = IncrementalCSVWriter(str(exception_csv), ['文件', '序号', '案号', '标识'])

    # AI队列 — 带状态列
    ai_queue_csv = output_dir / f'{file_name}_ai_queue.csv'
    AI_QUEUE_COLS = ['文件', '序号', '案号', '角色', '片段', '位置', 'snippet_id', '状态']
    ai_writer = IncrementalCSVWriter(str(ai_queue_csv), AI_QUEUE_COLS)

    stats = dict(read=0, skipped=0, rule_ok=0, uncertain=0, no_role=0, empty_text=0)
    batch_buf = []
    BATCH_COMMIT = 2000

    print(f"\n{'='*60}")
    print(f"  Phase 1: 规则提取（全文扫描 + 边界判断）")
    print(f"{'='*60}")
    print(f"  输入     : {input_path}")
    print(f"  提取结果 : {output_csv}")
    print(f"  AI队列   : {ai_queue_csv}")
    print(f"  异常     : {exception_csv}")
    print(f"  进度库   : {db_path}\n")

    try:
        reader = pd.read_csv(
            input_path,
            chunksize=chunk_size,
            encoding='utf-8',
            on_bad_lines='skip',
            usecols=lambda c: c in ['全文', '案号', '裁判日期', '法院'],
        )
        pbar = tqdm(desc='Phase 1 扫描', unit='条', mininterval=2.0)

        for chunk in reader:
            for row in chunk.itertuples(index=True):
                row_idx = row.Index
                pbar.update(1)
                stats['read'] += 1

                if args.limit and stats['read'] > args.limit:
                    raise StopIteration

                if db.is_processed(file_name, row_idx):
                    stats['skipped'] += 1
                    continue

                full_text_raw = getattr(row, '全文', None)
                case_no = str(getattr(row, '案号', '') or '')

                # 空文本
                if full_text_raw is None or (isinstance(full_text_raw, float) and str(full_text_raw) == 'nan'):
                    stats['empty_text'] += 1
                    db.mark_no_signature(file_name, row_idx)
                    exc_writer.append({'文件': file_label, '序号': row_idx + 1, '案号': case_no, '标识': '空文本'})
                    batch_buf.append(1)
                    if len(batch_buf) >= BATCH_COMMIT:
                        db.commit()
                        batch_buf.clear()
                    continue

                full_text = str(full_text_raw).strip()
                if not full_text or full_text == 'nan':
                    stats['empty_text'] += 1
                    db.mark_no_signature(file_name, row_idx)
                    exc_writer.append({'文件': file_label, '序号': row_idx + 1, '案号': case_no, '标识': '空文本'})
                    batch_buf.append(1)
                    if len(batch_buf) >= BATCH_COMMIT:
                        db.commit()
                        batch_buf.clear()
                    continue

                # 全文扫描提取
                certain, uncertain = RuleExtractor.extract_fulltext(full_text)

                if certain:
                    stats['rule_ok'] += 1
                    result = persons_to_wide_row(certain, file_label, row_idx + 1, case_no, source='规则')
                    writer.append(result)
                    db.mark_done(file_name, row_idx, 'rule', 0.85, result)
                elif not uncertain:
                    stats['no_role'] += 1
                    db.mark_no_signature(file_name, row_idx)
                    exc_writer.append({'文件': file_label, '序号': row_idx + 1, '案号': case_no, '标识': '无角色词'})
                else:
                    stats['no_role'] += 1

                # 收集不确定片段
                if uncertain:
                    stats['uncertain'] += len(uncertain)
                    for u in uncertain:
                        snippet_id = f"{row_idx}_{u.position}"
                        ai_writer.append({
                            '文件': file_label, '序号': row_idx + 1,
                            '案号': case_no, '角色': u.role,
                            '片段': u.snippet, '位置': u.position,
                            'snippet_id': snippet_id, '状态': '待处理'
                        })

                batch_buf.append(1)
                if len(batch_buf) >= BATCH_COMMIT:
                    db.commit()
                    batch_buf.clear()

        db.commit()
        pbar.close()

    except StopIteration:
        db.commit()
        pbar.close()
        print(f"\n[已达 limit={args.limit}，停止]")
    except KeyboardInterrupt:
        db.commit()
        pbar.close()
        print('\n[中断] 进度已保存，重新运行可从断点继续')

    # 统计
    processed = stats['read'] - stats['skipped']
    print('\n' + '-' * 60)
    print('Phase 1 完成')
    print('-' * 60)
    print(f'  读取总数   : {stats["read"]:>10,}')
    print(f'  跳过(续跑) : {stats["skipped"]:>10,}')
    print(f'  本次处理   : {processed:>10,}')
    if processed:
        print(f'  确定提取   : {stats["rule_ok"]:>10,}  ({stats["rule_ok"]/processed*100:.1f}%)')
        print(f'  不确定片段 : {stats["uncertain"]:>10,}')
        print(f'  空文本     : {stats["empty_text"]:>10,}  ({stats["empty_text"]/processed*100:.1f}%)')
        print(f'  无角色词   : {stats["no_role"]:>10,}  ({stats["no_role"]/processed*100:.1f}%)')
    print(f'  结果文件   : {output_csv}')
    print(f'  AI队列     : {ai_queue_csv}  ({stats["uncertain"]} 条待处理)')
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

    # 读取 AI 队列
    queue_df = pd.read_csv(str(queue_csv), encoding='utf-8-sig')
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

    print(f"\n{'='*60}")
    print(f"  Phase 2: AI 精修")
    print(f"{'='*60}")
    print(f"  队列文件  : {queue_csv}")
    print(f"  结果文件  : {result_csv}")
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
    ai_map = _ai_batch_extract(snippets, config)

    # 更新队列状态
    processed_ids = set(ai_map.keys())
    all_pending_ids = set(pending['snippet_id'].astype(str))

    for idx, row in queue_df.iterrows():
        sid = str(row['snippet_id'])
        if sid in processed_ids:
            queue_df.at[idx, '状态'] = '已处理'
        elif sid in all_pending_ids and sid not in processed_ids:
            # 发给AI了但没返回结果 — 可能AI认为不是人名(null)或批次失败
            # 如果该 snippet 的批次没失败，说明AI返回了null → 标记为已处理
            # 如果批次失败了 → 标记为失败
            queue_df.at[idx, '状态'] = '已处理'  # 默认已处理(AI认为无人名)

    # 保存更新后的队列
    queue_df.to_csv(str(queue_csv), index=False, encoding='utf-8-sig')

    # 合并到 result.csv
    if ai_map:
        merged, new, _ = _merge_ai_results(result_csv, queue_df, ai_map, file_label)
        print(f'\n  合并完成: 更新 {merged} 行，新增 {new} 行')

    # 统计
    updated_queue = pd.read_csv(str(queue_csv), encoding='utf-8-sig')
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

    queue_df = pd.read_csv(str(queue_csv), encoding='utf-8-sig')
    failed = queue_df[queue_df['状态'] == '失败']

    if len(failed) == 0:
        print('[完成] 没有失败的片段需要重试')
        pending = queue_df[queue_df['状态'] == '待处理']
        if len(pending) > 0:
            print(f'  但有 {len(pending)} 条待处理片段，请运行 phase2')
        return

    file_label = str(failed.iloc[0]['文件'])

    print(f"\n{'='*60}")
    print(f"  Retry: 重试失败片段")
    print(f"{'='*60}")
    print(f"  队列文件 : {queue_csv}")
    print(f"  结果文件 : {result_csv}")
    print(f"  失败片段 : {len(failed)} 条\n")

    snippets = []
    for _, row in failed.iterrows():
        snippets.append({
            'id': str(row['snippet_id']),
            'role': str(row['角色']),
            'text': str(row['片段']),
        })

    config = Config()
    ai_map = _ai_batch_extract(snippets, config)

    # 更新状态
    processed_ids = set(ai_map.keys())
    for idx, row in queue_df.iterrows():
        sid = str(row['snippet_id'])
        if row['状态'] == '失败':
            if sid in processed_ids:
                queue_df.at[idx, '状态'] = '已处理'
            else:
                queue_df.at[idx, '状态'] = '已处理'  # AI返回null也算处理了

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

    # Phase 1 进度
    db_path = output_dir / 'progress.sqlite'
    if db_path.exists():
        db = ProgressDB(str(db_path))
        stats = db.get_stats()
        print(f'\n  Phase 1 (规则提取):')
        print(f'    已完成      : {stats["done"]:>8,}')
        print(f'    待AI处理    : {stats["pending_ai"]:>8,}')
        print(f'    无角色词    : {stats["no_signature"]:>8,}')
        print(f'    合计        : {stats["total"]:>8,}')

    # Result CSV
    results = list(output_dir.glob('*_result.csv'))
    for r in results:
        df = pd.read_csv(str(r), encoding='utf-8-sig')
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
        qdf = pd.read_csv(str(q), encoding='utf-8-sig')
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
