#!/usr/bin/env python3
"""
裁判文书落款提取 — 统一入口

用法：
  # 处理单个 CSV（支持中断续跑）
  python src/run_pipeline.py process --input <CSV路径> [--output-dir data/output] [--limit N] [--no-ai]

  # 查看处理进度
  python src/run_pipeline.py status [--db data/output/progress.sqlite]

  # 审核并追加新发现的角色
  python src/run_pipeline.py review-roles [--db data/output/progress.sqlite]
"""
import argparse
import sys
import json
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# 把 src 目录加入路径
sys.path.insert(0, str(Path(__file__).parent))

from core import (
    Config, SignatureLocator, RuleExtractor,
    ProgressDB, persons_to_wide_row, IncrementalCSVWriter, ROLE_COLUMNS,
)


# ─────────────────────────────────────────────────────────────
# 子命令：process
# ─────────────────────────────────────────────────────────────
def cmd_process(args):
    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[ERROR] 文件不存在: {input_path}")
        sys.exit(1)

    file_name = input_path.stem
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    db_path    = output_dir / 'progress.sqlite'
    output_csv = output_dir / f'{file_name}_result.csv'

    config     = Config()
    tail_len   = config.get('processing.tail_length', 1500)
    chunk_size = config.get('processing.chunk_size', 5000)
    conf_thr   = config.get('processing.confidence_threshold', 0.5)

    db = ProgressDB(str(db_path))

    # 显示已有进度
    existing = db.get_stats()
    if existing['total'] > 0:
        print(f"[续跑] 已有进度 → done:{existing['done']}  "
              f"pending_ai:{existing['pending_ai']}  "
              f"no_signature:{existing['no_signature']}")

    writer = IncrementalCSVWriter(str(output_csv), ROLE_COLUMNS)
    
    exception_csv = output_dir / f'{file_name}_exceptions.csv'
    exc_writer = IncrementalCSVWriter(str(exception_csv), ['案号', '标识', '原文'])

    stats = dict(read=0, skipped=0, rule_ok=0, bad_sig=0, no_sig=0, empty_text=0)
    batch_buf = []
    BATCH_COMMIT = 2000  # 每2000条提交一次事务

    print(f"\n[开始] {file_name}")
    print(f"  输入: {input_path}")
    print(f"  提取结果: {output_csv}")
    print(f"  异常文书: {exception_csv}")
    print(f"  进度库: {db_path}\n")

    try:
        reader = pd.read_csv(
            input_path,
            chunksize=chunk_size,
            encoding='utf-8',
            on_bad_lines='skip',
            usecols=lambda c: c in ['全文', '案号', '裁判日期', '法院'],
        )
        pbar = tqdm(desc='处理中', unit='条', mininterval=2.0)

        for chunk in reader:
            for row in chunk.itertuples(index=True):
                row_idx = row.Index          # pandas 分配的全局行号（0-based）
                pbar.update(1)
                stats['read'] += 1

                if args.limit and stats['read'] > args.limit:
                    raise StopIteration

                # 断点续传：已处理过就跳过
                if db.is_processed(file_name, row_idx):
                    stats['skipped'] += 1
                    continue

                full_text_raw = getattr(row, '全文', None)
                case_no = str(getattr(row, '案号', '') or '')
                
                # 全文为空/NaN → 记入 exceptions
                if full_text_raw is None or (isinstance(full_text_raw, float) and str(full_text_raw) == 'nan'):
                    stats['empty_text'] += 1
                    db.mark_no_signature(file_name, row_idx)
                    exc_writer.append({'案号': case_no, '标识': '空文本', '原文': 'NaN'})
                    batch_buf.append(1)
                    if len(batch_buf) >= BATCH_COMMIT:
                        db.commit()
                        batch_buf.clear()
                    continue
                
                full_text = str(full_text_raw).strip()
                if not full_text or full_text == 'nan':
                    stats['empty_text'] += 1
                    db.mark_no_signature(file_name, row_idx)
                    exc_writer.append({'案号': case_no, '标识': '空文本', '原文': full_text[:500] if full_text else '空字符串'})
                    batch_buf.append(1)
                    if len(batch_buf) >= BATCH_COMMIT:
                        db.commit()
                        batch_buf.clear()
                    continue

                # ── 阶段1：落款定位 ──────────────────────────
                sig_area = SignatureLocator.locate(full_text, tail_len)

                if not sig_area:
                    stats['no_sig'] += 1
                    case_no = str(getattr(row, '案号', '') or '')
                    db.mark_no_signature(file_name, row_idx)
                    exc_writer.append({'案号': case_no, '标识': '无落款', '原文': full_text[:500]})
                else:
                    # ── 阶段2：规则提取 ──────────────────────
                    ok, conf, persons = RuleExtractor.extract(sig_area)

                    if ok:
                        stats['rule_ok'] += 1
                        case_no = str(getattr(row, '案号', '') or '')
                        result = persons_to_wide_row(persons, file_name, row_idx + 1, case_no)
                        writer.append(result)
                        db.mark_done(file_name, row_idx, 'rule', conf, result)
                    else:
                        stats['bad_sig'] += 1
                        case_no = str(getattr(row, '案号', '') or '')
                        db.mark_pending_ai(file_name, row_idx, json.dumps({'sig': sig_area, 'case_no': case_no}, ensure_ascii=False))
                        exc_writer.append({'案号': case_no, '标识': '落款有误', '原文': full_text})

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

    _print_phase1_summary(stats, output_csv)

    final = db.get_stats()
    print(f'\n[完成] 提取成功={final["done"]}  '
          f'落款有误={final["pending_ai"]}  '
          f'无落款={final["no_signature"]}')


def _print_phase1_summary(stats: dict, output_csv: Path):
    processed = stats['read'] - stats['skipped']
    print('\n' + '─' * 60)
    print('Phase 1 统计（规则提取）')
    print('─' * 60)
    print(f'  读取总数 : {stats["read"]:>10,}')
    print(f'  跳过(续跑): {stats["skipped"]:>10,}')
    print(f'  本次处理 : {processed:>10,}')
    if processed:
        print(f'  规则成功 : {stats["rule_ok"]:>10,}  ({stats["rule_ok"]/processed*100:.1f}%)')
        print(f'  空文本   : {stats["empty_text"]:>10,}  ({stats["empty_text"]/processed*100:.1f}%)')
        print(f'  无落款   : {stats["no_sig"]:>10,}  ({stats["no_sig"]/processed*100:.1f}%)')
        print(f'  落款有误 : {stats["bad_sig"]:>10,}  ({stats["bad_sig"]/processed*100:.1f}%)')
    print(f'  输出文件 : {output_csv}')
    print('─' * 60)




# ─────────────────────────────────────────────────────────────
# 子命令：status
# ─────────────────────────────────────────────────────────────
def cmd_status(args):
    db_path = Path(args.db)
    if not db_path.exists():
        print(f'[ERROR] 进度库不存在: {db_path}')
        return
    db = ProgressDB(str(db_path))
    stats = db.get_stats()
    print('\n处理进度：')
    print(f'  已完成 (done)       : {stats["done"]:,}')
    print(f'  待AI处理 (pending)  : {stats["pending_ai"]:,}')
    print(f'  无落款              : {stats["no_signature"]:,}')
    print(f'  合计                : {stats["total"]:,}')


# ─────────────────────────────────────────────────────────────
# 子命令：review-roles
# ─────────────────────────────────────────────────────────────
def cmd_review_roles(args):
    db_path = Path(args.db)
    if not db_path.exists():
        print(f'[ERROR] 进度库不存在: {db_path}')
        return
    db = ProgressDB(str(db_path))
    roles_json = str(Path(__file__).parent.parent / 'config' / 'roles.json')

    candidates = db.get_discovered_roles()
    if not candidates:
        print('暂无新发现的角色。')
        return

    print(f'\n发现 {len(candidates)} 个疑似新角色：\n')
    for i, r in enumerate(candidates, 1):
        print(f'  {i:>3}. [{r["role_text"]}]  出现 {r["count"]} 次')
        print(f'       样本: {r["sample"]}\n')

    print('请输入要确认加入规则字典的序号（逗号分隔，all=全部，回车跳过）：', end='')
    choice = input().strip()
    if not choice:
        print('跳过，未做更改。')
        return

    indices = range(1, len(candidates) + 1) if choice == 'all' \
        else [int(x.strip()) for x in choice.split(',') if x.strip().isdigit()]

    added = []
    for idx in indices:
        if 1 <= idx <= len(candidates):
            role = candidates[idx - 1]['role_text']
            db.confirm_role(role, roles_json)
            added.append(role)

    if added:
        print(f'\n✅ 已追加角色: {", ".join(added)}')
        print(f'   roles.json 已更新，下次运行自动生效。')


# ─────────────────────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='裁判文书落款提取系统',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest='command')

    # process
    p = sub.add_parser('process', help='处理 CSV 文件（支持断点续传）')
    p.add_argument('--input',      required=True,            help='输入 CSV 文件路径')
    p.add_argument('--output-dir', default='data/output',    help='输出目录')
    p.add_argument('--limit',      type=int, default=None,   help='处理行数上限（测试用）')
    p.add_argument('--no-ai',      action='store_true',      help='跳过 AI 兜底阶段')

    # status
    p2 = sub.add_parser('status', help='查看处理进度')
    p2.add_argument('--db', default='data/output/progress.sqlite')

    # review-roles
    p3 = sub.add_parser('review-roles', help='审核并追加新发现的角色')
    p3.add_argument('--db', default='data/output/progress.sqlite')

    args = parser.parse_args()

    if args.command == 'process':
        cmd_process(args)
    elif args.command == 'status':
        cmd_status(args)
    elif args.command == 'review-roles':
        cmd_review_roles(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
