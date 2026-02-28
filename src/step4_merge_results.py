"""
阶段4：合并所有结果
将阶段1、2、3的结果合并成最终Excel文件
"""
import argparse
import json
from pathlib import Path
import pandas as pd
from core import load_csv, save_excel, print_statistics


def main():
    parser = argparse.ArgumentParser(description='阶段4：合并结果')
    parser.add_argument('--input-dir', type=str, default='data/temp', help='输入目录')
    parser.add_argument('--output', type=str, default='data/output/final_result.xlsx', help='输出Excel路径')
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    
    all_data = []
    
    stage1_high = input_dir / "stage1_high_confidence.csv"
    stage2_ai = input_dir / "stage2_ai_extracted.csv"
    stage3_extracted = input_dir / "stage3_extracted.csv"
    
    stats = {
        'stage1_high': 0,
        'stage2_ai': 0,
        'stage3_smart': 0,
        'total': 0
    }
    
    print("合并结果...")
    
    if stage1_high.exists():
        df = load_csv(str(stage1_high))
        stats['stage1_high'] = len(df)
        all_data.append(df)
        print(f"✓ 阶段1高置信度: {len(df)} 条")
    
    if stage2_ai.exists():
        df = load_csv(str(stage2_ai))
        stats['stage2_ai'] = len(df)
        all_data.append(df)
        print(f"✓ 阶段2 AI增强: {len(df)} 条")
    
    if stage3_extracted.exists():
        df = load_csv(str(stage3_extracted))
        stats['stage3_smart'] = len(df)
        all_data.append(df)
        print(f"✓ 阶段3智能搜索: {len(df)} 条")
    
    if not all_data:
        print("错误：没有找到任何结果文件")
        return
    
    merged_df = pd.concat(all_data, ignore_index=True)
    
    merged_df = merged_df.sort_values('序号')
    
    output_columns = ['文件', '序号', '提取方法', '置信度']
    max_persons = 7
    for col in merged_df.columns:
        if col.startswith('姓名'):
            person_num = int(col.replace('姓名', ''))
            max_persons = max(max_persons, person_num)
    
    for i in range(1, max_persons + 1):
        output_columns.extend([f'姓名{i}', f'角色{i}'])
    
    output_df = merged_df[output_columns].copy()
    
    stats['total'] = len(output_df)
    
    save_excel([row.to_dict() for _, row in output_df.iterrows()], args.output)
    print(f"\n✓ 最终结果已保存: {args.output}")
    
    print_statistics(stats)
    
    stats_file = Path(args.output).parent / "final_statistics.json"
    with open(stats_file, 'w', encoding='utf-8') as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"✓ 统计报告已保存: {stats_file}")
    
    print(f"\n最终提取成功率: {stats['total']} 条")
    print("\n各阶段贡献：")
    if stats['total'] > 0:
        print(f"  规则提取: {stats['stage1_high']/stats['total']*100:.1f}%")
        print(f"  AI增强: {stats['stage2_ai']/stats['total']*100:.1f}%")
        print(f"  智能搜索: {stats['stage3_smart']/stats['total']*100:.1f}%")


if __name__ == "__main__":
    main()
