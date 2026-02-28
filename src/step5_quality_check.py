"""
阶段5：质量检查
随机抽样验证提取结果的准确性
"""
import argparse
import random
import pandas as pd
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='阶段5：质量检查')
    parser.add_argument('--input', type=str, default='data/output/final_result.xlsx', help='输入Excel文件')
    parser.add_argument('--sample', type=int, default=50, help='抽样数量')
    parser.add_argument('--seed', type=int, default=42, help='随机种子')
    args = parser.parse_args()
    
    print(f"读取文件: {args.input}")
    df = pd.read_excel(args.input)
    
    random.seed(args.seed)
    sample_size = min(args.sample, len(df))
    sample_indices = random.sample(range(len(df)), sample_size)
    
    print(f"\n随机抽取 {sample_size} 条进行质量检查：")
    print("="*80)
    
    for i, idx in enumerate(sample_indices, 1):
        row = df.iloc[idx]
        
        print(f"\n【样本 {i}/{sample_size}】")
        print(f"文件: {row.get('文件', 'N/A')}")
        print(f"序号: {row.get('序号', 'N/A')}")
        print(f"提取方法: {row.get('提取方法', 'N/A')}")
        print(f"置信度: {row.get('置信度', 'N/A')}")
        
        print("\n提取的人员信息:")
        persons = []
        for j in range(1, 20):
            name_col = f'姓名{j}'
            role_col = f'角色{j}'
            if name_col not in row or pd.isna(row[name_col]):
                break
            persons.append(f"  {row[role_col]}: {row[name_col]}")
        
        if persons:
            print('\n'.join(persons))
        else:
            print("  (无)")
        
        print("-"*80)
    
    print(f"\n请在原始CSV文件中核对上述序号的文书，确认提取准确性。")
    print("\n质量检查指南：")
    print("1. 对比原文落款，确认姓名和角色是否正确")
    print("2. 检查是否有遗漏的人员")
    print("3. 检查是否有错误提取的人员")
    print("4. 记录准确数量，计算准确率")
    
    output_dir = Path(args.input).parent
    sample_file = output_dir / f"quality_check_sample_{sample_size}.txt"
    
    with open(sample_file, 'w', encoding='utf-8') as f:
        f.write(f"质量检查抽样 - 共 {sample_size} 条\n")
        f.write("="*80 + "\n")
        for i, idx in enumerate(sample_indices, 1):
            row = df.iloc[idx]
            f.write(f"\n样本 {i}: 序号 {row.get('序号', 'N/A')}\n")
            f.write(f"提取方法: {row.get('提取方法', 'N/A')}\n")
            f.write("人员: ")
            persons = []
            for j in range(1, 20):
                name_col = f'姓名{j}'
                role_col = f'角色{j}'
                if name_col not in row or pd.isna(row[name_col]):
                    break
                persons.append(f"{row[role_col]}:{row[name_col]}")
            f.write(', '.join(persons) if persons else '(无)')
            f.write('\n' + '-'*80 + '\n')
    
    print(f"\n✓ 抽样清单已保存: {sample_file}")


if __name__ == "__main__":
    main()
