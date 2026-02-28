"""
阶段1：定位落款区域并进行规则提取
输出3个文件：高置信度、低置信度、无落款
"""
import argparse
import pandas as pd
from pathlib import Path
from tqdm import tqdm
from core import Config, SignatureLocator, RuleExtractor, persons_to_dict_row, save_csv, print_statistics


def main():
    parser = argparse.ArgumentParser(description='阶段1：定位与规则提取')
    parser.add_argument('--input', type=str, required=True, help='输入CSV文件路径')
    parser.add_argument('--output-dir', type=str, default='data/temp', help='输出目录')
    args = parser.parse_args()
    
    config = Config()
    conf_high = config.get('extraction.confidence_threshold_high', 0.8)
    conf_low = config.get('extraction.confidence_threshold_low', 0.5)
    
    print(f"读取文件: {args.input}")
    df = pd.read_csv(args.input)
    
    if '全文' not in df.columns:
        raise ValueError("CSV文件中未找到'全文'列")
    
    file_name = Path(args.input).stem
    
    high_confidence_data = []
    low_confidence_data = []
    no_signature_data = []
    
    stats = {
        'total': 0,
        'has_signature': 0,
        'no_signature': 0,
        'high_confidence': 0,
        'low_confidence': 0
    }
    
    print(f"开始处理 {len(df)} 条文书...")
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="阶段1处理中"):
        stats['total'] += 1
        full_text = str(row['全文'])
        
        signature_area = SignatureLocator.locate(full_text)
        
        if not signature_area:
            stats['no_signature'] += 1
            row_dict = row.to_dict()
            row_dict['序号'] = idx + 1
            no_signature_data.append(row_dict)
            continue
        
        stats['has_signature'] += 1
        success, confidence, persons = RuleExtractor.extract(signature_area)
        
        result_row = persons_to_dict_row(
            persons=persons,
            file_name=file_name,
            index=idx + 1,
            method='rule',
            confidence=confidence
        )
        
        result_row['全文'] = full_text
        result_row['落款区域'] = signature_area
        
        if confidence >= conf_high:
            stats['high_confidence'] += 1
            high_confidence_data.append(result_row)
        else:
            stats['low_confidence'] += 1
            low_confidence_data.append(result_row)
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    high_conf_path = output_dir / "stage1_high_confidence.csv"
    low_conf_path = output_dir / "stage1_low_confidence.csv"
    no_sig_path = output_dir / "stage1_no_signature.csv"
    
    if high_confidence_data:
        save_csv(high_confidence_data, str(high_conf_path))
        print(f"✓ 高置信度结果: {high_conf_path}")
    
    if low_confidence_data:
        save_csv(low_confidence_data, str(low_conf_path))
        print(f"✓ 低置信度结果: {low_conf_path}")
    
    if no_signature_data:
        save_csv(no_signature_data, str(no_sig_path))
        print(f"✓ 无落款结果: {no_sig_path}")
    
    print_statistics(stats)
    
    print(f"\n百分比统计:")
    print(f"  有落款: {stats['has_signature']/stats['total']*100:.1f}%")
    print(f"  无落款: {stats['no_signature']/stats['total']*100:.1f}%")
    print(f"  高置信度: {stats['high_confidence']/stats['total']*100:.1f}%")
    print(f"  低置信度: {stats['low_confidence']/stats['total']*100:.1f}%")


if __name__ == "__main__":
    main()
