"""
阶段2：AI增强 - 处理低置信度文书
使用GLM-4-Flash进行智能提取
"""
import argparse
import time
from pathlib import Path
from tqdm import tqdm
from core import Config, AIExtractor, load_csv, persons_to_dict_row, save_csv, print_statistics


def main():
    parser = argparse.ArgumentParser(description='阶段2：AI增强提取')
    parser.add_argument('--input', type=str, default='data/temp/stage1_low_confidence.csv', help='输入CSV文件路径')
    parser.add_argument('--output-dir', type=str, default='data/temp', help='输出目录')
    parser.add_argument('--delay', type=float, default=0.5, help='API调用间隔（秒）')
    args = parser.parse_args()
    
    config = Config()
    
    if not config.api_config['api_key']:
        print("错误：未配置API密钥")
        print("请在 config/.env 文件中设置 API_KEY")
        return
    
    print(f"读取文件: {args.input}")
    df = load_csv(args.input)
    
    if '落款区域' not in df.columns:
        raise ValueError("输入文件缺少'落款区域'列")
    
    ai_extractor = AIExtractor(config)
    
    ai_extracted_data = []
    failed_data = []
    
    stats = {
        'total': len(df),
        'ai_success': 0,
        'ai_failed': 0
    }
    
    print(f"开始AI增强处理 {len(df)} 条文书...")
    print(f"使用模型: {config.api_config['model']}")
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="AI处理中"):
        signature_area = str(row['落款区域'])
        
        try:
            persons = ai_extractor.extract(signature_area)
            
            if persons:
                stats['ai_success'] += 1
                result_row = persons_to_dict_row(
                    persons=persons,
                    file_name=str(row.get('文件', '')),
                    index=int(row.get('序号', idx + 1)),
                    method='ai',
                    confidence=0.95
                )
                result_row['全文'] = row.get('全文', '')
                result_row['落款区域'] = signature_area
                ai_extracted_data.append(result_row)
            else:
                stats['ai_failed'] += 1
                failed_data.append(row.to_dict())
            
            time.sleep(args.delay)
            
        except Exception as e:
            print(f"\n处理第 {idx+1} 条时出错: {e}")
            stats['ai_failed'] += 1
            failed_data.append(row.to_dict())
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if ai_extracted_data:
        ai_path = output_dir / "stage2_ai_extracted.csv"
        save_csv(ai_extracted_data, str(ai_path))
        print(f"\n✓ AI提取结果: {ai_path}")
    
    if failed_data:
        failed_path = output_dir / "stage2_failed.csv"
        save_csv(failed_data, str(failed_path))
        print(f"✓ 失败记录: {failed_path}")
    
    print_statistics(stats)
    
    success_rate = stats['ai_success'] / stats['total'] * 100 if stats['total'] > 0 else 0
    print(f"\nAI提取成功率: {success_rate:.1f}%")
    
    estimated_cost = stats['total'] * 500 * 0.18 / 1000000
    print(f"预估成本: {estimated_cost:.2f} 元")


if __name__ == "__main__":
    main()
