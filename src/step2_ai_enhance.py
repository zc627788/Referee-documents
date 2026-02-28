"""
阶段2：AI处理 - 处理所有有落款的文书（高置信度+低置信度）
根据置信度使用不同文本长度：高置信度500字，低置信度1000字
使用GLM-4-Flash进行智能提取
"""
import argparse
import time
from pathlib import Path
from tqdm import tqdm
from core import Config, AIExtractor, load_csv, persons_to_dict_row, save_csv, print_statistics
import pandas as pd


def main():
    parser = argparse.ArgumentParser(description='阶段2：AI处理所有有落款的文书')
    parser.add_argument('--high-conf-input', type=str, default='data/temp/stage1_high_confidence.csv', help='高置信度CSV')
    parser.add_argument('--low-conf-input', type=str, default='data/temp/stage1_low_confidence.csv', help='低置信度CSV')
    parser.add_argument('--output-dir', type=str, default='data/temp', help='输出目录')
    parser.add_argument('--delay', type=float, default=0.5, help='API调用间隔（秒）')
    args = parser.parse_args()
    
    config = Config()
    
    if not config.api_config['api_key']:
        print("错误：未配置API密钥")
        print("请在 config/.env 文件中设置 API_KEY")
        return
    
    # 读取高置信度和低置信度文件
    all_data = []
    
    high_conf_path = Path(args.high_conf_input)
    low_conf_path = Path(args.low_conf_input)
    
    if high_conf_path.exists():
        df_high = load_csv(str(high_conf_path))
        df_high['原始置信度'] = 'high'
        all_data.append(df_high)
        print(f"读取高置信度: {len(df_high)} 条")
    
    if low_conf_path.exists():
        df_low = load_csv(str(low_conf_path))
        df_low['原始置信度'] = 'low'
        all_data.append(df_low)
        print(f"读取低置信度: {len(df_low)} 条")
    
    if not all_data:
        print("错误：没有找到输入文件")
        return
    
    df = pd.concat(all_data, ignore_index=True)
    
    if '落款区域' not in df.columns:
        raise ValueError("输入文件缺少'落款区域'列")
    
    # 获取文本长度配置
    text_length_config = config.get('extraction.ai_text_length', {})
    high_length = text_length_config.get('high_confidence', 500)
    low_length = text_length_config.get('low_confidence', 1000)
    
    ai_extractor = AIExtractor(config)
    
    ai_extracted_data = []
    failed_data = []
    
    stats = {
        'total': len(df),
        'high_conf_processed': 0,
        'low_conf_processed': 0,
        'ai_success': 0,
        'ai_failed': 0
    }
    
    print(f"\n开始AI处理 {len(df)} 条有落款的文书...")
    print(f"使用模型: {config.api_config['model']}")
    print(f"高置信度文本长度: {high_length}字")
    print(f"低置信度文本长度: {low_length}字")
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="AI处理中"):
        original_conf = row.get('原始置信度', 'low')
        signature_area = str(row['落款区域'])
        
        # 根据原始置信度截取不同长度
        if original_conf == 'high':
            stats['high_conf_processed'] += 1
            if high_length > 0:
                signature_area = signature_area[-high_length:]
        else:
            stats['low_conf_processed'] += 1
            if low_length > 0:
                signature_area = signature_area[-low_length:]
        
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
    print(f"高置信度处理: {stats['high_conf_processed']} 条")
    print(f"低置信度处理: {stats['low_conf_processed']} 条")
    
    # 估算成本
    avg_chars = (stats['high_conf_processed'] * high_length + stats['low_conf_processed'] * low_length) / stats['total'] if stats['total'] > 0 else 0
    estimated_cost = stats['total'] * avg_chars * 0.18 / 1000000
    print(f"预估成本: {estimated_cost:.4f} 元")


if __name__ == "__main__":
    main()
