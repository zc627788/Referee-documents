"""
阶段3：处理无落款文书
智能搜索策略：在全文中搜索关键词，提取上下文后用AI验证
"""
import argparse
import time
from pathlib import Path
from tqdm import tqdm
from core import Config, SignatureLocator, AIExtractor, load_csv, persons_to_dict_row, save_csv, print_statistics


def main():
    parser = argparse.ArgumentParser(description='阶段3：无落款智能处理')
    parser.add_argument('--input', type=str, default='data/temp/stage1_no_signature.csv', help='输入CSV文件路径')
    parser.add_argument('--output-dir', type=str, default='data/temp', help='输出目录')
    parser.add_argument('--delay', type=float, default=0.5, help='API调用间隔（秒）')
    args = parser.parse_args()
    
    config = Config()
    max_regions = config.get('extraction.max_candidate_regions', 5)
    window_size = config.get('extraction.context_window_size', 200)
    
    if not config.api_config['api_key']:
        print("警告：未配置API密钥，将只执行关键词搜索")
        use_ai = False
    else:
        use_ai = True
        ai_extractor = AIExtractor(config)
    
    print(f"读取文件: {args.input}")
    df = load_csv(args.input)
    
    if '全文' not in df.columns:
        raise ValueError("输入文件缺少'全文'列")
    
    extracted_data = []
    truly_no_signature_data = []
    complex_data = []
    
    stats = {
        'total': len(df),
        'found_keywords': 0,
        'no_keywords': 0,
        'too_complex': 0,
        'extracted': 0
    }
    
    print(f"开始处理 {len(df)} 条无落款文书...")
    
    for idx, row in tqdm(df.iterrows(), total=len(df), desc="智能搜索中"):
        full_text = str(row['全文'])
        
        positions = SignatureLocator.find_keyword_positions(full_text)
        
        if not positions:
            stats['no_keywords'] += 1
            row_dict = row.to_dict()
            row_dict['原因'] = '全文未找到关键词'
            truly_no_signature_data.append(row_dict)
            continue
        
        stats['found_keywords'] += 1
        total_positions = sum(len(pos_list) for pos_list in positions.values())
        
        if total_positions > max_regions:
            stats['too_complex'] += 1
            row_dict = row.to_dict()
            row_dict['原因'] = f'候选区域过多({total_positions}个)'
            row_dict['关键词位置'] = str(positions)
            complex_data.append(row_dict)
            continue
        
        candidate_text = SignatureLocator.extract_context(full_text, positions, window_size)
        
        if len(candidate_text) > 1000:
            stats['too_complex'] += 1
            row_dict = row.to_dict()
            row_dict['原因'] = f'候选文本过长({len(candidate_text)}字)'
            complex_data.append(row_dict)
            continue
        
        if use_ai:
            try:
                persons = ai_extractor.extract(candidate_text)
                
                if persons:
                    stats['extracted'] += 1
                    result_row = persons_to_dict_row(
                        persons=persons,
                        file_name=str(row.get('案号', '')),
                        index=int(row.get('序号', idx + 1)),
                        method='ai_smart_search',
                        confidence=0.85
                    )
                    result_row['全文'] = full_text
                    result_row['候选区域'] = candidate_text
                    extracted_data.append(result_row)
                else:
                    row_dict = row.to_dict()
                    row_dict['原因'] = 'AI未提取到人员'
                    row_dict['候选区域'] = candidate_text
                    truly_no_signature_data.append(row_dict)
                
                time.sleep(args.delay)
                
            except Exception as e:
                print(f"\n处理第 {idx+1} 条时出错: {e}")
                row_dict = row.to_dict()
                row_dict['原因'] = f'AI处理失败: {str(e)}'
                truly_no_signature_data.append(row_dict)
        else:
            row_dict = row.to_dict()
            row_dict['原因'] = '未启用AI验证'
            row_dict['候选区域'] = candidate_text
            truly_no_signature_data.append(row_dict)
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    if extracted_data:
        extracted_path = output_dir / "stage3_extracted.csv"
        save_csv(extracted_data, str(extracted_path))
        print(f"\n✓ 提取成功: {extracted_path}")
    
    if truly_no_signature_data:
        no_sig_path = output_dir / "stage3_truly_no_signature.csv"
        save_csv(truly_no_signature_data, str(no_sig_path))
        print(f"✓ 确实无落款: {no_sig_path}")
    
    if complex_data:
        complex_path = output_dir / "stage3_complex.csv"
        save_csv(complex_data, str(complex_path))
        print(f"✓ 复杂文书: {complex_path}")
    
    print_statistics(stats)
    
    if stats['found_keywords'] > 0:
        extract_rate = stats['extracted'] / stats['found_keywords'] * 100
        print(f"\n找到关键词的文书中，成功提取率: {extract_rate:.1f}%")
    
    if use_ai and stats['extracted'] > 0:
        estimated_cost = stats['extracted'] * 400 * 0.18 / 1000000
        print(f"预估成本: {estimated_cost:.2f} 元")


if __name__ == "__main__":
    main()
