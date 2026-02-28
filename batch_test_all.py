"""批量测试所有月份数据 - 直接调用"""
import sys
sys.path.insert(0, 'src')

import pandas as pd
from pathlib import Path
from tqdm import tqdm
from core import Config, SignatureLocator, RuleExtractor, persons_to_dict_row, save_csv

def test_single_file(csv_path):
    """测试单个文件"""
    df = pd.read_csv(csv_path)
    file_name = Path(csv_path).stem
    
    stats = {
        '文件': file_name,
        '总计': len(df),
        '有落款': 0,
        '无落款': 0,
        '高置信度': 0,
        '低置信度': 0
    }
    
    config = Config()
    conf_high = config.get('extraction.confidence_threshold_high', 0.8)
    
    for idx, row in df.iterrows():
        full_text = str(row['全文'])
        signature_area = SignatureLocator.locate(full_text)
        
        if not signature_area:
            stats['无落款'] += 1
            continue
        
        stats['有落款'] += 1
        success, confidence, persons = RuleExtractor.extract(signature_area)
        
        if confidence >= conf_high:
            stats['高置信度'] += 1
        else:
            stats['低置信度'] += 1
    
    return stats

# 批量测试
print("="*80)
print("批量测试所有月份数据")
print("="*80)

input_dir = Path('data/input')
all_stats = []

for csv_file in tqdm(sorted(input_dir.glob('*.csv')), desc="处理中"):
    try:
        stats = test_single_file(csv_file)
        all_stats.append(stats)
        print(f"\n{stats['文件']}: 总{stats['总计']}条, "
              f"高置信{stats['高置信度']}({stats['高置信度']/stats['总计']*100:.0f}%), "
              f"低置信{stats['低置信度']}({stats['低置信度']/stats['总计']*100:.0f}%), "
              f"无落款{stats['无落款']}({stats['无落款']/stats['总计']*100:.0f}%)")
    except Exception as e:
        print(f"\n{csv_file.name}: 处理失败 - {e}")

# 汇总统计
print("\n" + "="*80)
print("汇总统计")
print("="*80)

if all_stats:
    df_summary = pd.DataFrame(all_stats)
    
    # 计算总体
    total = df_summary['总计'].sum()
    high_conf = df_summary['高置信度'].sum()
    low_conf = df_summary['低置信度'].sum()
    no_sig = df_summary['无落款'].sum()
    
    print(f"\n总文书数: {total}")
    print(f"高置信度: {high_conf} ({high_conf/total*100:.1f}%)")
    print(f"低置信度: {low_conf} ({low_conf/total*100:.1f}%)")
    print(f"无落款: {no_sig} ({no_sig/total*100:.1f}%)")
    print(f"\n规则提取成功率: {high_conf/total*100:.1f}%")
    
    # 保存详细报告
    df_summary.to_excel('data/output/batch_test_summary.xlsx', index=False)
    print(f"\n详细报告已保存: data/output/batch_test_summary.xlsx")
