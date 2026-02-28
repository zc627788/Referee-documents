"""分析多个月份数据的落款格式"""
import pandas as pd
import re
from pathlib import Path
from collections import Counter

def analyze_csv(csv_path):
    """分析单个CSV文件的落款"""
    df = pd.read_csv(csv_path)
    file_name = Path(csv_path).stem
    
    print(f"\n{'='*80}")
    print(f"分析文件: {file_name}")
    print(f"{'='*80}")
    print(f"总文书数: {len(df)}")
    
    # 提取最后1000字符，查找角色关键词
    role_keywords = []
    
    for idx, row in df.iterrows():
        if idx >= 5:  # 只分析前5条
            break
        
        full_text = str(row['全文'])
        tail_text = full_text[-800:]
        
        print(f"\n--- 第{idx+1}条文书落款（最后300字符）---")
        print(tail_text[-300:])
        
        # 查找所有可能的角色词
        roles = re.findall(r'(代理审判长|审判长|代理审判员|审判员|人民陪审员|陪审员|书记员|执行员|助理法官|法官|院长|副院长|庭长|副庭长)', tail_text)
        if roles:
            role_keywords.extend(roles)
            print(f"\n发现角色: {', '.join(set(roles))}")
    
    return role_keywords

# 分析所有月份
all_roles = []
input_dir = Path(r'D:\application\illegal\Referee-documents\data\input')

for csv_file in sorted(input_dir.glob('*.csv'))[:3]:  # 先分析前3个月
    roles = analyze_csv(csv_file)
    all_roles.extend(roles)

print(f"\n\n{'='*80}")
print("所有角色统计")
print(f"{'='*80}")
role_counter = Counter(all_roles)
for role, count in role_counter.most_common():
    print(f"{role}: {count}次")
