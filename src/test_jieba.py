import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from core.rule_extractor import RuleExtractor, COMMON_SURNAMES
import jieba.posseg as pseg
import re

queue_csv = Path('data/output/2016年01月裁判文书数据_ai_queue.csv')
if not queue_csv.exists():
    print("未找到 ai_queue.csv。请确保 data/output 中有该文件。")
    sys.exit(1)

df = pd.read_csv(str(queue_csv), encoding='utf-8-sig')
print(f"总计读取到 {len(df)} 条待精修数据。")

results = []
for _, row in df.iterrows():
    text = str(row['片段']).strip()
    role = str(row['角色'])
    
    idx = text.rfind(role)
    after = text[idx + len(role):] if idx != -1 else text
    after = re.sub(r'^[:：\s]*', '', after).strip()
    
    extracted = None
    if after:
        candidates = RuleExtractor._extract_candidates(after[:5], role)
        if candidates:
            cand = candidates[0]
            words = list(pseg.cut(after[:len(cand)+2]))
            if words and words[0].flag in ('nr', 'x', 'n', 'a'):
                extracted = cand
            elif len(cand) >= 2 and cand[0] in COMMON_SURNAMES:
                extracted = cand
                
    results.append({
        '角色': role,
        '原文片段': text,
        '截取后置': after,
        '提取结果': extracted if extracted else ''
    })

out_df = pd.DataFrame(results)
out_csv = Path('data/output/jieba_test_results.csv')
out_df.to_csv(str(out_csv), index=False, encoding='utf-8-sig')

# 打印一些提取成功的和未提取出来的供查看
success = out_df[out_df['提取结果'] != '']
failed = out_df[out_df['提取结果'] == '']

print(f"\n提取成功数: {len(success)} / {len(out_df)}")
print("=== 提取成功样例 (前 10 条) ===")
print(success[['角色', '截取后置', '提取结果']].head(10).to_string())

print("\n=== 未提取样例 (前 10 条) ===")
print(failed[['角色', '截取后置']].head(10).to_string())
print(f"\n结果已保存至: {out_csv}")
