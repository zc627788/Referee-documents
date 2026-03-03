import pandas as pd
from pathlib import Path

queue_csv = Path(r'e:\Referee-documents\data\output\2016年01月裁判文书数据_ai_queue.csv')
df = pd.read_csv(str(queue_csv), encoding='utf-8-sig', low_memory=False)

ids_to_reset = ['525867_1028', '525667_177']

mask = df['snippet_id'].astype(str).isin(ids_to_reset)
df.loc[mask, '状态'] = '待处理'
df.loc[mask, 'AI提取姓名'] = ''

df.to_csv(str(queue_csv), index=False, encoding='utf-8-sig')
print(f"Successfully reset IDs: {ids_to_reset}")
