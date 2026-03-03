
import pandas as pd
from pathlib import Path

def repair_na_flags(csv_path: Path):
    if not csv_path.exists():
        print(f"File not found: {csv_path}")
        return
    
    df = pd.read_csv(str(csv_path), encoding='utf-8-sig', low_memory=False)
    role_cols = [c for c in df.columns if c not in ['文件', '序号', '案号', 'flag', '来源']]
    
    count = 0
    for idx, row in df.iterrows():
        # 检查所有角色列是否为空
        vals = [str(row[c]) for c in role_cols if pd.notna(row[c])]
        non_empty = [v for v in vals if v.strip() and v.lower() != 'nan']
        
        if not non_empty:
            if row['flag'] != 'NA':
                df.at[idx, 'flag'] = 'NA'
                count += 1
    
    if count > 0:
        df.to_csv(str(csv_path), index=False, encoding='utf-8-sig')
        print(f"Successfully repaired {count} rows in {csv_path}")
    else:
        print(f"No repair needed for {csv_path}")

if __name__ == "__main__":
    p = Path(r'e:\Referee-documents\data\output\2016年01月裁判文书数据_result.csv')
    repair_na_flags(p)
