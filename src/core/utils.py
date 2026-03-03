"""工具函数：宽表输出格式（每角色一列，多人用分号分隔）"""
import csv
import json
import threading
from pathlib import Path
from typing import List, Dict
from collections import defaultdict
import sys

try:
    from ..models import Person
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from models import Person


def _load_role_columns() -> List[str]:
    """从 roles.json 读取列顺序"""
    target_roles = None
    try:
        from .config import Config
        config = Config()
        extraction_cfg = config.get('extraction', {})
        if 'target_roles' in extraction_cfg:
            target_roles = extraction_cfg['target_roles']
    except Exception:
        target_roles = None

    if target_roles:
        return ['文件', '序号', '案号'] + list(target_roles) + ['flag', '来源']

    for base in [Path(__file__).parent.parent.parent, Path.cwd()]:
        p = base / 'config' / 'roles.json'
        if p.exists():
            with open(p, encoding='utf-8') as f:
                data = json.load(f)
            ordered = sorted(data['roles'], key=lambda x: (x['priority'], -len(x['name'])))
            return ['文件', '序号', '案号'] + [r['name'] for r in ordered] + ['flag', '来源']
    return ['文件', '序号', '案号', '审判长', '审判员', '书记员', 'flag', '来源']


# 模块加载时确定固定列顺序
ROLE_COLUMNS: List[str] = _load_role_columns()


def persons_to_wide_row(persons: List[Person], file_name: str, index: int, case_no: str, source: str = '规则', ai_roles: List[str] = None) -> Dict:
    """
    将人员列表转为宽表字典行。
    同一角色多人时用 ; 拼接，如：'张三;李四'
    source: 来源标识，如 '规则' 或 '规则+AI'
    ai_roles: 进入了 AI 队列的角色列表
    """
    row: Dict = {'文件': file_name, '序号': index, '案号': case_no}
    groups: Dict[str, List[str]] = defaultdict(list)
    for p in persons:
        if not p.role or not p.name:
            continue
        if p.name not in groups[p.role]:
            groups[p.role].append(p.name)
        
    has_any_person = False
    for col in ROLE_COLUMNS[3:-2]:  # 跳过 文件/序号/案号 和最后的 flag/来源
        val = ';'.join(groups[col]) if col in groups else ''
        row[col] = val
        if val and str(val).strip() and str(val) != 'nan':
            has_any_person = True
            
    if ai_roles:
        roles_str = ', '.join(sorted(set(ai_roles)))
        row['flag'] = f'需要AI处理: {roles_str}'
    elif not has_any_person:
        row['flag'] = 'NA'
    else:
        row['flag'] = ''
        
    row['来源'] = source
    return row


class IncrementalCSVWriter:
    """追加写 CSV，第一次写时创建并写表头，之后直接追加。"""

    def __init__(self, output_path: str, columns: List[str]):
        self.path = output_path
        self.columns = columns
        self._lock = threading.Lock()
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        self._wrote_header = Path(output_path).exists() and Path(output_path).stat().st_size > 0

    def append(self, row: Dict):
        with self._lock:
            mode = 'a' if self._wrote_header else 'w'
            with open(self.path, mode, encoding='utf-8-sig', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.columns, extrasaction='ignore')
                if not self._wrote_header:
                    w.writeheader()
                    self._wrote_header = True
                w.writerow(row)

    def append_many(self, rows: List[Dict]):
        if not rows:
            return
        with self._lock:
            mode = 'a' if self._wrote_header else 'w'
            with open(self.path, mode, encoding='utf-8-sig', newline='') as f:
                w = csv.DictWriter(f, fieldnames=self.columns, extrasaction='ignore')
                if not self._wrote_header:
                    w.writeheader()
                    self._wrote_header = True
                w.writerows(rows)


def print_statistics(stats: dict):
    print('\n' + '=' * 60)
    print('处理统计')
    print('=' * 60)
    for k, v in stats.items():
        print(f'  {k}: {v}')
    print('=' * 60)
