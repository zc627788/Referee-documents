import pandas as pd
from pathlib import Path
from typing import List
from ..models import Person


def persons_to_dict_row(persons: List[Person], file_name: str, index: int, method: str = '', confidence: float = 0.0) -> dict:
    """
    将人员列表转换为字典行（用于DataFrame）
    
    Args:
        persons: 人员列表
        file_name: 文件名
        index: 序号
        method: 提取方法
        confidence: 置信度
        
    Returns:
        字典行
    """
    row = {
        '文件': file_name,
        '序号': index,
        '提取方法': method,
        '置信度': round(confidence, 2)
    }
    
    max_persons = max(7, len(persons))
    
    for i in range(max_persons):
        if i < len(persons):
            row[f'姓名{i+1}'] = persons[i].name
            row[f'角色{i+1}'] = persons[i].role
        else:
            row[f'姓名{i+1}'] = None
            row[f'角色{i+1}'] = None
    
    return row


def save_csv(data: List[dict], output_path: str):
    """保存为CSV"""
    df = pd.DataFrame(data)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8-sig')


def save_excel(data: List[dict], output_path: str):
    """保存为Excel"""
    df = pd.DataFrame(data)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)


def load_csv(input_path: str) -> pd.DataFrame:
    """加载CSV"""
    return pd.read_csv(input_path)


def print_statistics(stats: dict):
    """打印统计信息"""
    print("\n" + "="*60)
    print("处理统计")
    print("="*60)
    for key, value in stats.items():
        if isinstance(value, (int, float)):
            print(f"{key}: {value}")
        elif isinstance(value, dict):
            print(f"\n{key}:")
            for k, v in value.items():
                print(f"  {k}: {v}")
    print("="*60)
