import re
from typing import List, Tuple
import sys
from pathlib import Path

# 添加项目根目录到路径
if __name__ != '__main__':
    try:
        from ..models import Person
    except ImportError:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from models import Person
else:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from models import Person


class RuleExtractor:
    """规则提取器"""
    
    # 按优先级排序：先匹配长的（代理XX），后匹配短的（XX）
    ROLE_PATTERNS = {
        '代理审判长': r'代[\s\u3000]*理[\s\u3000]*审[\s\u3000]*判[\s\u3000]*长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '代理审判员': r'代[\s\u3000]*理[\s\u3000]*审[\s\u3000]*判[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '代书记员': r'代[\s\u3000]*书[\s\u3000]*记[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$|来自|关注)',
        '人民陪审员': r'人[\s\u3000]*民[\s\u3000]*陪[\s\u3000]*审[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '助理法官': r'助[\s\u3000]*理[\s\u3000]*法[\s\u3000]*官[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '副院长': r'副[\s\u3000]*院[\s\u3000]*长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '副庭长': r'副[\s\u3000]*庭[\s\u3000]*长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '审判长': r'审[\s\u3000]*判[\s\u3000]*长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '审判员': r'审[\s\u3000]*判[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '陪审员': r'陪[\s\u3000]*审[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '书记员': r'书[\s\u3000]*记[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$|来自|关注)',
        '执行员': r'执[\s\u3000]*行[\s\u3000]*员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '法官': r'法[\s\u3000]*官[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '院长': r'院[\s\u3000]*长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
        '庭长': r'庭[\s\u3000]*长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审|书|陪|代|执|法|院|庭|二[〇○0]|$)',
    }
    
    @staticmethod
    def extract(signature_text: str) -> Tuple[bool, float, List[Person]]:
        """
        使用规则提取
        
        Returns:
            (是否成功, 置信度, 人员列表)
        """
        persons = []
        matched_positions = set()  # 记录已匹配的位置，避免重复
        
        text = signature_text.replace('\n', ' ').replace('\r', ' ')
        text = re.sub(r'\s+', ' ', text)
        
        # 按顺序匹配（长的优先），避免重复匹配
        for role, pattern in RuleExtractor.ROLE_PATTERNS.items():
            matches = re.finditer(pattern, text)
            for match in matches:
                name = match.group(1).strip()
                pos = match.start()
                
                # 检查是否已经被匹配过（位置重叠）
                if any(abs(pos - p) < 10 for p in matched_positions):
                    continue
                
                if RuleExtractor._is_valid_name(name):
                    persons.append(Person(name=name, role=role))
                    matched_positions.add(pos)
        
        if len(persons) == 0:
            return False, 0.0, []
        
        confidence = min(1.0, len(persons) * 0.2 + 0.4)
        
        # 检查是否有重复的姓名
        names = [p.name for p in persons]
        if len(names) != len(set(names)):
            confidence *= 0.7
        
        success = confidence >= 0.7
        
        return success, confidence, persons
    
    @staticmethod
    def _is_valid_name(name: str) -> bool:
        """验证姓名是否合法"""
        if not name or len(name) < 2 or len(name) > 4:
            return False
        if not re.match(r'^[\u4e00-\u9fa5]+$', name):
            return False
        invalid_names = ['本院', '法院', '法庭', '人民', '公诉', '被告', '原告']
        if name in invalid_names:
            return False
        return True
