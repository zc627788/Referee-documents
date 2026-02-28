import re
from typing import List, Tuple
from ..models import Person


class RuleExtractor:
    """规则提取器"""
    
    ROLE_PATTERNS = {
        '审判长': r'审判长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '代理审判长': r'代理审判长[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '审判员': r'(?<!代理)审判员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '代理审判员': r'代理审判员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '陪审员': r'(?<!人民)陪审员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '人民陪审员': r'人民陪审员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '书记员': r'书记员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$|来自|关注)',
        '执行员': r'执行员[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '法官': r'(?<!助理)法官[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
        '助理法官': r'助理法官[\s\u3000：:]*?([\u4e00-\u9fa5]{2,4})(?=[\s\u3000]|审判|书记|陪审|代理|执行|法官|二[〇○0]|$)',
    }
    
    @staticmethod
    def extract(signature_text: str) -> Tuple[bool, float, List[Person]]:
        """
        使用规则提取
        
        Returns:
            (是否成功, 置信度, 人员列表)
        """
        persons = []
        
        text = signature_text.replace('\n', ' ').replace('\r', ' ')
        text = re.sub(r'\s+', ' ', text)
        
        for role, pattern in RuleExtractor.ROLE_PATTERNS.items():
            matches = re.finditer(pattern, text)
            for match in matches:
                name = match.group(1).strip()
                if RuleExtractor._is_valid_name(name):
                    persons.append(Person(name=name, role=role))
        
        if len(persons) == 0:
            return False, 0.0, []
        
        confidence = min(1.0, len(persons) * 0.2 + 0.4)
        
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
