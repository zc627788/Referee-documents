#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
import io
import re
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, 'src')
from core.rule_extractor import COMMON_SURNAMES, COMPOUND_SURNAMES, INVALID_WORDS

# Test the filtering logic
BLACKLIST = INVALID_WORDS | {'人民', '法院', '执行', '刑事', '民事', '审判', '陪审', '助理', '无', 'null', 'None', '印章', '署名', '简易程', '普通程', '特别程'}

def test_filter(name):
    if not name or len(name) < 2 or len(name) > 4:
        return None
    
    if name in BLACKLIST:
        return None
    
    if not re.match(r'^[\u4e00-\u9fa5·]+$', name):
        return None
    
    invalid_chars = set('的地得与和及后前中上下左右处理执行宣布开庭公开有同等权利义务条款项程序')
    if any(c in invalid_chars for c in name):
        return None
    
    return name

# Test cases
test_names = ['简易程', '李国英', '黄婷', '简易', '程序']
for name in test_names:
    result = test_filter(name)
    print(f"{name}: {result if result else 'FILTERED OUT ✓'}")
