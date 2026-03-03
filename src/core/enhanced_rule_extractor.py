#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增强的规则提取器
在AI处理之前，使用更强的规则再次尝试提取
目标：减少真正需要调用AI的片段数量
"""
import re
import json
from typing import Optional, Tuple
from pathlib import Path
import sys

try:
    from .rule_extractor import COMMON_SURNAMES, COMPOUND_SURNAMES, ETHNIC_CHARS, INVALID_WORDS, RuleExtractor
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from core.rule_extractor import COMMON_SURNAMES, COMPOUND_SURNAMES, ETHNIC_CHARS, INVALID_WORDS, RuleExtractor


class EnhancedRuleExtractor:
    """增强的规则提取器，用于Phase2之前的预处理"""

    DEFAULT_THRESHOLD = 0.7
    _NAME_PATTERN = r'[\u4e00-\u9fa5·](?:\s*[\u4e00-\u9fa5·]){1,3}'
    _ROLE_BOUNDARY_RE = None
    _ROLE_BOUNDARIES = None
    _DATE_RE = re.compile(
        r'^(?:二〇\d{2}年|一九\d{2}年|20\d{2}年|\d{4}年'
        r'|[一二三四五六七八九十〇]{1,3}月[一二三四五六七八九十〇]{1,3}日'
        r'|\d{1,2}月\d{1,2}日'
        r'|[一二三四五六七八九十〇]{1,3}月)'
    )
    _BAD_SUFFIXES = (
        '担任', '主审', '独任', '适用', '代理', '审理', '组成', '参加', '审判',
        '合议', '主持', '负责', '审结', '裁定', '判决'
    )
    _NOISE_SUFFIXES = (
        '来自马克数据网', '马克数据网', '微信公众号', '来自于', '来自', 
        '附录', '来源', '更多', '搜索', '百度', '于', '任', '由', '和', '本', '与'
    )
    _NAME_BOUNDARY_PREFIXES = (
        '担任', '主审', '独任', '适用', '审理', '组成', '合议', '主持', '负责',
        '参加', '出庭', '程序', '经审', '依法', '本案', '根据', '由于', '由',
        '审结', '裁定', '判决',
        '附', '附：', '微信', '公众号', '关注', '来源', '更多', '数据', '搜索', '马克',
        '来自', '简易程序', '普通程序'
    )
    _MASK_PREFIXES = ('某', '×', '＊', 'X', 'x')
    
    @staticmethod
    def try_extract(snippet: str, role: str, threshold: Optional[float] = None) -> Optional[str]:
        """
        尝试从片段中提取姓名（支持置信度阈值）
        
        Args:
            snippet: 文本片段
            role: 角色词
            threshold: 置信度阈值（默认0.7）
        
        Returns:
            提取到的姓名；空字符串表示确定无姓名；None表示不确定
        """
        name, score, _ = EnhancedRuleExtractor.try_extract_with_score(snippet, role, threshold)
        return name

    @staticmethod
    def try_extract_with_score(snippet: str, role: str, threshold: Optional[float] = None):
        """
        带置信度输出的提取（用于二次验证/统计）
        
        Returns:
            (name_or_empty_or_none, score, reason)
        """
        snippet = str(snippet).strip()
        if threshold is None:
            threshold = EnhancedRuleExtractor.DEFAULT_THRESHOLD

        if EnhancedRuleExtractor._is_no_name_snippet(snippet, role):
            return "", 1.0, "no_name"

        candidates = EnhancedRuleExtractor._collect_candidates(snippet, role)
        if not candidates:
            return None, 0.0, "uncertain"

        # 选取得分最高的候选
        best = max(candidates, key=lambda x: x["score"])
        if best["score"] >= threshold:
            return best["name"], best["score"], best["reason"]
        return None, best["score"], "low_confidence"

    @staticmethod
    def _collect_candidates(snippet: str, role: str):
        """收集候选姓名并评分"""
        snippet = EnhancedRuleExtractor._normalize_snippet(snippet)
        candidates = []
        role_re = re.escape(role)
        role_boundary = EnhancedRuleExtractor._get_role_boundary_regex()
        role_boundaries = EnhancedRuleExtractor._get_role_boundaries()
        if role_boundary:
            boundary = rf'(?=(?:{role_boundary})|[，。、；\s）\n\r]|$|(?:二〇\d{{2}}年|一九\d{{2}}年|20\d{{2}}年|\d{{4}}年))'
        else:
            boundary = r'(?=[，。、；\s）\n\r]|$|(?:二〇\d{2}年|一九\d{2}年|20\d{2}年|\d{4}年))'

        def _normalize_name(raw: str) -> str:
            return re.sub(r"\s+", "", raw)

        def _strip_bad_suffix(name: str) -> str:
            for suffix in EnhancedRuleExtractor._BAD_SUFFIXES:
                if name.endswith(suffix) and len(name) > len(suffix):
                    stripped = name[:-len(suffix)]
                    if len(stripped) >= 2:
                        return stripped
            return name

        def _strip_noise_suffix(name: str) -> str:
            # 优先匹配长后缀（水印和噪声）
            long_suffixes = (
                '来自马克数据网', '马克数据网', '微信公众号', '来自于', '来自',
                '附录', '来源', '更多', '搜索', '百度',
                '法律条文', '法规条文', '法律', '条文', '文书',
                '适用', '人民', '审查', '进行', '处理',
            )
            for suffix in long_suffixes:
                if name.endswith(suffix) and len(name) > len(suffix):
                    trimmed = name[:-len(suffix)]
                    if EnhancedRuleExtractor._is_valid_name(trimmed):
                        return trimmed
            
            # 单字噪声后缀（保护3字以下复姓，4字复姓名仍可去噪）
            single_suffixes = ('于', '任', '由', '和', '本', '与', '按', '后')
            for suffix in single_suffixes:
                if name.endswith(suffix) and len(name) > len(suffix):
                    if len(name) <= 2:
                        continue
                    # 复姓保护仅限3字，4字复姓名仍可去噪
                    if name[:2] in COMPOUND_SURNAMES and len(name) <= 3:
                        continue
                    trimmed = name[:-len(suffix)]
                    if EnhancedRuleExtractor._is_valid_name(trimmed):
                        return trimmed
            return name

        def _score_name(name: str, base: float) -> float:
            score = base
            if name[:2] in COMPOUND_SURNAMES:
                score += 0.05
            elif name[0] in COMMON_SURNAMES or name[0] in ETHNIC_CHARS:
                score += 0.03
            if len(name) == 2:
                score += 0.05
            elif len(name) == 3:
                score += 0.03
            elif len(name) >= 4:
                score -= 0.05
            return min(1.0, max(0.0, score))

        def _push(raw_name: str, base: float, reason: str):
            name = _strip_noise_suffix(_strip_bad_suffix(_normalize_name(raw_name)))
            if EnhancedRuleExtractor._is_valid_name(name):
                candidates.append({
                    "name": name,
                    "score": _score_name(name, base),
                    "reason": reason,
                })

        # 模式1: 角色词 + 冒号 + 姓名
        for m in re.finditer(rf"{role_re}[：:]\s*({EnhancedRuleExtractor._NAME_PATTERN}){boundary}", snippet):
            _push(m.group(1), 0.95, "role_colon_name")

        # 模式2: 角色词 + 空格 + 姓名
        for m in re.finditer(rf"{role_re}\s+({EnhancedRuleExtractor._NAME_PATTERN}){boundary}", snippet):
            _push(m.group(1), 0.85, "role_space_name")

        # 模式3: 角色词 + 姓名 + 标点
        for m in re.finditer(rf"{role_re}({EnhancedRuleExtractor._NAME_PATTERN}){boundary}", snippet):
            _push(m.group(1), 0.80, "role_name_punct")

        # 模式4: 姓名 + 角色词（反向）
        for m in re.finditer(rf"({EnhancedRuleExtractor._NAME_PATTERN})\s*{role_re}", snippet):
            _push(m.group(1), 0.78, "name_before_role")

        # 模式5: 角色词后基于边界的截断（多角色/落款块）
        extracted = EnhancedRuleExtractor._extract_by_boundary(snippet, role, role_boundaries)
        for cand in extracted:
            _push(cand, 0.82, "role_name_boundary")

        return candidates
    
    @staticmethod
    def _is_valid_name(name: str) -> bool:
        """验证是否是有效姓名"""
        return RuleExtractor._is_valid_name(name)

    @staticmethod
    def _get_role_boundary_regex() -> str:
        """获取用于边界判断的角色正则（从 roles.json 加载）"""
        if EnhancedRuleExtractor._ROLE_BOUNDARY_RE is not None:
            return EnhancedRuleExtractor._ROLE_BOUNDARY_RE

        roles = []
        for base in [Path(__file__).parent.parent.parent, Path.cwd()]:
            p = base / 'config' / 'roles.json'
            if p.exists():
                try:
                    with open(p, encoding='utf-8') as f:
                        data = json.load(f)
                    roles = [r.get('name') for r in data.get('roles', []) if r.get('name')]
                except Exception:
                    roles = []
                break

        roles = sorted(set(roles), key=len, reverse=True)
        EnhancedRuleExtractor._ROLE_BOUNDARIES = roles
        EnhancedRuleExtractor._ROLE_BOUNDARY_RE = '|'.join(re.escape(r) for r in roles) if roles else ''
        return EnhancedRuleExtractor._ROLE_BOUNDARY_RE

    @staticmethod
    def _get_role_boundaries() -> list:
        """返回角色边界列表"""
        if EnhancedRuleExtractor._ROLE_BOUNDARIES is None:
            EnhancedRuleExtractor._get_role_boundary_regex()
        return EnhancedRuleExtractor._ROLE_BOUNDARIES or []

    @staticmethod
    def _normalize_snippet(snippet: str) -> str:
        """基础清洗：合并汉字间空格、去全角空格"""
        text = re.sub(r'[\u3000\xa0]', ' ', str(snippet))
        text = re.sub(r'[\t ]{2,}', ' ', text)
        for _ in range(3):
            new = re.sub(r'([\u4e00-\u9fa5])\s+([\u4e00-\u9fa5])', r'\1\2', text)
            if new == text:
                break
            text = new
        return text

    @staticmethod
    def _extract_by_boundary(snippet: str, role: str, boundaries: list) -> list:
        """根据边界（标点/日期/下一角色）截断姓名"""
        snippet = EnhancedRuleExtractor._normalize_snippet(snippet)
        idx = snippet.find(role)
        if idx == -1:
            return []
        after = snippet[idx + len(role):]
        after = re.sub(r'^[：:\s]+', '', after)
        if not after:
            return []

        def is_boundary(tail: str) -> bool:
            if not tail:
                return True
            c = tail[0]
            if c in '，。、；：？！()（）\n\r \t':
                return True
            if EnhancedRuleExtractor._DATE_RE.match(tail):
                return True
            for prefix in EnhancedRuleExtractor._NAME_BOUNDARY_PREFIXES:
                if tail.startswith(prefix):
                    return True
            for r in boundaries:
                if tail.startswith(r):
                    return True
            return False

        candidates = []
        for length in [4, 3, 2]:
            if len(after) < length:
                continue
            cand = after[:length]
            tail = after[length:]
            if is_boundary(tail):
                candidates.append(cand)
        return candidates
    
    @staticmethod
    def _is_no_name_snippet(snippet: str, role: str) -> bool:
        """
        判断片段是否确定不包含姓名
        这些片段可以直接跳过，不需要AI处理
        """
        # 检查角色词后面的内容
        idx = snippet.find(role)
        if idx == -1:
            return False
        
        after = snippet[idx + len(role):].strip()
        if not after:
            return True
        
        # 移除开头的冒号和空格
        after = re.sub(r'^[：:\s]+', '', after)

        # 匿名占位（如 某某、××、XX）
        compact = re.sub(r'\s+', '', after)
        if compact and compact[0] in EnhancedRuleExtractor._MASK_PREFIXES:
            return True
        
        # 模式1: 纯日期
        # 例如：审判员二〇一六年一月
        if re.match(r'^[二一]〇\d{2}年|^20\d{2}年|^\d{4}年\d{1,2}月', after):
            return True
        
        # 模式2: 开头是介词或连词
        # 例如：审判员于、审判员根据
        if re.match(r'^[于根据依照按照经过通过]', after):
            return True
        
        # 模式3: 法律文书常见词汇
        # 例如：审判员法律、审判员依法
        if re.match(r'^(法律|条文|规定|条款|依法|本院|本案|查明|认定|判决|裁定|简易程序|普通程序)', after):
            return True
        
        # 模式4: 来源、引用等
        # 例如：审判员来源、审判员百度
        if re.match(r'^(来源|百度|微信|马克|数据|搜索|关注|公众|更多|信息)', after):
            return True
        
        # 模式5: 以"经"开头的（经审查、经审理等）
        if re.match(r'^经[审查理]', after):
            return True
        
        return False
    
    @staticmethod
    def batch_filter(snippets: list) -> Tuple[dict, list]:
        """
        批量过滤片段
        
        Args:
            snippets: 片段列表 [{'id': str, 'role': str, 'text': str}, ...]
        
        Returns:
            (extracted_map, remaining_snippets)
            - extracted_map: 已提取的结果 {snippet_id: {'name': str, 'role': str}}
            - remaining_snippets: 仍需AI处理的片段列表
        """
        extracted = {}
        remaining = []
        
        for s in snippets:
            snippet_id = s['id']
            role = s['role']
            text = s['text']
            
            name = EnhancedRuleExtractor.try_extract(text, role)
            
            if name is None:
                # 不确定，需要AI
                remaining.append(s)
            elif name == "":
                # 确定无姓名，跳过（不加入extracted）
                pass
            else:
                # 提取到姓名
                extracted[snippet_id] = {
                    'name': name,
                    'role': role
                }
        
        return extracted, remaining
