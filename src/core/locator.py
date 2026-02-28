from typing import Optional


class SignatureLocator:
    """落款定位器"""
    
    SIGNATURE_KEYWORDS = [
        '审判长', '审判员', '代理审判员', '陪审员', 
        '人民陪审员', '书记员', '执行员', '法官'
    ]
    
    @staticmethod
    def locate(full_text: str, tail_length: int = 1000) -> Optional[str]:
        """
        定位落款区域
        
        Args:
            full_text: 裁判文书全文
            tail_length: 从末尾截取的字符数
            
        Returns:
            落款文本，如果未找到返回None
        """
        if not full_text or len(full_text) < 100:
            return None
        
        tail_text = full_text[-tail_length:]
        
        has_signature = any(keyword in tail_text for keyword in SignatureLocator.SIGNATURE_KEYWORDS)
        
        if not has_signature:
            return None
        
        last_keyword_pos = -1
        for keyword in SignatureLocator.SIGNATURE_KEYWORDS:
            pos = tail_text.rfind(keyword)
            if pos > last_keyword_pos:
                last_keyword_pos = pos
        
        if last_keyword_pos == -1:
            return None
        
        start_pos = max(0, last_keyword_pos - 600)
        signature_area = tail_text[start_pos:]
        
        return signature_area
    
    @staticmethod
    def find_keyword_positions(full_text: str) -> dict:
        """
        在全文中查找关键词位置（用于无落款文书的智能搜索）
        
        Returns:
            {keyword: [pos1, pos2, ...]}
        """
        positions = {}
        for keyword in SignatureLocator.SIGNATURE_KEYWORDS:
            pos_list = []
            start = 0
            while True:
                pos = full_text.find(keyword, start)
                if pos == -1:
                    break
                pos_list.append(pos)
                start = pos + 1
            if pos_list:
                positions[keyword] = pos_list
        return positions
    
    @staticmethod
    def extract_context(full_text: str, positions: dict, window_size: int = 200) -> str:
        """
        从关键词位置提取上下文
        
        Args:
            full_text: 全文
            positions: 关键词位置字典
            window_size: 窗口大小
            
        Returns:
            合并后的候选文本
        """
        contexts = []
        seen_ranges = set()
        
        for keyword, pos_list in positions.items():
            for pos in pos_list:
                start = max(0, pos - window_size)
                end = min(len(full_text), pos + window_size)
                
                range_key = (start, end)
                if range_key not in seen_ranges:
                    contexts.append(full_text[start:end])
                    seen_ranges.add(range_key)
        
        return '\n---\n'.join(contexts)
