"""落款区域定位器"""
import re
from typing import Optional


class SignatureLocator:
    KEYWORDS = [
        '代理审判长', '审判长', '代理审判员', '助理审判员', '审判员',
        '人民陪审员', '合议庭成员', '陪审员',
        '代理书记员', '代书记员', '书记员',
        '首席仲裁员', '主审法官', '执行法官', '执行员',
        '副主任', '主任', '副院长', '院长', '副庭长', '庭长',
        '仲裁员', '法官', '法警',
    ]

    @staticmethod
    def locate(full_text: str, tail_length: int = 1500) -> Optional[str]:
        """
        定位落款区域。
        取文末 tail_length 个字符，从后往前找最后一个关键词所在位置，
        向前扩展 600 字作为落款区域。
        """
        if not full_text or len(full_text) < 50:
            return None

        tail = full_text[-tail_length:]
        # 判断尾部是否包含任意关键词
        if not any(kw in tail for kw in SignatureLocator.KEYWORDS):
            return None

        # 从后往前找最后出现的关键词位置
        last_pos = -1
        for kw in SignatureLocator.KEYWORDS:
            p = tail.rfind(kw)
            if p > last_pos:
                last_pos = p

        if last_pos == -1:
            return None

        # 向前取600字作为落款区域
        start = max(0, last_pos - 600)
        return tail[start:]
