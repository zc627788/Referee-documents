"""
规则提取器 v5.0 (全文扫描 + 边界判断 + AI队列)
不再依赖落款定位，直接扫描全文中所有角色关键词，用边界字符判断姓名确定性。
  - 确定（名字后是空白/标点/文末）→ 直接写入结果
  - 不确定（名字后是中文字符）→ 放入 AI 队列
"""
import re
import json
from pathlib import Path
from typing import List, Tuple, Dict, Optional
from dataclasses import dataclass
import sys

try:
    from ..models import Person
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from models import Person


# ── 边界字符集 ──────────────────────────────────────────
# 名字后面紧跟这些字符 → 100%确定名字已结束
_BOUNDARY_CHARS = frozenset(
    ' \t\n\r\u3000\xa0'                     # 空白类
    '，。、；：？！""''（）《》【】〈〉｛｝'    # 中文标点
    ',.;:?!()[]{}/<>"\''                    # 英文标点
    '\r\n'                                   # 换行
)

# ── 无效词黑名单（碰巧出现的2字动名词，不是人名）──
INVALID_WORDS = frozenset({
    '本院', '法院', '法庭', '人民', '公诉', '被告', '原告',
    '审判', '书记', '陪审', '执行', '合议', '仲裁',
    '委托', '代理', '特别', '送达', '认定', '依法',
    '中华', '共和', '最高', '高级', '中级', '基层',
    '民事', '刑事', '行政', '商事', '海事',
    '一审', '二审', '再审', '简易', '普通',
    '法律', '规定', '条款', '协议', '合同',
    '条文', '财产', '理由', '裁定', '判决', '申请',
    '解除', '查封', '冻结', '印章', '笔录', '结束',
    '来源', '百度', '搜索', '关注', '微信', '公众',
    '马克', '数据', '更多', '信息', '网站', '署名',
    '扶养', '抚养', '通知', '赡养',
    '公共', '安全', '秩序', '经济', '回避', '处理',
    '从宽', '文书', '引用', '本案', '本判', '决定',
    '查清', '查明', '迟延', '指出', '无罪', '宣布', '休庭',
    '保全', '许可', '缺席', '撤回', '起诉', '事实', '不清',
    '宣布休庭', '法律条文', '特别授权', '适用法', '正当行',
    '提醒', '提出', '经向', '经审', '经本院', '爱民之心', '上述事实',
    '危害', '上列', '下列', '见习', '须知',
    '公正', '都有', '有一', '希望', '应当', '必须', '应该', '能够',
    '一颗', '之心', '心态', '态度', '精神', '品德', '信念',
    '做到', '实现', '保障', '保护', '维护', '服务',
    '都有一', '都希望', '都应当', '都必须', '都能够', '都有一颗',
    '劳动', '需要', '进行', '要求', '同意', '听取', '责令', '征收',
    '其它', '其它事', '其他', '提出申请', '依法审', '依法作',
    '仲裁委', '仲裁委员', '成立仲', '符合法',
})

# ── 首字黑名单 ─────────────────────────────────────────
_DIGIT_CHARS = frozenset('一二三四五六七八九十零〇')

# ── 百家姓与少数民族首字字典 ───────────────────────────
COMMON_SURNAMES = frozenset(
    "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻"
    "柏水窦章云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任袁柳酆鲍史唐费廉岑薛雷贺倪汤"
    "滕殷罗毕郝邬安常乐于时傅皮卞齐康伍余元卜顾孟平黄和穆萧尹姚邵汪祁毛禹狄米"
    "贝明臧计伏成戴谈宋茅庞熊纪舒屈项祝董梁杜阮蓝闵席季麻强贾路娄危江童颜郭梅"
    "盛林刁钟徐邱骆高夏蔡田胡凌霍虞万支柯昝管卢莫经房裘缪干解应宗丁宣贲邓郁单"
    "杭洪包诸左石崔吉钮龚程嵇邢滑裴陆荣翁荀羊甄家封芮羿储靳汲邴糜松井段富巫乌"
    "焦巴弓牧隗山谷车侯宓蓬全郗班仰秋仲伊宫宁仇栾暴甘钭厉戎祖武符刘景詹束龙叶"
    "幸司韶郜黎蓟薄印宿白怀蒲台从鄂索咸赖卓蔺屠蒙池乔阴冒翟先敬沙欧盖燕桓公万"
    "俟司马上官欧阳夏侯诸葛闻人东方赫连皇甫尉迟公羊澹台公冶宗政濮阳淳于单于太"
    "叔申屠公孙仲孙轩辕令狐钟离宇文长孙慕容鲜于闾丘司徒司空亓官司寇仉督子车颛"
    "孙端木巫马公西漆雕乐正壤驷公良拓跋夹谷宰父谷梁晋楚闫法汝鄢涂钦段干百里东"
    "郭南门呼延归海羊舌微生岳帅缑亢况郈有琴梁丘左丘东门西门商牟佘佴伯赏南宫墨"
    "哈谯笪年爱阳佟第五言福"
    "曾艾申肖衣隋师晏温迟关丛游麦门库脱木妥"
    "冉谭拜朝介向圣吕希那达益央旺扎尼普格桑仁增"
    "覃铁付樊宝图迪练柴"
    "廖聂曲边牛綦楼庄过兰劳耿栗易苑农匡桂苟谌邸才"
    "阎逯岩弋侍虎郄鞠初冷惠简戈盘莎尚寿湛辛晁沃敖饶"
    "刀信税豆甲都瓦来相"
    "牟岳商文延古吐涂仉闻官佟闫亢漆次"
    "塔玛音丹吾"
)

COMPOUND_SURNAMES = frozenset([
    "万俟", "司马", "上官", "欧阳", "夏侯", "诸葛", "闻人", "东方",
    "赫连", "皇甫", "尉迟", "公羊", "澹台", "公冶", "宗政", "濮阳",
    "淳于", "单于", "太叔", "申屠", "公孙", "仲孙", "轩辕", "令狐",
    "钟离", "宇文", "长孙", "慕容", "鲜于", "闾丘", "司徒", "司空",
    "亓官", "司寇", "子车", "颛孙", "端木", "巫马", "公西", "漆雕",
    "乐正", "壤驷", "公良", "拓跋", "夹谷", "宰父", "谷梁", "段干",
    "百里", "东郭", "南门", "呼延", "归海", "羊舌", "微生", "梁丘",
    "左丘", "东门", "西门", "南宫", "第五"
])

ETHNIC_CHARS = frozenset(
    "阿买吐巴欧次旦吾合扎吉热依努木库拉卡克麦买沙苏古斯帕买买提帕提玛"
    "哈斯木加孜布里亚特蒙古维吾尔藏科"
)


# ── 年份正则 ──
_YEAR_RE = re.compile(r'(?:一九|二[〇０O0ＯΟО○零][一二三四五六七八九十\d]{2}\s*年|20[0-9]{2}\s*年)')


def _is_boundary_text(tail: str) -> bool:
    """
    判断名字后面的文本是否构成边界。
    tail: 名字后面的文本（已跳过排版空格）。
    """
    if not tail:
        return True
    c = tail[0]
    # 1. 换行符
    if c in '\n\r':
        return True
    # 2. 标点符号
    if c in '，。、；：？！\u201c\u201d\u2018\u2019（）《》【】〈〉,.;:?!()[]{}/':
        return True
    # 3. 日期模式
    if c in '二一〇零０012':
        if _YEAR_RE.match(tail):
            return True
        if c in '12' and len(tail) >= 2 and tail[1].isdigit():
            return True
        if c == '二' and len(tail) >= 2 and tail[1] in '〇０0OＯΟО○零':
            return True
    # 4. 角色关键词
    for r in ROLES:
        if tail.startswith(r):
            return True
    # 5. 法律文书常见后缀
    for suffix in ('附', '更多', '来源', '本案', '来自', '搜索', '百度', '马克',
                   '微信', '关注', '公众', '此件', '法律', '条文'):
        if tail.startswith(suffix):
            return True
    return False


# ── 不确定片段结构 ──────────────────────────────────────
@dataclass
class UncertainSnippet:
    """规则无法确定的片段，需要交给AI处理"""
    role: str           # 角色词，如"审判员"
    snippet: str        # 角色词+上下文片段（前5字+角色词+后30字）
    position: int       # 角色词在原文中的位置


def _load_roles() -> List[str]:
    """从 config/roles.json 加载角色列表（priority 升序, 名称长度降序）。"""
    for p in [Path(__file__).parent.parent.parent, Path.cwd()]:
        path = p / 'config' / 'roles.json'
        if path.exists():
            with open(path, encoding='utf-8') as f:
                data = json.load(f)
            return [r['name'] for r in sorted(
                data['roles'], key=lambda x: (x['priority'], -len(x['name']))
            )]
    return ['代理审判长', '审判长', '代理审判员', '审判员',
            '人民陪审判员', '人民陪审员', '陪审员', '书记员', '代书记员', '执行员', '法官']


ROLES: List[str] = _load_roles()
ROLES_SET: frozenset = frozenset(ROLES)


class RuleExtractor:
    """v5.0 全文扫描提取器：边界字符判断确定性"""

    @staticmethod
    def extract_fulltext(full_text: str) -> Tuple[List[Person], List[UncertainSnippet]]:
        """
        全文扫描提取。
        Returns:
            certain_persons: 确定的人员列表（名字后有明确边界）
            uncertain_snippets: 不确定的片段列表（需AI处理）
        """
        certain: List[Person] = []
        uncertain: List[UncertainSnippet] = []
        seen_certain: set = set()
        seen_uncertain: set = set()

        # 1. 基础清洗（统一空格、修复角色词内空格）
        text = RuleExtractor._normalize(full_text)

        # 2. 定位所有角色关键词（长优先去重叠）
        markers = []
        for r in ROLES:
            for m in re.finditer(re.escape(r), text):
                markers.append({'val': r, 'start': m.start(), 'end': m.end()})

        markers.sort(key=lambda x: (x['start'], -(x['end'] - x['start'])))
        deduped = []
        for mk in markers:
            if deduped and mk['start'] < deduped[-1]['end']:
                continue
            deduped.append(mk)
        markers = deduped

        # 3. 对每个角色关键词做名字提取 + 边界判断
        for i, mk in enumerate(markers):
            role = mk['val']
            role_end = mk['end']

            # 取角色词后30字作为搜索区
            raw_after = text[role_end:role_end + 30]

            # 清理搜索区：去全角ASCII、乱码
            search = re.sub(r'[\uff01-\uff5e]', '', raw_after)
            search = re.sub(r'[?\ufffd]+', '', search)

            # 合并汉字间空格
            search = re.sub(r'\s+', ' ', search).strip()
            for _ in range(5):
                new = re.sub(r'([\u4e00-\u9fa5]) ([\u4e00-\u9fa5])', r'\1\2', search)
                if new == search:
                    break
                search = new

            # 去除开头冒号
            search = re.sub(r'^[：:]+', '', search).strip()
            if not search:
                continue

            # 标点分割成候选组
            parts = re.split(r'[、，。；：\.,;:（）\(\)《》<>\n\r]+', search)

            for part in parts:
                part = part.strip()
                if not part:
                    continue

                # 按空格再拆
                candidates = part.split()
                for cand_raw in candidates:
                    if not cand_raw:
                        continue

                    # 在原文中找到候选名字的确切位置，以判断其后的边界字符
                    # cand_raw 可能很长（粘连），我们尝试提取2~4字的候选
                    names_extracted = RuleExtractor._extract_candidates(cand_raw, role)
                    if not names_extracted:
                        # 没提取到合法名字 → 可能是无效文本或需AI判断
                        # 只有当cand_raw的首字是百家姓/少民字时才放入AI队列
                        if (len(cand_raw) >= 2
                                and (cand_raw[0] in COMMON_SURNAMES or cand_raw[0] in ETHNIC_CHARS)
                                and cand_raw not in INVALID_WORDS
                                and cand_raw[:2] not in INVALID_WORDS):
                            snippet_key = (role, cand_raw[:10])
                            if snippet_key not in seen_uncertain:
                                seen_uncertain.add(snippet_key)
                                # 构建上下文片段
                                ctx_start = max(0, mk['start'] - 5)
                                ctx_end = min(len(text), role_end + 35)
                                snippet_text = text[ctx_start:ctx_end]
                                uncertain.append(UncertainSnippet(
                                    role=role, snippet=snippet_text, position=mk['start']
                                ))
                        continue

                    for name in names_extracted:
                        # ── 核心：边界字符判断 ──────────
                        # 在 raw_after 中找名字（允许字间空格）
                        name_pattern = r'\s*'.join(re.escape(c) for c in name)
                        match = re.search(name_pattern, raw_after)
                        
                        if match:
                            pos = match.end()
                            # 取名字后面的剩余文本（跳过排版空格）
                            remaining = raw_after[pos:]
                            tail = remaining.lstrip(' \t\u3000\xa0')
                            
                            if not tail:
                                # 搜索区结尾 → 从原文往后看
                                abs_pos = role_end + len(raw_after)
                                while abs_pos < len(text) and text[abs_pos] in ' \t\u3000\xa0':
                                    abs_pos += 1
                                tail = text[abs_pos:abs_pos+10] if abs_pos < len(text) else ''
                            
                            is_certain = _is_boundary_text(tail)
                        else:
                            is_certain = False

                        key = (role, name)
                        if is_certain:
                            if key not in seen_certain:
                                seen_certain.add(key)
                                certain.append(Person(name=name, role=role))
                        else:
                            if key not in seen_uncertain and key not in seen_certain:
                                seen_uncertain.add(key)
                                ctx_start = max(0, mk['start'] - 5)
                                ctx_end = min(len(text), role_end + 35)
                                snippet_text = text[ctx_start:ctx_end]
                                uncertain.append(UncertainSnippet(
                                    role=role, snippet=snippet_text, position=mk['start']
                                ))

        return certain, uncertain

    @staticmethod
    def _extract_candidates(raw: str, role: str) -> List[str]:
        """从一个可能粘连的字符串中，提取所有合法姓名候选"""
        names = []

        if len(raw) <= 4:
            # 短字符串：直接验证
            # 4字名末尾是职务前置字 → 截3
            if len(raw) == 4 and raw[-1] in '代本书法记长员兼一':
                if RuleExtractor._is_valid_name(raw[:3]):
                    names.append(raw[:3])
                    return names
            # 4字名末尾是双字角色词 → 截2
            if len(raw) == 4 and raw[2:] in ('主审', '注本', '主任', '实习'):
                if RuleExtractor._is_valid_name(raw[:2]):
                    names.append(raw[:2])
                    return names
            # 4字候选：非复姓非少民 → 拆两个2字名
            if len(raw) == 4 and '·' not in raw:
                if (raw[:2] not in COMPOUND_SURNAMES
                        and raw[0] not in ETHNIC_CHARS
                        and raw[2] not in ETHNIC_CHARS):
                    n1, n2 = raw[:2], raw[2:]
                    if (n1[0] in COMMON_SURNAMES and n2[0] in COMMON_SURNAMES
                            and RuleExtractor._is_valid_name(n1)
                            and RuleExtractor._is_valid_name(n2)):
                        names.extend([n1, n2])
                        return names
            # 整体验证
            if RuleExtractor._is_valid_name(raw):
                names.append(raw)
                return names
        else:
            # 长字符串（>4字，粘连）：从开头截取2~4字
            for l in [3, 2, 4]:
                sub = raw[:l]
                if RuleExtractor._is_valid_name(sub):
                    names.append(sub)
                    return names
        return names

    @staticmethod
    def _normalize(text: str) -> str:
        """基础清洗"""
        text = re.sub(r'[\u3000\xa0]', ' ', text)
        text = text.replace('〇', '〇')
        text = re.sub(r'[\ufffd]+', ' ', text)
        # 修复角色词内部空格（如"审 判 长" → "审判长"）
        for role in ROLES:
            spaced = r'\s*'.join(re.escape(c) for c in role)
            text = re.sub(spaced, role, text)
        text = re.sub(r'[ \t]{3,}', '  ', text)
        return text

    @staticmethod
    def _is_valid_name(name: str) -> bool:
        """姓名验证"""
        length = len(name)
        if length < 2 or length > 12:
            return False
        if not re.match(r'^[\u4e00-\u9fa5·]+$', name):
            return False
        if name in INVALID_WORDS or name in ROLES_SET:
            return False
        if re.search(r'[年月日时〇零百千亿]', name):
            return False
        if name[0] in _DIGIT_CHARS:
            return False
        if name[-1] in '搜附第页及即亦另打注申引结数内号字县市区省乡镇村庭厅局委办处股科院网':
            return False
        if name[0] in '第案本该其此各':
            return False
        # 地名拦截
        if name[:2] in {'商丘', '郑州', '洛阳', '北京', '上海', '广州', '深圳', '天津', '重庆',
                        '杭州', '成都', '武汉', '西安', '济南', '南阳', '平顶山', '新乡', '许昌',
                        '漯河', '三门峡', '信阳', '周口', '驻马店', '济源', '安阳', '鹤壁', '濮阳',
                        '温州', '苏州', '广陵', '通州', '海州', '荆州', '宿州', '卓城', '合肥',
                        '长沙', '山东', '浙江', '云南', '乐陵', '卫辉', '宁陵', '夏邑', '虞城',
                        '柘城', '睢县', '民权', '梁园', '睢阳'}:
            return False
        if '之' in name and length >= 3:
            return False
        if name.startswith(('应当', '依法', '同意', '要求', '必须', '需要', '已经', '进行', '可以')):
            return False
        if name.endswith(('责任', '听取', '责令', '征收')):
            return False
        # 少民·名 → 放宽
        if '·' in name or name[0] in ETHNIC_CHARS:
            return True
        if length > 4:
            return False
        if length >= 3 and name[:2] in COMPOUND_SURNAMES:
            return True
        if name[0] in COMMON_SURNAMES:
            return True
        return False

    # ── 保留旧接口供兼容 ──────────────────────────────────
    @staticmethod
    def extract(text: str) -> Tuple[bool, float, List[Person]]:
        """兼容旧接口：只返回确定的人员"""
        certain, _ = RuleExtractor.extract_fulltext(text)
        conf = RuleExtractor._calc_confidence(certain)
        return len(certain) > 0, conf, certain

    @staticmethod
    def _calc_confidence(persons: List[Person]) -> float:
        if not persons:
            return 0.0
        score = 0.5
        roles_found = {p.role for p in persons}
        if roles_found & {'书记员', '代书记员', '代理书记员'}:
            score += 0.15
        if roles_found & {'审判长', '代理审判长', '首席仲裁员', '主审法官', '院长', '副院长', '庭长', '副庭长', '主任', '副主任'}:
            score += 0.15
        if roles_found & {'审判员', '代理审判员', '助理审判员', '仲裁员', '执行法官', '执行员', '法官'}:
            score += 0.1
        if roles_found & {'人民陪审员', '合议庭成员', '陪审员', '法警'}:
            score += 0.1
        if len(roles_found) >= 2:
            score += 0.1
        names = [p.name for p in persons]
        if len(names) != len(set(names)):
            score *= 0.8
        return round(min(1.0, score), 3)
