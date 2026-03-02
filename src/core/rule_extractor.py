"""
规则提取器 v4.0 (纯结构物理切片法)
抛弃自然语言分词(Jieba)猜测词性的不确定性，转而利用法律文书严格的排版坐标，
通过【角色锚点->长度限制前缀->清洁->百家姓验证】进行提取，彻底解决错字、漏字、加字问题。
"""
import re
import json
from pathlib import Path
from typing import List, Tuple
import sys

try:
    from ..models import Person
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from models import Person


# ── 水印与附录拦截词（直接斩断长文本）──────────────────
_APPENDIX_WORDS = ['本案裁决所依据',
                   '百度搜索', '马克数据', 'macrodat', '微信公众',
                   'www.', '更多数据', '关注公众号']
_STOP_WORDS = [
    '来源', '来自', '关注', '微信', '马克', '更多', '数据', '附法', '提供', '查询', '提示',
    '校对', '说明', '打印', '此件', '扫描', '复印', '公众号', '附件', '附一', '附二',
    '附三', '附：', '附言', '附带', '附注', '附本', '法律', '担任', '速录', '宣布', '休庭', '特别',
    '授权', '联系', '电话', '地址', '条文', '适用', '相关', '正当', '行使', '二〇', '二0', '二O',
    '一月', '二月', '三月', '四月', '五月', '六月', '七月', '八月', '九月', '十月', '十一月', '十二月',
    '【', '[', '(', '（', '搜索', '搜到', '搜出',
    '提交', '申请', '上诉', '被诉', '原告', '被告', '本院', '本案',
    '查明', '查清', '事实', '裁定', '判决', '受理', '人民法院', '知识产权',
    '分别', '根据', '依照', '执行', '复议', '上述', '以上', '以下', '由于', '经审查', '经审理', '经查', '指出',
    '审理', '审查', '代理', '代书', '代审', '提醒', '提出', '市区', '镇党',
    '危害', '见习', '实习', '上列', '下列', '如下', '须知', '以下',
    '经办人', '附录', '附页', '相关法律',
]

# ── 年份停止正则（如「二〇一六」「二０一六」「二O一六」等）────────
_YEAR_RE = re.compile(r'(?:一九|二[〇０0OＯΟО○][一二三四五六七八九十\d]{2}\s*年|20[0-9]{2}\s*年)')

# ── 无效词黑名单（用于排除碰巧就在法官后面的2字动名词，防止被误认为名字）──
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
    # v4.1补充：避免“法官都有一颗公正的心”等句子碎片被误提
    '公正', '都有', '有一', '希望', '应当', '必须', '应该', '能够',
    '一颗', '之心', '心态', '态度', '精神', '品德', '信念',
    '做到', '实现', '保障', '保护', '维护', '服务',
    '都有一', '都希望', '都应当', '都必须', '都能够', '都有一颗',
    
    # v4.2补充：其它常见动名词汇（首字在百家姓中容易被误提）
    '劳动', '需要', '进行', '要求', '同意', '听取', '责令', '征收',
    '其它', '其它事', '其他', '提出申请', '依法审', '依法作',
})

# ── 尾字粘连黑名单（提取的名字末字若是这些字→裁剪掉）─────────────
# 极度精简，避免误伤（“法”不能放→“巴鑫法”，“本”不能放→“卢成本”，“代”不能放→“杨代”）
_BAD_TAIL_CHARS = frozenset(
    '附另打搜及即亦注申引等党兼'
)

# ── 首字黑名单 ─────────────────────────────────────────
_DIGIT_CHARS = frozenset('一二三四五六七八九十零〇')


# ── 绝版硬核：百家姓与少数民族首字字典 ───────────────────
# 囊括绝大数常见姓氏，防止截取到 "支持"、"通过" 等2字动词
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
    # 以下为10万条实测中发现的百家姓缺失，按批次补充
    "冉谭拜朝介向圣吕希那达益央旺扎尼普格桑仁增"
    "覃铁付樊宝图迪练柴"
    # 第3批：2205条exceptions全量分析补充
    "廖聂曲边牛綦楼庄过兰劳耿栗易苑农匡桂苟谌邸才"
    "阎逯岩弋侍虎郄鞠初冷惠简戈盘莎尚寿湛辛晁沃敖饶"
    "刀信税豆甲都瓦来相"
    # 第4批：全量审计补充
    "牟岳商文延古吐涂仉闻官佟闫亢漆次"
    # 少数民族常见首字
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
    """基于纯物理切片法 (Strict Span) 的终端规则提取器"""

    @staticmethod
    def extract(signature_text: str) -> Tuple[bool, float, List[Person]]:
        """提取落款人员。Returns: (是否提取到人员, 置信度, 人员列表)"""
        persons: List[Person] = []
        seen: set = set()

        # 1. 第一层清理：去除全角空格，整理常见排版
        text = RuleExtractor._normalize(signature_text)

        # 2. 第二层清理：强力截肢附录长文（如果出现相关法律等词，直接丢掉后面的所有内容）
        earliest_appendix = len(text)
        for appendix in _APPENDIX_WORDS:
            idx = text.find(appendix)
            if 0 <= idx < earliest_appendix:
                earliest_appendix = idx
        text = text[:earliest_appendix]

        # 3. 定位所有物理坐标锚点（角色 和 日期）
        markers = []
        for r in ROLES:
            for m in re.finditer(r, text):
                markers.append({'val': r, 'start': m.start(), 'end': m.end()})
                
        for m in _YEAR_RE.finditer(text):
            markers.append({'val': m.group(), 'start': m.start(), 'end': m.end()})

        # 【关键】去重叠：当长短角色在同一位置重叠时（如"人民陪审员"与"陪审员"），
        # 只保留更长的那个，避免名字被错误分配到短角色
        markers.sort(key=lambda x: (x['start'], -(x['end'] - x['start'])))
        deduped = []
        for mk in markers:
            # 如果与上一个已保留的 marker 有重叠（即当前 start < 上一个 end），跳过
            if deduped and mk['start'] < deduped[-1]['end']:
                continue
            deduped.append(mk)
        markers = deduped

        # 4. 纯结构物理切片提取
        for i, curr_marker in enumerate(markers):
            if curr_marker['val'] not in ROLES_SET:
                continue  # 如果当前锚点是日期，不能作为起始点，跳过
                
            role = curr_marker['val']
            start_pos = curr_marker['end']
            # 下一个锚点的开头，或者文章结尾
            end_pos = markers[i+1]['start'] if i+1 < len(markers) else len(text)
            
            raw_segment = text[start_pos:end_pos].strip()
            
            # 【核心护城河】：限制前缀距离（截取角色后30字符，容纳含空格的名字）
            search_area = raw_segment[:30].strip()
            
            # 清除乱码字符（全角字母如ＸＸ、问号?、特殊字符等）
            search_area = re.sub(r'[\uff01-\uff5e]', '', search_area)  # 全角ASCII
            search_area = re.sub(r'[?？\ufffd]+', '', search_area)
            
            # 【激进合并】：彻底合并汉字间的所有空格
            # 先把连续空格替换为单空格，再循环合并汉字间单空格
            search_area = re.sub(r'\s+', ' ', search_area).strip()
            for _ in range(5):
                new = re.sub(r'([\u4e00-\u9fa5]) ([\u4e00-\u9fa5])', r'\1\2', search_area)
                if new == search_area:
                    break
                search_area = new
            # 合并后限制字符数（多人名在合并后紧凑排列）
            search_area = search_area[:16]
            
            # 【全新围栏】：拦截一切可能是水印残段的截断词
            earliest_stop = len(search_area)
            for sw in _STOP_WORDS:
                idx = search_area.find(sw)
                if 0 <= idx < earliest_stop:
                    earliest_stop = idx
            search_area = search_area[:earliest_stop]
            
            # 去除冒号和常见标点，将标点转化为空格打断名字
            search_area = re.sub(r'^[：:]+', ' ', search_area)
            cleaned_area = re.sub(r'[、，。；：\.,;:（）\(\)《》<>\n\r]', ' ', search_area).strip()
            if not cleaned_area:
                continue

            # 按空格切分成多个候选组（处理“审判员 张三 李四”的情况）
            candidates = cleaned_area.split()
            
            for cand in candidates:
                # 剔除尾部粘连的错字（附、二、另 等等）
                while len(cand) > 2 and cand[-1] in _BAD_TAIL_CHARS:
                    cand = cand[:-1]
                    
                if len(cand) > 4:
                    # 如果候选词依然很长（超过4个字），说明是没有空格的粘连词（如 张亚军因为本案...）
                    # 我们尝试从开头截取 2-4 个符合规则的字作为名字，优先取3个字
                    extracted = False
                    for l in [3, 2, 4]:
                        sub_cand = cand[:l]
                        if RuleExtractor._is_valid_name_v4(sub_cand):
                            key = (role, sub_cand)
                            if key not in seen:
                                seen.add(key)
                                persons.append(Person(name=sub_cand, role=role))
                            extracted = True
                            break
                    if extracted: continue
                            
                # 针对 4 字名的精准后置纠偏（替代不可靠的 jieba）
                # OCR导致姓名和随后的职位无空格粘连时，常形成"3字真名+1字职位前缀"
                # 如果这个候选词刚好 4 个字，且最后一个字是典型的职务前置字，且前3个字是合法名字，直接修剪
                if len(cand) == 4 and cand[-1] in '代本书法记长员兼一':
                    cand3 = cand[:3]
                    if RuleExtractor._is_valid_name_v4(cand3):
                        cand = cand3
                        
                # 4字名末尾是"主审""实习"等2字角色/词 → 截掉后2字
                if len(cand) == 4 and cand[2:] in ('主审', '注本', '主任', '实习'):
                    cand2 = cand[:2]
                    if RuleExtractor._is_valid_name_v4(cand2):
                        cand = cand2

                # 4字候选：如果不是复姓也不是少数民族名，尝试拆成两个2字名
                # 关键条件：两半的首字都必须在百家姓中，且不是少数民族字
                if len(cand) == 4 and '·' not in cand:
                    if (cand[:2] not in COMPOUND_SURNAMES
                            and cand[0] not in ETHNIC_CHARS
                            and cand[2] not in ETHNIC_CHARS):
                        n1, n2 = cand[:2], cand[2:]
                        if (n1[0] in COMMON_SURNAMES
                                and n2[0] in COMMON_SURNAMES
                                and RuleExtractor._is_valid_name_v4(n1)
                                and RuleExtractor._is_valid_name_v4(n2)):
                            for n in (n1, n2):
                                key = (role, n)
                                if key not in seen:
                                    seen.add(key)
                                    persons.append(Person(name=n, role=role))
                            continue

                # 【终极验证】：如果符合中国人的名字规则，则采纳，放入列表
                if RuleExtractor._is_valid_name_v4(cand):
                    key = (role, cand)
                    if key not in seen:
                        seen.add(key)
                        persons.append(Person(name=cand, role=role))
                        
        confidence = RuleExtractor._calc_confidence(persons)
        return len(persons) > 0, confidence, persons

    # ── 内部方法 ──────────────────────────────────────────────────

    @staticmethod
    def _normalize(text: str) -> str:
        """基础清洗"""
        # 统一全半角空格
        text = re.sub(r'[\u3000\xa0]', ' ', text)
        text = text.replace('〇', '〇')
        
        # 去除乱码字符（如 ���）
        text = re.sub(r'[\ufffd]+', ' ', text)
        
        # 不再全文合并汉字间空格（这会导致姓名粘连），
        # 只在角色关键词内部合并空格（如 "审 判 长" → "审判长"）
        for role in ROLES:
            # 生成带空格的变体匹配（如 "审\\s*判\\s*长"）
            spaced = r'\s*'.join(re.escape(c) for c in role)
            text = re.sub(spaced, role, text)
        
        # 压缩3个及以上连续空格为双空格（保留双空格作为名字分隔符）
        text = re.sub(r'[ \t]{3,}', '  ', text)
        return text

    @staticmethod
    def _is_valid_name_v4(name: str) -> bool:
        """v4.0物理切片的姓氏守门员机制：用字典白名单阻挡任何提取到的非人名两字组合(如"许可")"""
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

        # 增加首尾字符的脏字阻断（覆盖所有地理及机构残体后缀，剔除州防止误杀"李宗州"）
        if name[-1] in '搜附第页及即亦另打注申引结数内号字县市区省乡镇村庭厅局委办处股科':
            return False
        if name[0] in '第案本该其此各':
            return False
        
        # 拦截常见被误认为名字的地名（如温州等）
        if name in {'温州', '苏州', '广陵', '通州', '海州', '荆州', '上海', '北京', '天津', '重庆',
                    '宿州', '卓城', '广州', '深圳', '南京', '合肥', '长沙', '山东', '浙江', '云南',
                    '乐陵', '卫辉'}:
            return False
        
        # 拦截明显不是名字的组合（含“之”字的成语不作人名，比如“爱民之心”）
        if '之' in name and len(name) >= 3:
            return False
            
        # 拦截带有典型非人名前缀的3~4字组合（如“应当将”、“应当责”）
        # 因为“应”、“依”是百家姓，所以需要拦截这些特定的词开头
        if name.startswith(('应当', '依法', '同意', '要求', '必须', '需要', '已经', '进行', '可以')):
            return False
            
        # 拦截带有典型非人名后缀的组合
        if name.endswith(('责任', '听取', '责令', '征收')):
            return False

        # 少民·名或者藏维等常见开头字 -> 放宽通过
        if '·' in name or name[0] in ETHNIC_CHARS:
            return True
            
        # 标准汉字名，最大长度限制为4字
        if length > 4:
            return False
            
        # 复合姓（如欧阳、司马等）
        if length >= 3 and name[:2] in COMPOUND_SURNAMES:
            return True
            
        # 百家姓首字认证
        if name[0] in COMMON_SURNAMES:
            return True
            
        return False

    @staticmethod
    def _calc_confidence(persons: List[Person]) -> float:
        """评估置信度"""
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

