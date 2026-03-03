#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
阶段1.1终极版：预清洗 + 多模式向量化 + AC自动机双向扫描
核心优化：
1. 预清洗：去除括号、脱敏字符
2. 非捕获组处理"人民"误判
3. combine_first多模式优先匹配
4. 姓氏正向截断法
5. AC自动机双向扫描兜底
"""
import sys
import io
import pandas as pd
import re
from pathlib import Path
from tqdm import tqdm
import ahocorasick

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core.rule_extractor import COMMON_SURNAMES, COMPOUND_SURNAMES, INVALID_WORDS, INVALID_TAIL2

BASE = Path(__file__).parent
QUEUE_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_ai_queue.csv"
RESULT_PATH = BASE / "data" / "output" / "2016年01月裁判文书数据_result.csv"

# 黑名单：人民、法院等高频非姓名词
BLACKLIST = INVALID_WORDS | {'人民', '法院', '执行', '刑事', '民事', '审判', '陪审', '助理', '无', 'null', 'None', '印章', '署名', '简易程', '普通程', '特别程'}

# 边界字符（用于正则）
BOUNDARY_CHARS_STR = '速录员人民陪审判长代理书记附法律条文庭独二一三四五六七八九十零页共第本篇校对引评进行担任适用于之的回避参加议主中华国和以及查实勤丽清海时等签发翻译赔偿要永嘉胜蓝升注释宣布休复庭开公任理独民年月日'
BOUNDARY_PATTERN = f'[{BOUNDARY_CHARS_STR}]+'

# 无效后缀词（合并INVALID_TAIL2 + 额外的单字和特定后缀）
INVALID_SUFFIXES = INVALID_TAIL2 | {
    # 单字后缀
    '附', '注', '释', '开', '庭', '任', '理', '查', '审', '独', '民', '国', '和', '华',
    # 2字特定后缀（INVALID_TAIL2中没有的）
    '附注', '附相', '附本', '公开', '开庭', '宣布', '休庭', '复议', '独任', '任审', '任庭',
    '和国', '华人', '中华', '国民', '事诉', '诉讼',
    # 日期相关
    '年一', '月二', '日书', '一月', '二月', '三月', '四月', '五月', '六月',
    '七月', '八月', '九月', '十月', '十一', '十二', '二十', '三十',
    # 数字相关
    '第一', '第二', '第三', '第四', '第五', '第六', '第七', '第八', '第九',
    '百', '千', '万', '条', '款', '项', '章', '节', '编'
}

# 熔断字符：只要名字里包含这些字，直接从该字截断或丢弃
# 比如 "关法律" -> 遇到"法"，截断成"关"，然后因长度不足被丢弃
# 注意：不包含"三四五六七八九十"等可能出现在真实姓名中的字
STOP_CHARS = set('法条律规定附注搜微信息网站数据马克百度来源更多0123456789')

# 高危姓氏：虽然是姓氏，但在法律文书中常作为噪音出现的字
# 如果这些字作为姓氏，且后面跟的不像人名，则过滤
# 注意：不包含"关"（关羽）等常见真实姓名
RISKY_SURNAMES = {'年', '月', '日', '时', '本', '宣'}

# 角色正则映射表：解决 "代理书记员" vs "代书记员" 的模糊匹配问题
# 文书中常见缩写：代理→代、助理→助、人民陪审员→陪审员
ROLE_PATTERN_MAP = {
    '代理书记员': r'代(?:理)?书记员',  # 匹配"代书记员"或"代理书记员"
    '代理审判员': r'代(?:理)?审判员',  # 匹配"代审判员"或"代理审判员"
    '代理审判长': r'代(?:理)?审判长',  # 匹配"代审判长"或"代理审判长"
    '助理审判员': r'助(?:理)?审判员',  # 匹配"助审判员"或"助理审判员"
    '人民陪审员': r'(?:人民)?陪审员',  # 匹配"陪审员"或"人民陪审员"
}

print("=" * 80)
print("阶段1.1终极版：多策略融合提取")
print("=" * 80)

# 读取数据
queue_df = pd.read_csv(QUEUE_PATH, encoding='utf-8-sig', dtype=str)
result_df = pd.read_csv(RESULT_PATH, encoding='utf-8-sig', dtype=str)

print(f"\nai_queue总片段数: {len(queue_df):,}")

# 检测脱敏标记（全量检测，用于后续过滤）
queue_df['has_desensitization'] = queue_df['片段'].str.contains(r'[ＸX]{2,}', regex=True, na=False)

# 确保 AI提取姓名 列存在（phase1 的新鲜输出可能没有此列）
if 'AI提取姓名' not in queue_df.columns:
    queue_df['AI提取姓名'] = ''

# 筛选待处理
pending_mask = (queue_df['AI提取姓名'] == '(无)') | (queue_df['AI提取姓名'] == '') | queue_df['AI提取姓名'].isna()
work_df = queue_df[pending_mask].copy()

print(f"待处理片段数: {len(work_df):,}")

if len(work_df) == 0:
    print("没有待处理数据。")
    sys.exit(0)

# ============================================================
# 步骤1: 预清洗 (Pre-cleaning)
# ============================================================
print("\n步骤1: 预清洗文本...")

# 去除括号内容
work_df['片段_clean'] = work_df['片段'].str.replace(r'[\(（].*?[\)）]', '', regex=True)
# 去除脱敏字符（用于提取，但会检查原始片段）
work_df['片段_clean'] = work_df['片段_clean'].str.replace(r'[ＸX\?？]', '', regex=True)

# ============================================================
# 步骤2: 多模式向量化提取
# ============================================================
print("步骤2: 多模式向量化提取...")

def get_role_pattern(role_name):
    """
    获取角色的正则模式，支持模糊匹配
    处理常见缩写：代理→代、助理→助、人民陪审员→陪审员
    """
    # 优先使用映射表中的模糊匹配模式
    if role_name in ROLE_PATTERN_MAP:
        return ROLE_PATTERN_MAP[role_name]
    
    # 兜底：处理"人民"前缀的非捕获组
    if '陪审' in role_name:
        return r'(?:人民)?' + re.escape(role_name)
    
    # 默认：精确匹配
    return re.escape(role_name)

# 存储提取结果
final_candidates = pd.Series(index=work_df.index, dtype=str)

# 按角色分组处理
for role in tqdm(work_df['角色'].unique(), desc="按角色提取"):
    if pd.isna(role):
        continue
    
    mask = work_df['角色'] == role
    texts = work_df.loc[mask, '片段_clean']
    
    r_pat = get_role_pattern(role)
    
    # 模式A: "某某某担任..." (最高优先级)
    pat_a = r'(?<!由)([\u4e00-\u9fa5]{2,4})担任' + r_pat
    res_a = texts.str.extract(pat_a, expand=False)
    
    # 模式B: "由...担任"
    pat_b = r'由' + r_pat + r'([\u4e00-\u9fa5]{2,4})担任'
    res_b = texts.str.extract(pat_b, expand=False)
    
    # 模式C: 姓名中间有空格 "刘 红"
    pat_c = r_pat + r'\s+([\u4e00-\u9fa5]\s+[\u4e00-\u9fa5]{1,2})'
    res_c = texts.str.extract(pat_c, expand=False).str.replace(r'\s+', '', regex=True)
    
    # 模式D: 标准 "角色 姓名"
    pat_d = r_pat + r'\s*([^\s\u3000]+)'
    res_d = texts.str.extract(pat_d, expand=False)
    
    # 优先级合并: A > B > C > D
    combined = res_a.combine_first(res_b).combine_first(res_c).combine_first(res_d)
    
    # 立即清洗：去除边界字符和无效后缀
    def quick_clean(name):
        if pd.isna(name) or not name:
            return None
        name = str(name).strip()
        # 去除边界字符
        name = re.sub(f'^{BOUNDARY_PATTERN}', '', name)
        name = re.sub(f'{BOUNDARY_PATTERN}$', '', name)
        # 去除无效后缀（多次迭代）
        for _ in range(3):  # 最多3次
            for suffix in sorted(INVALID_SUFFIXES, key=len, reverse=True):
                if name.endswith(suffix):
                    name = name[:-len(suffix)]
                    break
        return name if len(name) >= 2 else None
    
    combined = combined.apply(quick_clean)
    
    final_candidates.loc[mask] = combined

# ============================================================
# 步骤3: 姓氏正向截断法清洗
# ============================================================
print("步骤3: 姓氏正向截断清洗...")

def positive_truncate_v2(name):
    """
    铁血版清洗：基于姓氏正向截断 + 敏感词熔断
    核心改进：
    1. STOP_CHARS熔断机制 - 遇到禁止字符立即截断
    2. RISKY_SURNAMES过滤 - 高危姓氏需额外验证
    3. 更严格的长度控制 - 单姓最多3字，复姓最多4字
    """
    if pd.isna(name) or not name:
        return None
    
    name = str(name).strip()
    
    # 1. 熔断机制：遇到禁止字符，直接丢弃其后的所有内容
    # 例如："关法律" -> 遇到"法"，截断为"关" -> 长度不够丢弃
    for i, char in enumerate(name):
        if char in STOP_CHARS:
            name = name[:i]
            break
    
    # 截断后再次去除空白
    name = name.strip()
    
    # 2. 长度硬性检查（绝大多数名字是2-3字，极少4字）
    if len(name) < 2 or len(name) > 4:
        return None
    
    # 3. 黑名单全匹配过滤
    if name in BLACKLIST:
        return None
    
    # 4. 纯汉字校验（允许·用于少数民族姓名）
    if not re.match(r'^[\u4e00-\u9fa5·]+$', name):
        return None
    
    # 5. 拒绝包含虚词、动词、方位词等明显非人名字符
    invalid_chars = set('的地得与和及后前中上下左右处理执行宣布开庭公开有同等权利义务条款项')
    if any(c in invalid_chars for c in name):
        return None
    
    # 5.5. 特殊模式过滤：法律程序相关词汇和法律条文
    invalid_patterns = ['简易程', '普通程', '特别程', '程序', '序由', '序公', '序独', '第一', '第二', '第三', '第四', '第五', '第六', '第七', '第八', '第九', '担任', '适用', '执行']
    if any(pattern in name for pattern in invalid_patterns):
        return None
    
    # 5.6. 拒绝少数民族姓名的错误截断（如果包含·但被截断了，说明提取错误）
    # 少数民族姓名应该保留完整的·分隔格式
    if len(name) <= 3 and not ('·' in name):
        # 检查是否可能是少数民族姓名的一部分（常见维吾尔族、哈萨克族等姓名特征）
        minority_chars = {'尔', '提', '木', '买', '热', '古', '力', '汗', '江', '肯', '巴', '克', '斯', '布', '德'}
        if name[-1] in minority_chars and len(name) <= 3:
            # 可能是少数民族姓名被错误截断，拒绝
            return None
    
    first_char = name[0]
    first_two = name[:2] if len(name) >= 2 else ''
    
    # 5. 高危姓氏特殊检查
    # 如果姓是"年"、"月"、"日"等，且名字不像人名，杀掉
    if first_char in RISKY_SURNAMES:
        # 如果名字里包含"书"、"记"、"员"、"注"、"文"等字，大概率是误提取
        if any(c in name for c in ['书', '记', '员', '注', '文', '院', '庭', '理']):
            return None
        # 如果是2字名且第二字也在高危列表，杀掉（如"年一"、"月二"）
        if len(name) == 2 and name[1] in RISKY_SURNAMES:
            return None
        # 特殊处理："年"开头的名字，如果第二字是数字（一二三四五六七八九十），大概率是日期
        if first_char == '年' and len(name) >= 2:
            if name[1] in '一二三四五六七八九十':
                return None
    
    # 6. 少数民族姓名（带·）
    if '·' in name and len(name) >= 4:
        return name
    
    # 7. 正规姓氏校验
    is_compound = first_two in COMPOUND_SURNAMES
    is_common = first_char in COMMON_SURNAMES
    
    if is_compound:
        # 复姓最多取4字（复姓2字+名2字）
        return name if len(name) <= 4 else name[:4]
    elif is_common:
        # 单姓通常2-3字。如果是4字，极大概率是提取错误
        return name if len(name) <= 3 else name[:3]
    
    return None

# 向量化应用清洗函数
cleaned = final_candidates.apply(positive_truncate_v2)
valid_results_pass1 = cleaned.dropna()

print(f"模式提取+正向截断得到: {len(valid_results_pass1):,} 条")

# ============================================================
# 步骤4: AC自动机双向扫描兜底
# ============================================================
print("步骤4: AC自动机双向扫描兜底...")

# 构建AC自动机
automaton = ahocorasick.Automaton()
for surname in COMMON_SURNAMES:
    automaton.add_word(surname, surname)
for surname in COMPOUND_SURNAMES:
    automaton.add_word(surname, surname)
automaton.make_automaton()

# 角色词列表
ROLE_KEYWORDS_LIST = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员', '陪审员']

def ac_bidirectional_scan(row):
    """AC自动机双向扫描"""
    text = row['片段_clean']
    role = row['角色']
    
    if pd.isna(text) or pd.isna(role):
        return None
    
    # 找到角色词位置
    role_pos = text.find(role)
    if role_pos == -1:
        return None
    
    # 找到所有姓氏位置
    surname_positions = []
    for end_index, surname in automaton.iter(text):
        start_index = end_index - len(surname) + 1
        surname_positions.append((start_index, end_index, surname))
    
    if not surname_positions:
        return None
    
    # 找距离角色词最近的姓氏（距离<10）
    min_distance = 10
    best_match = None
    
    for start_idx, end_idx, surname in surname_positions:
        distance = abs(start_idx - (role_pos + len(role)))
        
        if distance < min_distance:
            # 提取姓氏后2-3个字作为候选姓名
            if surname in COMPOUND_SURNAMES:
                candidate = text[start_idx:start_idx+4] if start_idx+4 <= len(text) else text[start_idx:start_idx+3]
            else:
                candidate = text[start_idx:start_idx+3] if start_idx+3 <= len(text) else text[start_idx:start_idx+2]
            
            # 验证候选姓名
            if len(candidate) >= 2 and re.match(r'^[\u4e00-\u9fa5]+$', candidate):
                # 黑名单过滤
                if candidate in BLACKLIST:
                    continue
                
                # 检查是否包含"人民"等无效词
                has_invalid = False
                for invalid_word in ['人民', '法院', '审判', '陪审', '书记']:
                    if invalid_word in candidate:
                        has_invalid = True
                        break
                if has_invalid:
                    continue
                
                # 检查第二个字是否是常见的非姓名字符
                # 这些字符通常不会出现在真实姓名中
                if len(candidate) >= 2:
                    second_char = candidate[1]
                    invalid_second_chars = {'本', '时', '条', '关', '请', '国', '布', '群'}
                    if second_char in invalid_second_chars:
                        continue
                
                # 应用铁血版清洗（包含STOP_CHARS熔断和RISKY_SURNAMES过滤）
                cleaned = positive_truncate_v2(candidate)
                
                # 如果清洗后为None，说明是脏数据，跳过
                if cleaned is None:
                    continue
                
                min_distance = distance
                best_match = cleaned
    
    return best_match

# 只对第一轮未提取到的数据进行AC扫描
remaining_mask = ~work_df.index.isin(valid_results_pass1.index)
remaining_df = work_df[remaining_mask].copy()

# 过滤掉有脱敏标记的片段（这些无法准确提取）
remaining_df = remaining_df[~remaining_df['has_desensitization']].copy()

print(f"剩余待扫描: {len(remaining_df):,} 条")

if len(remaining_df) > 0:
    ac_results = []
    for idx, row in tqdm(remaining_df.iterrows(), total=len(remaining_df), desc="AC扫描"):
        result = ac_bidirectional_scan(row)
        if result:
            ac_results.append({'index': idx, 'name': result})
    
    if ac_results:
        ac_df = pd.DataFrame(ac_results).set_index('index')
        valid_results_pass2 = ac_df['name']
        print(f"AC扫描新增: {len(valid_results_pass2):,} 条")
    else:
        valid_results_pass2 = pd.Series(dtype=str)
else:
    valid_results_pass2 = pd.Series(dtype=str)

# 合并两轮结果
valid_results = pd.concat([valid_results_pass1, valid_results_pass2])

print(f"\n总提取: {len(valid_results):,} 条")

# ============================================================
# 步骤4.5: 全局"回炉重造"（修复历史脏数据）
# ============================================================
print("\n步骤4.5: 对全量数据进行二次清洗（修复历史脏数据）...")

# 无论之前是否处理过，对所有已有结果再次应用最新的严格清洗逻辑
# 这一步能把 "关法律"、"年一月" 这种漏网之鱼洗掉
def reprocess_existing(val):
    """对已提取的数据重新应用铁血版清洗"""
    if pd.isna(val) or val == '' or val == '(无)':
        return val
    cleaned = positive_truncate_v2(val)
    return cleaned if cleaned else '(无)'  # 如果清洗后变为None，说明是脏数据，标记为(无)

# 对 queue_df 的全量数据清洗
mask_has_data = queue_df['AI提取姓名'].notna() & (queue_df['AI提取姓名'] != '') & (queue_df['AI提取姓名'] != '(无)')
original_count = mask_has_data.sum()

if original_count > 0:
    print(f"  发现 {original_count:,} 条已提取数据，开始二次清洗...")
    queue_df.loc[mask_has_data, 'AI提取姓名'] = queue_df.loc[mask_has_data, 'AI提取姓名'].apply(reprocess_existing)
    
    # 统计清洗后变为(无)的数量
    cleaned_to_invalid = (queue_df['AI提取姓名'] == '(无)').sum()
    if cleaned_to_invalid > 0:
        print(f"  ✓ 清除了 {cleaned_to_invalid} 条脏数据（标记为'(无)'）")
        # 将这些行的状态改回待处理
        queue_df.loc[queue_df['AI提取姓名'] == '(无)', '状态'] = '待处理'
        queue_df.loc[queue_df['AI提取姓名'] == '(无)', 'AI提取姓名'] = ''
    else:
        print(f"  ✓ 未发现脏数据")

print("全局清洗完成。")

# ============================================================
# 步骤5: 批量更新
# ============================================================
if len(valid_results) > 0:
    print("\n步骤5: 批量更新数据 (优化版)...")
    
    # 1. 更新Queue状态
    queue_df.loc[valid_results.index, 'AI提取姓名'] = valid_results
    queue_df.loc[valid_results.index, '状态'] = '已处理'
    
    # 2. 准备更新数据
    updates = pd.DataFrame({
        'row_num': work_df.loc[valid_results.index, 'snippet_id'].str.split('_').str[0].astype(int) + 1,
        'role': work_df.loc[valid_results.index, '角色'],
        'name': valid_results
    })
    
    # 统一类型：确保 row_num 是字符串，方便后续作为索引匹配
    updates['row_num'] = updates['row_num'].astype(str)
    
    # 去重：同一行同一角色保留第一个
    updates = updates.drop_duplicates(subset=['row_num', 'role'], keep='first')
    
    # 3. 将 result_df 设为索引，准备更新
    # 必须确保 result_df 的序号也是字符串类型，以便匹配
    result_df['序号'] = result_df['序号'].astype(str)
    result_indexed = result_df.set_index('序号')
    
    update_count = 0
    
    # --- 优化点1：按角色批量更新 (利用索引对齐，避免全表扫描) ---
    for role in tqdm(updates['role'].unique(), desc="更新Result表"):
        # 当前角色需要更新的数据：Series(index=row_num, value=name)
        role_updates = updates[updates['role'] == role].set_index('row_num')['name']
        
        # 找到需要更新的行（取交集）
        common_indices = result_indexed.index.intersection(role_updates.index)
        
        if len(common_indices) > 0:
            # 获取当前值
            current_values = result_indexed.loc[common_indices, role]
            
            # 仅当当前值为空时才更新
            empty_mask = (current_values.isna()) | (current_values == '') | (current_values == 'nan')
            target_indices = common_indices[empty_mask]
            
            if len(target_indices) > 0:
                result_indexed.loc[target_indices, role] = role_updates.loc[target_indices]
                result_indexed.loc[target_indices, '来源'] = '二轮规则'
                update_count += len(target_indices)
    
    print(f"Result表更新: {update_count:,} 行")

    # --- 优化点2：Flag 清理 (移除循环内的全表扫描) ---
    print("清理flag：移除已处理角色...")
    
    # 1. 聚合每个 row_num 更新了哪些角色
    # 结果格式: Series(index=row_num, value={role1, role2})
    row_updated_roles_series = updates.groupby('row_num')['role'].apply(set)
    
    # 2. 仅提取 flag 不为空 且 在更新列表中的行
    # 找到 result_indexed 中需要处理 flag 的行索引
    flag_mask = result_indexed['flag'].notna() & (result_indexed['flag'] != '')
    # 只处理 flag 不为空 且 确实有数据更新 的行
    target_flag_indices = result_indexed.index[flag_mask].intersection(row_updated_roles_series.index)
    
    clean_ai_count = 0
    partial_clean_count = 0
    
    if len(target_flag_indices) > 0:
        # 提取需要处理的 flag 子集
        flags_to_process = result_indexed.loc[target_flag_indices, 'flag']
        roles_processed = row_updated_roles_series.loc[target_flag_indices]
        
        # 定义单行清理函数
        def clean_single_flag(flag_val, processed_set):
            if not isinstance(flag_val, str) or '需要AI处理:' not in flag_val:
                return flag_val
            
            try:
                parts = flag_val.split('需要AI处理:')
                prefix = parts[0]
                ai_roles_str = parts[1]
                
                # 当前 flag 中记录的待处理角色
                current_ai_roles = [r.strip() for r in ai_roles_str.split(',') if r.strip()]
                
                # 移除本次已提取的角色
                remaining_roles = [r for r in current_ai_roles if r not in processed_set]
                
                if not remaining_roles:
                    return '' # 全部处理完了
                else:
                    return f"{prefix}需要AI处理: {', '.join(remaining_roles)}"
            except:
                return flag_val

        # 3. 向量化应用清理逻辑 (List Comprehension 比 apply 更快)
        new_flags = [
            clean_single_flag(flag, roles) 
            for flag, roles in zip(flags_to_process, roles_processed)
        ]
        
        # 4. 批量回写
        result_indexed.loc[target_flag_indices, 'flag'] = new_flags
        
        # 简单统计
        new_flags_series = pd.Series(new_flags)
        clean_ai_count = (new_flags_series == '').sum()
        partial_clean_count = (new_flags_series != '').sum()

    if clean_ai_count > 0:
        print(f"  完全清理 {clean_ai_count:,} 行的flag")
    if partial_clean_count > 0:
        print(f"  部分清理 {partial_clean_count:,} 行的flag")

    # 4. 恢复索引并保存
    result_df = result_indexed.reset_index()
    # 恢复序号为 int (如果原本是int的话，可选)
    # result_df['序号'] = pd.to_numeric(result_df['序号'], errors='ignore')
    
    result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig', na_rep='')

# 统计（在清理前）
total_success = len(queue_df[queue_df['状态'].str.contains('已处理', na=False)])
remaining = len(queue_df[(queue_df['AI提取姓名'] == '(无)') | (queue_df['AI提取姓名'] == '') | queue_df['AI提取姓名'].isna()])

# 标记脱敏片段为N/A（无法提取）
desensitized_mask = queue_df['has_desensitization'].fillna(False) & ((queue_df['AI提取姓名'] == '') | queue_df['AI提取姓名'].isna())
if desensitized_mask.sum() > 0:
    print(f"\n标记脱敏片段为N/A: {desensitized_mask.sum():,} 条")
    queue_df.loc[desensitized_mask, 'AI提取姓名'] = 'N/A'
    queue_df.loc[desensitized_mask, '状态'] = 'N/A(脱敏)'

# ============================================================
# 步骤6: 修复result.csv中的N/A记录/空记录flag
# ============================================================
print(f"\n步骤6: 修复result.csv中的空记录...")

# 角色列
if 'ROLES' in dir():
    role_columns = ROLES
else:
    role_columns = ['审判长', '审判员', '代理审判长', '代理审判员', '书记员', '代理书记员', '助理审判员']

# 找出所有角色列都为空且flag非'需要AI处理'的记录
empty_roles = (result_df[role_columns].isna() | (result_df[role_columns] == '')).all(axis=1)
no_ai_flag = ~result_df['flag'].str.contains('需要AI处理', na=False)
empty_records = empty_roles & no_ai_flag

# 1. 之前逻辑：需要设置来源为空的（保留）
need_source_na = empty_records & result_df['来源'].notna() & (result_df['来源'] != '')
if need_source_na.sum() > 0:
    print(f"  修改 {need_source_na.sum():,} 条空记录的来源为空")
    result_df.loc[need_source_na, '来源'] = ''

# 2. 新需求：如果记录为空，确保 flag 设置为 'NA'
need_flag_na = empty_records & (result_df['flag'] != 'NA')
if need_flag_na.sum() > 0:
    print(f"  修改 {need_flag_na.sum():,} 条空记录的 flag 为 'NA'")
    result_df.loc[need_flag_na, 'flag'] = 'NA'

# 保存result（无论是否有更新，都保存以确保数据一致性）
result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig', na_rep='')

# ============================================================
# 步骤6.5: 同步清理 result.csv 中的 flag（修复数据不一致 bug）
# ============================================================
print(f"\n步骤6.5: 同步清理result.csv中的flag（与ai_queue保持一致）...")

# 找出即将被清理的条目
to_remove_mask = queue_df['状态'].isin(['已处理', '已处理(终极版)', 'N/A(脱敏)'])
to_remove_df = queue_df[to_remove_mask]

if len(to_remove_df) > 0:
    # 按序号聚合即将被清理的角色
    removed_roles_by_serial = to_remove_df.groupby('序号')['角色'].apply(set)
    
    # 确保 result_df 的序号是字符串类型
    result_df['序号'] = result_df['序号'].astype(str)
    
    # 只处理 flag 含 '需要AI处理:' 且序号在清理列表中的行（向量化筛选）
    has_ai_flag = result_df['flag'].str.contains('需要AI处理:', na=False)
    in_remove_list = result_df['序号'].isin(removed_roles_by_serial.index)
    target_mask = has_ai_flag & in_remove_list
    
    if target_mask.sum() > 0:
        # 建立序号→角色集合的字典（快速查找）
        roles_dict = removed_roles_by_serial.to_dict()
        
        # 对目标行提取 flag 和序号
        target_flags = result_df.loc[target_mask, 'flag']
        target_serials = result_df.loc[target_mask, '序号']
        
        def clean_flag(flag_val, serial):
            try:
                removed_set = roles_dict.get(serial, set())
                if not removed_set:
                    return flag_val
                parts = flag_val.split('需要AI处理:')
                ai_roles_str = parts[1] if len(parts) > 1 else ''
                current_roles = [r.strip() for r in ai_roles_str.split(',') if r.strip()]
                remaining = [r for r in current_roles if r not in removed_set]
                return '' if not remaining else f"需要AI处理: {', '.join(remaining)}"
            except Exception:
                return flag_val
        
        # 向量化应用（仅对目标子集）
        new_flags = [clean_flag(f, s) for f, s in zip(target_flags, target_serials)]
        result_df.loc[target_mask, 'flag'] = new_flags
        
        flag_cleaned_count = sum(1 for f in new_flags if f == '')
        flag_partial_count = sum(1 for f in new_flags if f != '' and f != target_flags.iloc[0])
        
        print(f"  完全清理flag: {flag_cleaned_count:,} 行")
        if flag_partial_count > 0:
            print(f"  部分清理flag: {flag_partial_count:,} 行")
    else:
        print(f"  无需清理")
    
    # 保存更新后的 result
    result_df.to_csv(RESULT_PATH, index=False, encoding='utf-8-sig', na_rep='')

# 清理已处理的条目（已进入result.csv，无需保留在queue中）
print(f"\n清理ai_queue: 移除已处理和N/A条目...")
before_count = len(queue_df)
queue_df = queue_df[~queue_df['状态'].isin(['已处理', '已处理(终极版)', 'N/A(脱敏)'])].copy()
removed_count = before_count - len(queue_df)
print(f"  移除 {removed_count:,} 条已处理/N/A条目")
print(f"  保留 {len(queue_df):,} 条待处理条目")

# 保存清理后的queue
queue_df.to_csv(QUEUE_PATH, index=False, encoding='utf-8-sig')

print(f"\n{'='*80}")
print(f"阶段1.1完成（终极版）")
print(f"{'='*80}\n")

print(f"原始片段总数: {before_count:,} 条")
print(f"总提取成功: {total_success:,} 条")
print(f"本次新增: {len(valid_results):,} 条")
print(f"已移入result.csv: {removed_count:,} 条")
print(f"队列剩余待处理: {len(queue_df):,} 条")
print(f"总成功率: {total_success/before_count*100:.1f}%")

print(f"\n优化策略:")
print(f"  ✓ 预清洗（去除括号、脱敏字符）")
print(f"  ✓ 非捕获组处理'人民'误判")
print(f"  ✓ 多模式优先匹配（担任>由...担任>空格>标准）")
print(f"  ✓ 姓氏正向截断法")
print(f"  ✓ AC自动机双向扫描兜底")

print(f"\n文件已更新:")
print(f"  - {RESULT_PATH}")
print(f"  - {QUEUE_PATH}")
