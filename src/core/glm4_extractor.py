#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GLM-4 AI提取器
使用GLM-4大模型批量提取裁判文书中的角色和姓名
按照cost_analysis.md方案：一次请求最多500条，返回JSON数组
"""
import json
import time
import re
import threading
from typing import List, Dict, Optional
from openai import OpenAI
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from .config import Config
except ImportError:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from core.config import Config


class GLM4Extractor:
    """使用GLM-4批量提取姓名和角色"""
    
    def __init__(self, config: Optional[Config] = None):
        if config is None:
            config = Config()
        
        api_cfg = config.get('api', {})
        self.base_url = api_cfg.get('base_url', 'https://vectorengine.ai/v1')
        self.api_key = api_cfg.get('api_key', '')
        self.model = api_cfg.get('model', 'glm-4')
        self.timeout = api_cfg.get('timeout', 120)
        self.max_retries = api_cfg.get('max_retries', 3)
        self.concurrency = api_cfg.get('concurrency', 10)
        
        # 批量处理配置
        processing_cfg = config.get('processing', {})
        self.batch_size = processing_cfg.get('ai_batch_size', 50)  # 测试验证：50条/批，ID匹配率100%
        
        # 日志写入锁（线程安全）
        self.log_lock = threading.Lock()
        
        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout
        )
    
    def extract_batch(self, snippets: List[Dict], log_file: Path = None, response_dir: Path = None) -> Dict[str, Dict]:
        """
        批量提取姓名（并发处理多个批次）
        
        Args:
            snippets: 片段列表 [{'id': str, 'role': str, 'text': str}, ...]
            log_file: 日志文件路径（可选）
            response_dir: 原始响应保存目录（可选）
        
        Returns:
            结果字典 {snippet_id: {'name': str, 'role': str}}
        """
        if not snippets:
            return {}
        
        result_map = {}
        total_batches = (len(snippets) + self.batch_size - 1) // self.batch_size
        
        print(f'  GLM-4批量处理: {len(snippets)} 条片段，分 {total_batches} 批，每批最多 {self.batch_size} 条')
        print(f'  并发数: {self.concurrency}')
        
        # 准备所有批次
        batches = []
        for batch_idx in range(0, len(snippets), self.batch_size):
            batch = snippets[batch_idx:batch_idx + self.batch_size]
            batches.append((batch_idx // self.batch_size, batch))
        
        # 使用线程池并发处理
        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            # 提交所有任务（传递log_file和response_dir参数）
            future_to_batch = {}
            for batch_num, batch in batches:
                # 每个批次的响应保存路径
                resp_path = None
                if response_dir:
                    response_dir.mkdir(parents=True, exist_ok=True)
                    resp_path = response_dir / f"batch_{batch_num:03d}_{int(time.time())}.json"
                
                future = executor.submit(self._process_batch_with_log, batch, log_file, resp_path)
                future_to_batch[future] = batch_num
            
            # 收集结果（带进度条）
            with tqdm(total=len(batches), desc='GLM-4提取', unit='批') as pbar:
                for future in as_completed(future_to_batch):
                    batch_results = future.result()
                    result_map.update(batch_results)
                    pbar.update(1)
        
        print(f'  GLM-4成功提取: {len(result_map)} 个姓名')
        return result_map
    
    def _process_batch_with_log(self, batch: List[Dict], log_file: Path = None, save_response: Path = None) -> Dict[str, Dict]:
        """
        处理单个批次并实时写入日志（线程安全）
        
        Args:
            batch: 一批片段，最多batch_size条
            log_file: 日志文件路径
            save_response: 原始响应保存路径
        
        Returns:
            本批次的提取结果
        """
        import datetime
        
        # 调用原始处理方法
        batch_results = self._process_batch(batch, save_response)
        
        # 实时写入日志（线程安全）
        if log_file:
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            with self.log_lock:  # 确保多线程写入安全
                with open(log_file, 'a', encoding='utf-8') as f:
                    f.write(f"\n[{timestamp}] 批次完成 - 处理 {len(batch)} 条，提取 {len(batch_results)} 个姓名\n")
                    f.write(f"{'-'*80}\n")
                    
                    # 记录每条结果
                    for item in batch:
                        sid = item['id']
                        role = item['role']
                        text = item['text'][:60] + '...' if len(item['text']) > 60 else item['text']
                        
                        if sid in batch_results:
                            name = batch_results[sid]['name']
                            f.write(f"[✓] {sid} | {role} | {name}\n")
                            f.write(f"    {text}\n")
                        else:
                            f.write(f"[✗] {sid} | {role} | (无)\n")
                            f.write(f"    {text}\n")
                    
                    f.write(f"{'-'*80}\n\n")
                    f.flush()  # 立即刷新到磁盘
        
        return batch_results
    
    def _process_batch(self, batch: List[Dict], save_response: Path = None) -> Dict[str, Dict]:
        """
        处理单个批次（一次API调用）
        
        Args:
            batch: 一批片段，最多batch_size条
            save_response: 保存原始响应的文件路径（可选）
        
        Returns:
            本批次的提取结果
        """
        import datetime
        
        # 构建输入JSON数组
        input_items = []
        for item in batch:
            input_items.append({
                'id': item['id'],
                'role': item['role'],
                'text': item['text']
            })
        
        # 构建prompt
        prompt = self._build_batch_prompt(input_items)
        
        # 调用API
        for retry in range(self.max_retries):
            try:
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {
                            "role": "system",
                            "content": "你是一个专业的裁判文书信息提取助手。你需要从简短文本片段中准确提取人名，并以JSON数组格式返回结果。"
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,  # 降低温度确保更高的确定性
                )
                
                result_text = response.choices[0].message.content.strip()
                
                # 保存原始响应（如果指定了保存路径）
                if save_response:
                    response_data = {
                        'timestamp': datetime.datetime.now().isoformat(),
                        'model': self.model,
                        'input_count': len(batch),
                        'input_items': input_items,
                        'raw_response': result_text,
                        'retry_count': retry
                    }
                    with open(save_response, 'w', encoding='utf-8') as f:
                        json.dump(response_data, f, ensure_ascii=False, indent=2)
                
                # 解析JSON结果
                return self._parse_batch_result(result_text, batch)
                
            except Exception as e:
                if retry == self.max_retries - 1:
                    print(f"\n  批次处理失败: {e}")
                    # 保存失败信息
                    if save_response:
                        error_data = {
                            'timestamp': datetime.datetime.now().isoformat(),
                            'model': self.model,
                            'input_count': len(batch),
                            'input_items': input_items,
                            'error': str(e),
                            'retry_count': retry
                        }
                        error_file = save_response.parent / f"{save_response.stem}_ERROR.json"
                        with open(error_file, 'w', encoding='utf-8') as f:
                            json.dump(error_data, f, ensure_ascii=False, indent=2)
                    return {}
                time.sleep(2 * (retry + 1))
        
        return {}
    
    def _build_batch_prompt(self, items: List[Dict]) -> str:
        """
        构建批量处理的prompt
        
        Args:
            items: [{'id': str, 'role': str, 'text': str}, ...]
        
        Returns:
            完整的prompt字符串
        """
        prompt_parts = [
            "请从以下裁判文书片段中提取对应角色的姓名。",
            "",
            "角色别名说明（这些是同一个角色的不同叫法）：",
            "- '代审判长' = '代理审判长'",
            "- '代审判员' = '代理审判员'",
            "- '代书记员' = '代理书记员'",
            "",
            "输入数据（JSON数组）：",
            json.dumps(items, ensure_ascii=False, indent=2),
            "",
            "提取规则：",
            "1. **每个id只能返回一个结果**，绝对不要为同一个id返回多条记录",
            "2. **只提取对应角色的姓名**，忽略片段中的其他角色（注意角色别名）",
            "3. 姓名长度：汉族姓名2-4个汉字，少数民族姓名可以更长（如5-8字）",
            "4. **少数民族姓名保留·分隔符**（如'古丽合尼木·尼牙孜'）",
            "5. 如果片段中有多个姓名，**必须提取紧跟在角色词后的第一个完整姓名**",
            "6. **去除姓名中的所有空格**（如'张 紫 君'提取为'张紫君'，'徐 一'提取为'徐一'）",
            "7. **姓名后可能紧跟其他字符**（如'欧汉庭附'提取'欧汉庭'，'李月菊二'提取'李月菊'）",
            "8. **完全忽略其他角色词和水印文字**（如'马 克 数 据 网'是水印，不是姓名）",
            "9. 如果片段是纯法律条文或程序性描述（如'审判员移送执行员'、'书记员署名'），name字段设为null",
            "10. 严格按照以下JSON数组格式返回，不要添加任何其他文字",

            "",
            "示例：",
            "- '书记员 赖云清李韵虹' → 提取'赖云清'（第一个姓名）",
            "- '书记员 张 紫 君速 录 员 卢 山' → 提取'张紫君'（去除空格，忽略速录员）",
            "- '书记员 徐 一 马 克 数 据 网' → 提取'徐一'（去除空格，忽略水印）",
            "- '书记员 欧汉庭附相关法律条文' → 提取'欧汉庭'（忽略后面的'附'和法律条文）",
            "- '审判长 王晓萍人民审判员 曹开贵' → 提取'王晓萍'（忽略其他角色）",
            "- '代理审判员 古丽合尼木·尼牙孜' → 提取'古丽合尼木·尼牙孜'（少数民族姓名）",
            "- '书记员 宝 音 其 其 格' → 提取'宝音其其格'（去除空格）",
            "- '审判员移送执行员执行' → null（法律条文，无姓名）",
            "- '审判人员、书记员署名' → null（程序性描述，无姓名）",
            "",
            "**重要：每个id只能出现一次！**",
            "错误示例：[{\"id\": \"123_456\", \"name\": \"张三\"}, {\"id\": \"123_456\", \"name\": \"李四\"}] ✗",
            "正确示例：[{\"id\": \"123_456\", \"name\": \"张三\"}] ✓",
            "",
            "返回格式（仅JSON数组，不要markdown代码块）：",
            "[\n  {\"id\": \"片段ID\", \"name\": \"姓名或null\"},\n  ...\n]",
            "",
            "返回结果："
        ]
        
        return '\n'.join(prompt_parts)
    
    def _parse_batch_result(self, result_text: str, batch: List[Dict]) -> Dict[str, Dict]:
        """
        解析批量处理的JSON结果
        
        Args:
            result_text: API返回的文本
            batch: 原始批次数据（用于补充角色信息）
        
        Returns:
            解析后的结果字典
        """
        result_map = {}
        
        try:
            # 清理可能的markdown代码块
            result_text = result_text.strip()
            if result_text.startswith('```'):
                lines = result_text.split('\n')
                result_text = '\n'.join(lines[1:-1]) if len(lines) > 2 else result_text
            result_text = result_text.replace('```json', '').replace('```', '').strip()
            
            # 解析JSON (增加 strict=False 允许处理字符串内的控制字符)
            try:
                results = json.loads(result_text, strict=False)
            except json.JSONDecodeError:
                # 尝试进一步清洗：移除不可见控制字符（除了换行符、回车符和制表符）
                clean_text = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', result_text)
                results = json.loads(clean_text, strict=False)
            
            # 构建ID到角色的映射
            id_to_role = {item['id']: item['role'] for item in batch}
            
            # 构建ID到原始文本的映射（用于验证）
            id_to_text = {item['id']: item['text'] for item in batch}
            
            # 处理每个结果（只保留每个id的第一个结果）
            seen_ids = set()
            duplicate_count = 0
            
            for item in results:
                snippet_id = item.get('id')
                name = item.get('name')
                
                # 检查重复id
                if snippet_id in seen_ids:
                    duplicate_count += 1
                    continue  # 跳过重复的id
                
                if snippet_id and name and name != 'null' and isinstance(name, str):
                    # 验证姓名格式（支持少数民族姓名，最长50字，允许·分隔符和分号）
                    name = name.strip()
                    if re.match(r'^[\u4e00-\u9fa5·;]{2,50}$', name):
                        # 核心校验：姓名必须真实存在于原文中（忽略空格）
                        raw_text = id_to_text.get(snippet_id, "")
                        clean_name = re.sub(r'\s+', '', name)
                        clean_text = re.sub(r'\s+', '', raw_text)
                        
                        if clean_name in clean_text:
                            result_map[snippet_id] = {
                                'name': name,
                                'role': id_to_role.get(snippet_id, '')
                            }
                            seen_ids.add(snippet_id)
                        else:
                            # 如果姓名不在原文中，记录警告并设为 null
                            print(f"\n  校验失败: ID {snippet_id} 提取的名字 '{name}' 不在原文中，已忽略")
                            seen_ids.add(snippet_id)
                elif snippet_id:
                    seen_ids.add(snippet_id)  # 标记为已见，即使name为null
            
            if duplicate_count > 0:
                print(f"\n  警告: 发现 {duplicate_count} 个重复的id，已自动去重（保留第一个）")
        
        except json.JSONDecodeError as e:
            print(f"\n  JSON解析失败: {e}")
            print(f"  返回内容: {result_text[:200]}...")
        except Exception as e:
            print(f"\n  结果处理失败: {e}")
        
        return result_map
