import json
import re
import requests
from typing import List
import sys
from pathlib import Path

try:
    from .config import Config
    from ..models import Person
except ImportError:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from core.config import Config
    from models import Person


class AIExtractor:
    """AI提取器 - 使用GLM-4-Flash"""
    
    def __init__(self, config: Config = None):
        if config is None:
            config = Config()
        self.config = config
        self.api_config = config.api_config
    
    def extract(self, signature_text: str) -> List[Person]:
        """
        使用AI提取
        
        Args:
            signature_text: 落款文本
            
        Returns:
            人员列表
        """
        if not self.api_config['api_key']:
            print("警告：未配置API密钥，跳过AI提取")
            return []
        
        prompt = self._build_prompt(signature_text)
        
        try:
            result = self._call_api(prompt)
            persons = self._parse_response(result)
            return persons
        except Exception as e:
            print(f"AI提取失败: {e}")
            return []
    
    def _build_prompt(self, signature_text: str) -> str:
        """构建提示词"""
        return f"""你是一个专业的法律文书信息提取助手。请从以下文本中提取所有人员的姓名和角色。

文本内容：
{signature_text}

要求：
1. 提取所有出现的人员姓名及其角色
2. 角色包括：审判长、审判员、代理审判员、陪审员、人民陪审员、书记员、执行员、法官、助理法官等
3. 按出现顺序排列
4. 姓名必须是2-4个汉字
5. 严格返回JSON数组格式

返回格式示例：
[
  {{"name": "张三", "role": "审判长"}},
  {{"name": "李四", "role": "审判员"}},
  {{"name": "王五", "role": "书记员"}}
]

如果没有找到任何人员信息，返回空数组 []

请直接返回JSON，不要包含任何其他文字说明。"""
    
    def _call_api(self, prompt: str) -> str:
        """调用API"""
        url = self.api_config['base_url']
        if not url.endswith('/chat/completions'):
            if url.endswith('/v1'):
                url = f"{url}/chat/completions"
            elif url.endswith('/'):
                url = f"{url}v1/chat/completions"
            else:
                url = f"{url}/v1/chat/completions"
        
        headers = {
            "Authorization": f"Bearer {self.api_config['api_key']}",
            "Content-Type": "application/json"
        }
        
        data = {
            "model": self.api_config['model'],
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "max_tokens": 1000
        }
        
        max_retries = self.api_config.get('max_retries', 3)
        timeout = self.api_config.get('timeout', 30)
        
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    url,
                    headers=headers,
                    json=data,
                    timeout=timeout
                )
                response.raise_for_status()
                result = response.json()
                return result['choices'][0]['message']['content']
            except Exception as e:
                if attempt == max_retries - 1:
                    raise
                print(f"API调用失败，重试 {attempt + 1}/{max_retries}: {e}")
        
        raise Exception("API调用失败")
    
    def _parse_response(self, response: str) -> List[Person]:
        """解析API响应"""
        json_match = re.search(r'\[.*\]', response, re.DOTALL)
        if not json_match:
            return []
        
        try:
            data = json.loads(json_match.group())
            persons = []
            for item in data:
                if 'name' in item and 'role' in item:
                    name = item['name'].strip()
                    role = item['role'].strip()
                    if self._is_valid_name(name):
                        persons.append(Person(name=name, role=role))
            return persons
        except json.JSONDecodeError:
            return []
    
    @staticmethod
    def _is_valid_name(name: str) -> bool:
        """验证姓名是否合法"""
        if not name or len(name) < 2 or len(name) > 4:
            return False
        if not re.match(r'^[\u4e00-\u9fa5]+$', name):
            return False
        return True
