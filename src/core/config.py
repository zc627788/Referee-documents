import json
import os
from pathlib import Path
from typing import Dict, Any


class Config:
    """配置管理器"""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            project_root = Path(__file__).parent.parent.parent
            config_path = project_root / "config" / "config.json"
        
        self.config_path = Path(config_path)
        self._config = self._load_config()
        self._load_env()
    
    def _load_config(self) -> Dict[str, Any]:
        """加载配置文件"""
        if not self.config_path.exists():
            return self._get_default_config()
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def _load_env(self):
        """加载环境变量（API密钥等敏感信息）"""
        env_path = self.config_path.parent / ".env"
        if env_path.exists():
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key.strip()] = value.strip()
        
        api_key = os.environ.get('API_KEY', '')
        if api_key:
            self._config['api']['api_key'] = api_key
    
    def _get_default_config(self) -> Dict[str, Any]:
        """默认配置"""
        return {
            "api": {
                "base_url": "https://api.vectorengine.ai/v1",
                "api_key": "",
                "model": "glm-4-flash",
                "timeout": 30,
                "max_retries": 3
            },
            "extraction": {
                "confidence_threshold_high": 0.8,
                "confidence_threshold_low": 0.5,
                "max_candidate_regions": 5,
                "context_window_size": 200
            },
            "processing": {
                "max_workers": 4,
                "batch_size": 100
            }
        }
    
    def get(self, key: str, default=None):
        """获取配置项（支持点号分隔的路径）"""
        keys = key.split('.')
        value = self._config
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
                if value is None:
                    return default
            else:
                return default
        return value
    
    def save(self):
        """保存配置到文件"""
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self._config, f, indent=2, ensure_ascii=False)
    
    @property
    def api_config(self):
        return self._config['api']
    
    @property
    def extraction_config(self):
        return self._config['extraction']
    
    @property
    def processing_config(self):
        return self._config['processing']
