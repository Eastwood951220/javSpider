import os
import json
from typing import Dict
from pathlib import Path
from logs.manager import get_logger

logger = get_logger("cookies_manager", console=True, file=False)

class CookieManager:
    """Cookies 管理工具，支持多爬虫多 cookies"""

    def __init__(self, base_path: Path = None):
        """
        Args:
            base_path: cookies 存放目录，默认 cookies/
        """
        self.base_path = base_path or Path(__file__).parent
        self.logger = logger

    def load_cookies(self, name: str) -> Dict[str, str]:
        """
        加载指定名字的 cookies 文件，返回字典

        Args:
            name: cookies 文件名（不含 .json 后缀）

        Returns:
            Dict[str, str]
        """
        cookies_file = self.base_path / f"{name}_cookies.json"

        if not cookies_file.exists():
            self.logger.warning(f"⚠️ 未找到 {cookies_file}，可能需要先登录！")
            return {}

        try:
            with open(cookies_file, "r", encoding="utf-8") as f:
                raw_cookies = json.load(f)
            cookies = {c["name"]: c["value"] for c in raw_cookies}
            self.logger.info(f"✅ 成功加载 {len(cookies)} 个 cookies: {cookies_file.name}")
            return cookies
        except (json.JSONDecodeError, KeyError, IOError) as e:
            self.logger.warning(f"⚠️ 加载 cookies 失败 {cookies_file}: {e}")
            return {}
