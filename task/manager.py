# manager.py
import yaml
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from pathlib import Path
import sys

# 添加项目根路径
sys.path.append(str(Path(__file__).resolve().parent.parent))

from task.utils import determine_source, build_final_url, ensure_string
from logs.manager import get_logger  # 使用优化日志模块


# 固定配置文件路径
CONFIG_FILE = Path(__file__).resolve().parent / "task.yml"


# ====================================================================== #
# 🎛️ 过滤配置数据类
# ====================================================================== #
@dataclass
class FilterConfig:
    """任务过滤配置"""
    only_chinese: bool = False
    _extra_filters: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not isinstance(self._extra_filters, dict):
            self._extra_filters = {}

    def __getattr__(self, name: str) -> Any:
        if name in self._extra_filters:
            return self._extra_filters[name]
        raise AttributeError(f"'FilterConfig' 没有属性 '{name}'")

    def get(self, key: str, default: Any = None) -> Any:
        if hasattr(self, key) and not key.startswith('_'):
            return getattr(self, key)
        return self._extra_filters.get(key, default)

    def has(self, key: str) -> bool:
        return (hasattr(self, key) and not key.startswith('_')) or key in self._extra_filters

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        result = {'only_chinese': self.only_chinese}
        result.update(self._extra_filters)
        return result


# ====================================================================== #
# 📦 任务数据类
# ====================================================================== #
@dataclass
class Task:
    """任务定义"""
    name: str
    url: str
    url_type: str
    is_skip: bool = False
    filter: FilterConfig = field(default_factory=FilterConfig)
    source: Optional[str] = None
    final_url: Optional[str] = None

    def __post_init__(self):
        self.name = ensure_string(self.name)
        self.url = ensure_string(self.url)
        self.url_type = ensure_string(self.url_type)

        if self.filter is None:
            self.filter = FilterConfig()

        self.source = self.source or determine_source(self.url)
        self._build_final_url()

    def _build_final_url(self):
        try:
            self.final_url = build_final_url(
                self.url, self.url_type, self.filter.to_dict(), self.source
            )
        except Exception:
            self.final_url = self.url

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Task":
        """从字典创建 Task"""
        try:
            filter_data = data.get("filter", {}) or {}
            known_filters = {"only_chinese": filter_data.get("only_chinese", False)}
            extra_filters = {k: v for k, v in filter_data.items() if k not in known_filters}

            return cls(
                name=ensure_string(data["name"]),
                url=ensure_string(data["url"]),
                url_type=ensure_string(data["url_type"]),
                is_skip=data.get("is_skip", False),
                filter=FilterConfig(
                    only_chinese=known_filters["only_chinese"],
                    _extra_filters=extra_filters
                ),
                source=data.get("source")
            )
        except KeyError as e:
            raise ValueError(f"任务配置缺少必要字段: {e}")
        except Exception as e:
            raise RuntimeError(f"解析任务配置失败: {e}")

    def to_dict(self) -> Dict[str, Any]:
        """序列化任务为字典"""
        return {
            "name": self.name,
            "url": self.url,
            "url_type": self.url_type,
            "is_skip": self.is_skip,
            "filter": self.filter.to_dict(),
            "source": self.source,
            "final_url": self.final_url
        }

    def get(self, key: str, default: Any = None) -> Any:
        """
        安全地获取任务属性
        - 支持获取顶层属性，如 "name"、"url"
        - 支持访问 filter 下的字段，如 "filter.only_chinese"
        """
        try:
            # 支持点号路径访问，例如 "filter.only_chinese"
            if "." in key:
                obj = self
                for part in key.split("."):
                    obj = getattr(obj, part)
                return obj
            return getattr(self, key, default)
        except AttributeError:
            return default


# ====================================================================== #
# 🧩 任务加载器
# ====================================================================== #
class TaskLoader:
    """任务加载器，负责从 YAML 文件读取任务配置"""

    def __init__(self):
        """使用默认配置初始化"""
        self.config_file: Path = CONFIG_FILE
        self.logger = get_logger("TaskLoader", console=True, file=False, level=logging.INFO)
        self.tasks: List[Task] = []
        self._load_tasks()

    def _load_tasks(self):
        """加载任务配置文件"""
        if not self.config_file.exists():
            self.logger.error(f"配置文件不存在: {self.config_file}")
            raise FileNotFoundError(f"配置文件不存在: {self.config_file}")

        try:
            with open(self.config_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            tasks_data = data.get("tasks", [])
            if not isinstance(tasks_data, list) or not tasks_data:
                self.logger.warning("未在配置文件中找到有效任务列表")
                return

            self.tasks = [Task.from_dict(t) for t in tasks_data]
            self.logger.info(f"✅ 成功加载 {len(self.tasks)} 个任务")

        except yaml.YAMLError as e:
            self.logger.error(f"YAML 解析错误: {e}")
            raise
        except Exception as e:
            self.logger.error(f"加载任务时出错: {e}")
            raise

    def get_all_tasks(self) -> List[Task]:
        """返回所有任务（浅拷贝）"""
        return self.tasks.copy()

    def find_task_by_name(self, name: str) -> Optional[Task]:
        """按名称查找任务"""
        name = ensure_string(name)
        return next((t for t in self.tasks if t.name == name), None)
