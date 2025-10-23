# utils/util.py
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
sys.path.append(str(Path(__file__).resolve().parent.parent))

from logs.manager import get_logger
# 日志记录器
logger = get_logger("utils", console=True, file=False)


def determine_source(url: str) -> str:
    """
    根据URL判断数据源

    Args:
        url: 要分析的URL

    Returns:
        str: 数据源标识 (javdb, javbus, 或其他)
    """
    if not url or not isinstance(url, str):
        return "unknown"

    url_lower = url.lower()
    if "javdb.com" in url_lower:
        return "javdb"
    elif "javbus.com" in url_lower:
        return "javbus"

    # 提取主域名
    try:
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain.startswith("www."):
            domain = domain[4:]
        return domain.split(".")[0]
    except Exception as e:
        logger.warning(f"无法解析URL域名: {url}, 错误: {e}")
        return "unknown"


def normalize_url(url: str) -> str:
    """
    标准化URL格式，确保带协议头

    Args:
        url: 原始URL

    Returns:
        str: 标准化后的URL
    """
    if not url or not isinstance(url, str):
        return ""

    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def extract_domain(url: str) -> Optional[str]:
    """
    从URL中提取域名

    Args:
        url: 要提取的URL

    Returns:
        Optional[str]: 域名或None
    """
    if not url or not isinstance(url, str):
        return None

    try:
        parsed = urlparse(url)
        return parsed.netloc or None
    except Exception as e:
        logger.warning(f"提取域名失败: {url}, 错误: {e}")
        return None


def ensure_string(value: Any) -> str:
    """
    确保值为字符串类型

    Args:
        value: 任意类型

    Returns:
        str: 转换后的字符串
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return str(value, errors="ignore")
    return str(value)

# =====================
# 🔧 公共工具函数
# =====================

def build_final_url(base_url: str, url_type: str, filter_conf: Dict[str, Any], source: str) -> str:
    """
    根据筛选条件构建最终URL

    Args:
        base_url: 基础URL
        url_type: URL类型 (actor, code, company等)
        filter_conf: 过滤配置字典
        source: 数据源 (javdb, javbus等)

    Returns:
        str: 构建后的最终URL
    """
    base_url = normalize_url(base_url)
    source = source.lower() if source else "unknown"
    url_type = url_type.lower() if url_type else "unknown"

    try:
        if source == "javdb":
            if url_type == "actor":
                return _build_javdb_actor_url(base_url, filter_conf)
            elif url_type == "code":
                return _build_javdb_code_url(base_url, filter_conf)
            else:
                return _build_javdb_other_url(base_url, filter_conf)
        elif source == "javbus":
            return _build_javbus_url(base_url, filter_conf, url_type)
        else:
            return base_url
    except Exception as e:
        logger.warning(f"构建最终URL失败: {base_url}, 错误: {e}")
        return base_url


# =====================
# 🔧 公共工具函数
# =====================

def _merge_url_params(base_url: str, new_params: Dict[str, List[str]], overwrite: bool = False) -> str:
    """
    合并或覆盖URL参数

    Args:
        base_url: 原始URL
        new_params: 新增或修改的参数字典
        overwrite: True=覆盖原参数，False=合并参数

    Returns:
        str: 更新后的URL
    """
    try:
        parsed = urlparse(base_url)
        existing_params = parse_qs(parsed.query)

        for key, values in new_params.items():
            if not isinstance(values, list):
                values = [str(values)]
            if overwrite or key not in existing_params:
                existing_params[key] = values
            else:
                for val in values:
                    if val not in existing_params[key]:
                        existing_params[key].append(val)

        query = urlencode(existing_params, doseq=True)
        return urlunparse(parsed._replace(query=query))
    except Exception as e:
        logger.warning(f"合并URL参数失败: {base_url}, 错误: {e}")
        return base_url


# =====================
# 🎬 JAVDB URL 构建
# =====================

def _build_javdb_actor_url(base_url: str, filter_conf: Dict[str, Any]) -> str:
    """JAVDB 演员页面URL"""
    params = {"sort_type": ["0"], "t": ["d"]}
    if filter_conf.get("exclude_multi_person"):
        params["t"].append("s")
    if filter_conf.get("only_chinese"):
        params["t"].append("c")
    return _merge_url_params(base_url, params, overwrite=False)


def _build_javdb_code_url(base_url: str, filter_conf: Dict[str, Any]) -> str:
    """JAVDB 作品代码页面URL"""
    f_value = ["cnsub"] if filter_conf.get("only_chinese") else ["download"]
    params = {"sort_type": ["5"], "f": f_value}
    return _merge_url_params(base_url, params, overwrite=True)


def _build_javdb_other_url(base_url: str, filter_conf: Dict[str, Any]) -> str:
    """JAVDB 其他类型页面URL"""
    f_value = ["cnsub"] if filter_conf.get("only_chinese") else ["download"]
    params = {"f": f_value}
    return _merge_url_params(base_url, params, overwrite=True)


# =====================
# 📀 JAVBUS URL 构建
# =====================

def _build_javbus_url(base_url: str, filter_conf: Dict[str, Any], url_type: str) -> str:
    """JAVBUS 页面URL，目前保持原URL"""
    return base_url
