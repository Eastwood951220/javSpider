import re
from typing import Any, Dict, List


def _parse_field_value(field_name, value_span) -> Any:
    """根据字段名解析对应的值"""
    if field_name == "日期":
        return value_span.css("::text").get(default="").strip()

    elif field_name == "時長":
        duration_text = value_span.css("::text").get(default="0").strip()
        match = re.search(r"\d+", duration_text)
        return int(match.group()) if match else 0

    elif field_name in ["導演", "片商", "系列"]:
        return value_span.css("a::text").get(default="").strip()

    elif field_name == "評分":
        rating_text = " ".join(value_span.xpath(".//text()").getall()).strip()
        if not rating_text:
            return 0.0
        match = re.search(r"([\d.]+)\s*分", rating_text)
        return float(match.group(1)) if match else 0.0

    elif field_name == "類別":
        return [t.strip() for t in value_span.css("a::text").getall() if t.strip()]

    elif field_name == "演員":
        actors = []
        actor_links = value_span.css("a")
        for a in actor_links:
            sibling_female = a.xpath("following-sibling::strong[contains(@class,'female')]")
            if sibling_female:
                actor_name = a.css("::text").get(default="").strip()
                if actor_name:
                    actors.append(actor_name)
        return actors

    return ""


def _should_skip_item(filters, detail_data: Dict) -> bool:
    """根据过滤器判断是否跳过该项"""
    # 取消注释以下代码以启用过滤
    rating_min = filters.get("rating_min", 0)
    duration_max = filters.get("duration_max")
    max_actors = filters.get("max_actors")
    exclude_tags = filters.get("exclude_tags", [])

    if detail_data["rating"] < rating_min:
        return True

    if duration_max and detail_data["duration"] > duration_max:
        return True

    if max_actors and len(detail_data["actors"]) > max_actors:
        return True

    if exclude_tags and any(t.upper() in [et.upper() for et in exclude_tags]
                            for t in detail_data["tags"]):
        return True

    return False

def _calculate_magnet_weight_javdb(magnet_data: Dict) -> float:
    """计算磁力链接的权重"""
    base_weight = magnet_data["size"]

    # 中文优先加权
    priority_boost = 10000 if any("字幕" in t for t in magnet_data["tags"]) else 0

    return base_weight + priority_boost

def _calculate_magnet_weight_javbus(magnet_data: Dict) -> float:
    base_weight = magnet_data.get("size", 0)

    tags = magnet_data.get("tags", [])
    has_chinese = any("字幕" in t for t in tags)

    # 字幕优先 + 高清优先
    priority_boost = 0
    if has_chinese:
        priority_boost += 10000

    return base_weight + priority_boost


def _prefilter_magnets_javdb(magnets, only_chinese: bool) -> List:
    """预过滤磁力链接"""
    if not only_chinese:
        return magnets

    chinese_magnets = [m for m in magnets if m.css(".tags .is-warning")]
    return chinese_magnets if chinese_magnets else magnets


def _prefilter_magnets_javbus(magnets, only_chinese: bool) -> List:
    """预过滤磁力链接"""
    if not only_chinese:
        return magnets

    chinese_magnets = []

    for m in magnets:
        tags = m.get("tags", [])
        if any("中字" in t or "字幕" in t for t in tags):
            chinese_magnets.append(m)

    return chinese_magnets if chinese_magnets else magnets


def _parse_size(text: str) -> float:
    """解析大小文本为 MB"""
    if not text:
        return 0.0

    match = re.search(r"([\d.]+)\s*(GB|MB)", text, re.IGNORECASE)
    if not match:
        return 0.0

    size = float(match.group(1))
    unit = match.group(2).upper()
    return size * 1024 if unit == "GB" else size


def _parse_actors(response) -> List[str]:
    """多层策略解析演员信息"""
    selectors = [
        ".star-box a[title]::attr(title)",
        "p a[href*='/star/']::text",
        ".star-name a::text"
    ]

    actors: List[str] = []
    for sel in selectors:
        names = response.css(sel).getall()
        if names:
            actors.extend([n.strip() for n in names if n.strip()])

    # 去重 + 保留顺序
    return list(dict.fromkeys(actors))


def _safe_extract_first(selector, default: str = "") -> str:
    """安全提取单值"""
    try:
        value = selector.get()
        return value.strip() if value else default
    except Exception:
        return default


def _safe_join_texts(selector) -> str:
    """拼接多段文本"""
    try:
        return " ".join(selector.css("::text").getall()).strip()
    except Exception:
        return ""
