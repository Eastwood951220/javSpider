import re
from typing import Optional, Dict, Any, List, Tuple
import scrapy
from scrapy.exceptions import CloseSpider
from bs4 import BeautifulSoup

from db.mongo import MongoDBManager
from logs.manager import get_logger
from task.manager import Task
from .utils import (_parse_actors,
                    _safe_extract_first,
                    _safe_join_texts,
                    _calculate_magnet_weight_javbus,
                    _parse_size,
                    _prefilter_magnets_javbus)


class JavbusSpider(scrapy.Spider):
    @property
    def logger(self):
        return self._logger

    name = "javbus_spider"
    allowed_domains = ["javbus.com"]

    custom_settings = {
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 2,
        "RANDOMIZE_DOWNLOAD_DELAY": False,
        "AUTOTHROTTLE_ENABLED": False,
    }

    FIELD_MAPPING = {
        "識別碼": "code",
        "發行日期": "release_date",
        "長度": "duration",
        "導演": "director",
        "發行商": "maker",
        "系列": "series",
        "類別": "tags",
        "演員": "actors"
    }

    def __init__(self, task: Optional[Task] = None, *args, **kwargs):
        """
        初始化爬虫。
        Args:
            task: Task 对象，包含 name、source、final_url、keywords 等。
        """
        super().__init__(**kwargs)
        self._logger = None
        self.task = task or Task(
            name="UnknownTask",
            source="javbus",
            final_url="https://javbus.com"
        )
        self.start_urls = [self.task.final_url]

        # 初始化组件
        self._init_components()

        # 状态变量
        self.duplicate_count = 0
        self.max_duplicates = 3
        self.stop_current_actor = False

        self.total_count = 0  # 总尝试影片数
        self.success_count = 0  # 成功影片数
        self.fail_count = 0  # 失败影片数
        self.failed_items = []
        self.chinese_count = 0  # 含中文字幕磁力的影片数

    def _init_components(self):
        """初始化日志、数据库等组件"""
        # 初始化日志系统
        self._init_logger()

        # 初始化 MongoDB
        self._init_mongodb()

    def _init_logger(self):
        """初始化日志系统"""
        try:
            self.logger = get_logger(
                name=f"spider_{self.task.name}",
                console=True,
                file=True,
            )
            self.logger.info(f"🕷️ 启动 JavDB 爬虫: {self.task.name}")
            self.logger.debug(f"任务详情: {self.task}")
        except Exception as e:
            # 如果自定义日志失败，使用scrapy的日志
            self.logger = self.logger
            self.logger.error(f"❌ 自定义日志初始化失败，使用默认日志: {e}")

    def _init_mongodb(self):
        """初始化 MongoDB 连接"""
        try:
            self.mongo = MongoDBManager()
            self.collection_name = self._get_collection_name()
            self.logger.info(f"✅ 已连接 MongoDB，集合名: {self.collection_name}")
        except Exception as e:
            self.logger.error(f"❌ MongoDB 初始化失败: {e}")
            self.mongo = None
            raise

    def _get_collection_name(self) -> str:
        """生成集合名称"""
        base_name = getattr(self.task, 'name', 'unknown_task').replace(" ", "_")
        return base_name

    async def start(self):
        if not self.start_urls:
            self.logger.error("❌ 没有设置起始URL")
            return

        yield scrapy.Request(
            url=self.start_urls[0],
            callback=self.parse_list,
            errback=self.handle_error,
            meta={'download_timeout': 30}
        )

    def handle_error(self, failure):
        """处理请求错误"""
        self.logger.error(f"❌ 请求失败: {failure.value}")

    def parse_list(self, response):
        self.logger.info(f"正在抓取名称: {self.task.name}, URL: {response.url}")

        if self.stop_current_actor:
            self.logger.info(f"🛑 已停止该项目 {self.task.name} 的爬取。")
            return

        items = response.css("div.item a.movie-box")
        if not items:
            self.logger.info("⚠️ 未找到任何作品，爬虫结束。")
            return

        for item in items:
            if self.stop_current_actor:
                break
            yield from self.process_list_item(item, response)

        # 处理下一页
        next_url = response.css("a#next::attr(href)").get()
        if next_url and not self.stop_current_actor:
            yield response.follow(
                next_url,
                callback=self.parse_list,
                errback=self.handle_error,
                priority=-10
            )

    def process_list_item(self, item, response):
        title = item.css("img::attr(title)").get() or ""
        href = item.css("::attr(href)").get()
        code = item.css("date::text").get()
        print(f"{title} | {href} | {code}")

        # 检查是否已存在
        if self._is_duplicate_item(code):
            return

        # 请求详情页
        yield response.follow(
            href,
            callback=self.parse_detail,
            meta={
                "name": self.task.name,
                "title": title,
                "code": code
            },
            errback=self.handle_error,
            priority=0
        )

    def _is_duplicate_item(self, code: str) -> bool:
        existing = self.mongo.find_one(self.collection_name, {
            "code": code,
            "magnet": {"$ne": None}
        })
        if existing:
            # 更新最后检查时间
            self.mongo.update_one(self.collection_name, {"_id": existing["_id"]})

            self.duplicate_count += 1
            self.logger.info(f"⏩ 已存在 {code}（连续重复 {self.duplicate_count}）")

            is_skip = self.task.get("is_skip", True)

            # 检查是否达到重复上限
            if is_skip and self.duplicate_count >= self.max_duplicates:
                self.logger.warning(f"🚫 任务 {self.task.name} 连续 {self.max_duplicates} 个重复，停止爬取。")
                self.stop_current_actor = True
                raise CloseSpider(f"任务 {self.task.name} 达到重复上限，停止")

            return True
        else:
            self.duplicate_count = 0  # 重置计数器
            return False

    def parse_detail(self, response):
        name = response.meta["name"]
        title = response.meta["title"]
        code = response.meta["code"]
        url = response.url
        self.total_count += 1  # 🎬 总计+1

        self.logger.info(f"正在抓取名称: {name}-{code}-{title}")
        try:
            # 解析基本信息
            detail_data = self._parse_basic_info(response)

            script_text = "".join(response.css("script::text").getall())
            gid_match = re.search(r"var\s+gid\s*=\s*(\d+);", script_text)
            uc_match = re.search(r"var\s+uc\s*=\s*(\d+);", script_text)
            img_match = re.search(r"var\s+img\s*=\s*['\"](.+?)['\"];", script_text)

            if not gid_match or not uc_match or not img_match:
                self.fail_count += 1  # ❌ 无磁力链接
                self.logger.warning(f"⚠️ 无法解析 gid/uc/img: {response.url}")
                return

            gid = gid_match.group(1)
            uc = uc_match.group(1)
            img = img_match.group(1)

            ajax_url = f"https://www.javbus.com/ajax/uncledatoolsbyajax.php?gid={gid}&lang=zh&img={img}&uc={uc}&floor=735"

            yield scrapy.Request(
                url=ajax_url,
                callback=self.parse_magnet_ajax,
                meta={
                    "title": title,
                    "code": code,
                    "detail_data": detail_data
                },
                errback=self.handle_error,
                priority=10
            )
        except Exception as e:
            self.fail_count += 1
            self.failed_items.append({
                "task": self.task.name,
                "code": code,
                "url": url,
                "reason": f"解析出错: {e}"
            })
            self.logger.error(f"❌ 解析详情页失败: {code} | 错误: {e}")

    def _parse_basic_info(self, response) -> Dict[str, Any]:
        """
        解析 JavBus 影片详情页基本信息

        Args:
            response: scrapy Response 对象
        Returns:
            Dict[str, Any]: 包含影片基本信息的字典
        """

        # 初始化结果字典
        result: Dict[str, Any] = {
            "code": "",
            "title": "",
            "release_date": "",
            "duration": 0,
            "director": "",
            "maker": "",
            "series": "",
            "tags": [],
            "actors": []
        }

        try:
            info_block = response.css("div.row.movie div.info")

            # 封面与标题
            result["title"] = _safe_extract_first(response.css(".screencap img::attr(title)"))

            # 提取主要信息
            for p in info_block.css("p"):
                header = _safe_extract_first(p.css(".header::text")).rstrip(":：")

                if not header or header not in self.FIELD_MAPPING:
                    continue

                key = self.FIELD_MAPPING[header]
                value = self._extract_field_value(header, p)

                # 类型兼容：防止返回 None 或异常类型
                if isinstance(result[key], list) and isinstance(value, list):
                    result[key].extend(value)
                else:
                    result[key] = value

            # 如果演员为空，再做兜底解析
            if not result["actors"]:
                result["actors"] = _parse_actors(response)

        except Exception as e:
            self.logger.warning(f"[解析失败] {response.url} ({type(e).__name__}): {e}")

        return result

    def _extract_field_value(self, header: str, p_element) -> Any:
        """根据字段头提取对应的值"""
        try:
            if header == "發行日期":
                # 提取所有文本并去掉字段头
                text = _safe_join_texts(p_element)
                text = re.sub(r"[發行日期:：\s]", "", text)
                # 兼容多种日期格式
                match = re.search(r"\d{4}[-/]\d{2}[-/]\d{2}", text)
                return match.group() if match else ""

            elif header == "長度":
                text = _safe_join_texts(p_element)
                match = re.search(r"\d+", text)
                return int(match.group()) if match else 0

            elif header in ["導演", "發行商", "系列"]:
                return _safe_extract_first(p_element.css("a::text"))

            elif header == "類別":
                # 向下查找下一个 <p> 标签（兄弟节点）
                next_p = p_element.xpath("following-sibling::p[1]")
                return [
                    t.strip() for t in next_p.css(".genre a::text").getall() if t.strip()
                ]

            elif header == "演員":
                return [t.strip() for t in p_element.css("a::text").getall() if t.strip()]

            elif header == "識別碼":
                # 只选第二个 span（排除 header）
                spans = p_element.css("span::text").getall()
                if len(spans) >= 2:
                    return spans[1].strip()
                # 或直接选择非 .header 的 span
                code = p_element.css("span:not(.header)::text").get()
                return code.strip() if code else ""

        except Exception as e:
            self.logger.debug(f"字段解析异常 [{header}]: {e}")

        # 默认返回
        return [] if header in ["類別", "演員"] else ""

    # ----------------------------------------------------------------------

    def parse_magnet_ajax(self, response):
        code = response.meta["code"]
        title = response.meta["title"]
        detail_data = response.meta["detail_data"]
        self.logger.info(f"ajax请求 {code} | {title}")

        # # 获取最佳磁力链接
        best_magnet, max_size, has_chinese_sub = self.get_best_magnet(
            response,
            only_chinese=self.task.filter.get("only_chinese", False)
        )
        if not best_magnet:
            self.fail_count += 1  # ❌ 无磁力链接
            self.failed_items.append({
                "task": self.task.name,
                "code": code,
                "reason": "无可用磁力链接"
            })
            self.logger.info(f"⚠️ {code} | {title} 没有可用磁力链接，跳过")
            return

        # 构建最终数据
        item = self._build_final_item(
            title, code, best_magnet, max_size, has_chinese_sub, detail_data
        )
        self.success_count += 1  # ✅ 成功影片 +1
        if has_chinese_sub:
            self.chinese_count += 1  # 🇨🇳 含中文字幕 +1
        self.logger.info(f"✅ 完整详情: {code} | {title} |  大小: {max_size}MB")

        yield item

    def get_best_magnet(self, response, only_chinese: bool = False) -> Tuple[str, float, bool]:
        """
        从 JavBus 详情页提取最佳磁力链接

        Args:
            response: scrapy Response 对象
            only_chinese: 是否仅选择中文字幕版本

        Returns:
            (best_magnet_url, weight, has_chinese_sub)
        """
        soup = BeautifulSoup(response.text, "lxml")
        magnets = soup.find_all("tr")
        if not magnets:
            self.logger.warning(f"⚠️ 未找到任何磁力链接: {response.url}")
            return "", 0.0, False

        # 预过滤
        filtered_magnets = _prefilter_magnets_javbus(magnets, only_chinese)
        best_magnet = ""
        max_weight = 0
        has_chinese_sub = False

        for magnet_elem in filtered_magnets:
            magnet_data = self._parse_magnet_element(magnet_elem)

            if not magnet_data:
                continue

            weight = _calculate_magnet_weight_javbus(magnet_data)

            if weight >= max_weight:
                best_magnet = magnet_data["url"]
                max_weight = weight
                has_chinese_sub = magnet_data["has_chinese_sub"]

        return best_magnet, round(max_weight, 2), has_chinese_sub

    def _parse_magnet_element(self, magnet_elem) -> Optional[Dict]:
        """解析单个磁力链接元素"""
        try:

            # 提取 magnet 链接
            a_tag = magnet_elem.find("a", href=True)
            if not a_tag:
                return None

            magnet_url = a_tag["href"].strip()
            if not magnet_url.startswith("magnet:?"):
                return None

            # 文件大小（第二列）
            size_td = magnet_elem.find_all("td")[1] if len(magnet_elem.find_all("td")) > 1 else None
            size_text = size_td.get_text(strip=True) if size_td else ""
            size = _parse_size(size_text)

            # 日期（第三列）
            date_td = magnet_elem.find_all("td")[2] if len(magnet_elem.find_all("td")) > 2 else None
            date = date_td.get_text(strip=True) if date_td else ""

            # 标签（第一列内的 .btn）
            tags = [btn.get_text(strip=True) for btn in magnet_elem.select(".btn")]
            has_chinese_sub = any("中字" in t or "字幕" in t for t in tags)

            return {
                "url": magnet_url,
                "size": size,
                "tags": tags,
                "has_chinese_sub": has_chinese_sub
            }

        except Exception as e:
            self.logger.warning(f"❌ 解析磁链出错: {e}")
            return None

    # ----------------------------------------------------------------------

    def _build_final_item(self, title: str, code: str, best_magnet: str,
                          max_size: float, has_chinese_sub: bool,
                          detail_data: Dict) -> Dict[str, Any]:
        """构建最终的数据项"""
        tags = detail_data.get("tags", []).copy()
        if has_chinese_sub and "字幕" not in tags:
            tags.append("中文字幕")

        return {
            "name": self.task.name,
            "title": title,
            "code": code,
            "magnet": best_magnet,
            "size": max_size,
            "release_date": detail_data["release_date"],
            "director": detail_data["director"],
            "maker": detail_data["maker"],
            "series": detail_data["series"],
            "tags": tags,
            "actors": detail_data["actors"],
        }

    @logger.setter
    def logger(self, value):
        self._logger = value

    def close(self, reason):
        """爬虫结束时输出统计结果"""
        self.logger.info("📊 爬取统计结果 --------------------------------")
        self.logger.info(f"🧩 任务名称: {self.task.name}")
        self.logger.info(f"🎬 总计尝试影片数: {self.total_count}")
        self.logger.info(f"✅ 成功获取磁力数: {self.success_count}")
        self.logger.info(f"🇨🇳 含中文字幕磁力数: {self.chinese_count}")
        self.logger.info(f"❌ 失败（无磁力/出错）数: {self.fail_count}")
        self.logger.info(f"📦 爬虫结束原因: {reason}")
        self.logger.info("--------------------------------------------")

        if self.failed_items:
            self.logger.warning("❗ 以下影片未成功爬取:")
            for fail in self.failed_items:
                self.logger.warning(
                    f"  [任务: {fail['task']}] {fail['code']} | {fail['reason']} | URL: {fail['url']}"
                )