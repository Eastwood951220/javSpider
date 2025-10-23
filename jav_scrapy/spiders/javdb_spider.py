#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
jav_scrapy/spiders/javdb_spider.py
JavDB 爬虫：根据 task 动态配置日志与 MongoDB。
"""
from typing import Optional, Dict, Any, Tuple
import scrapy
from scrapy.exceptions import CloseSpider

from logs.manager import get_logger
from cookies.manager import CookieManager
from task.manager import Task
from db.mongo import MongoDBManager
from .utils import (_parse_field_value,
                    _should_skip_item,
                    _calculate_magnet_weight,
                    _prefilter_magnets,
                    _parse_size)


class JavdbSpider(scrapy.Spider):
    @property
    def logger(self):
        return self._logger

    name = "javdb_spider"
    allowed_domains = ["javdb.com"]

    # 字段映射表，避免重复的if-else判断
    FIELD_MAPPING = {
        "日期": "release_date",
        "時長": "duration",
        "導演": "director",
        "片商": "maker",
        "系列": "series",
        "評分": "rating",
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
            source="javdb",
            final_url="https://javdb.com"
        )
        self.start_urls = [self.task.final_url]

        # 初始化组件
        self._init_components()

        # 状态变量
        self.duplicate_count = 0
        self.max_duplicates = 5
        self.stop_current_actor = False

    def _init_components(self):
        """初始化日志、数据库等组件"""
        # 初始化日志系统
        self._init_logger()

        # 初始化 MongoDB
        self._init_mongodb()

        # 初始化 Cookies
        self._init_cookies()

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

    def _init_cookies(self):
        """初始化 Cookies"""
        try:
            cookie_mgr = CookieManager()
            self.cookies = cookie_mgr.load_cookies('javdb')
            self.logger.info("✅ 已加载 Cookies")
        except Exception as e:
            self.logger.warning(f"⚠️ Cookies 加载失败: {e}")
            self.cookies = None

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
            cookies=self.cookies,
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

        items = response.css("div.item a.box")
        if not items:
            self.logger.info("⚠️ 未找到任何作品，爬虫结束。")
            return

        for item in items:
            if self.stop_current_actor:
                break
            req = self.process_list_item(item, response)
            if req:
                req.priority = 10  # 详情页优先级高
                yield req

        # 处理下一页
        next_url = response.css("nav.pagination a[rel='next']::attr(href)").get()
        if next_url and not self.stop_current_actor:
            yield response.follow(
                next_url,
                callback=self.parse_list,
                errback=self.handle_error,
                priority=-10
            )

    def process_list_item(self, item, response):
        title = item.css("::attr(title)").get() or ""
        href = response.urljoin(item.css("::attr(href)").get())
        code = item.css(".video-title strong::text").get() or ""

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
            errback=self.handle_error
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

        self.logger.info(f"正在抓取名称: {name}-{code}-{title}")
        # 解析基本信息
        detail_data = self._parse_basic_info(response)

        # 应用过滤器
        # if _should_skip_item(self.task.filter, detail_data):
        #     self.logger.info(f"⏩ {title} | {code} 被过滤器跳过")
        #     return

        # 获取最佳磁力链接
        best_magnet, max_size, has_chinese_sub = self.get_best_magnet(
            response,
            only_chinese=self.task.filter.get("only_chinese", False)
        )

        if not best_magnet:
            self.logger.info(f"⚠️ {code} | {title} 没有可用磁力链接，跳过")
            return

        # 构建最终数据
        item = self._build_final_item(
            title, code, best_magnet, max_size, has_chinese_sub, detail_data
        )

        self.logger.info(f"✅ 完整详情: {code} | {title} |  大小: {max_size}MB")
        yield item

    def _parse_basic_info(self, response) -> Dict[str, Any]:
        """解析基本信息面板"""
        panel_blocks = response.css("nav.movie-panel-info > div.panel-block")

        # 初始化默认值
        result = {
            "release_date": "",
            "duration": 0,
            "director": "",
            "maker": "",
            "series": "",
            "rating": 0.0,
            "tags": [],
            "actors": []
        }

        for block in panel_blocks:
            field_name = block.css("strong::text").get(default="").strip().rstrip(":")
            value_span = block.css("span.value")

            if field_name not in self.FIELD_MAPPING:
                continue

            field_key = self.FIELD_MAPPING[field_name]
            result[field_key] = _parse_field_value(field_name, value_span)

        return result

    def get_best_magnet(self, response, only_chinese: bool = False) -> Tuple[str, float, bool]:
        """
        从 JavDB 详情页提取最佳磁力链接
        :return: tuple(best_magnet, max_size_in_MB, has_chinese_sub)
        """
        magnets = response.css("#magnets-content .item")

        # 预过滤
        filtered_magnets = _prefilter_magnets(magnets, only_chinese)

        best_magnet = ""
        max_weight = 0
        has_chinese_sub = False

        for magnet_elem in filtered_magnets:
            magnet_data = self._parse_magnet_element(magnet_elem)
            if not magnet_data:
                continue

            weight = _calculate_magnet_weight(magnet_data)

            if weight > max_weight:
                best_magnet = magnet_data["url"]
                max_weight = weight
                has_chinese_sub = magnet_data["has_chinese_sub"]

        return best_magnet, round(max_weight, 2), has_chinese_sub

    def _parse_magnet_element(self, magnet_elem) -> Optional[Dict]:
        """解析单个磁力链接元素"""
        try:
            # 获取磁力链接
            magnet_url = (magnet_elem.css("button.copy-to-clipboard::attr(data-clipboard-text)").get() or
                          magnet_elem.css(".magnet-name a::attr(href)").get())

            if not magnet_url or not magnet_url.startswith("magnet:?"):
                return None

            # 获取文件大小
            meta_text = magnet_elem.css(".magnet-name .meta::text").get(default="").strip()
            size = _parse_size(meta_text)

            # 获取标签
            tags = magnet_elem.css(".magnet-name .tags .tag::text").getall()
            has_chinese_sub = any("字幕" in tag for tag in tags)

            return {
                "url": magnet_url,
                "size": size,
                "tags": tags,
                "meta_text": meta_text,
                "has_chinese_sub": has_chinese_sub
            }

        except Exception as e:
            self.logger.warning(f"❌ 解析磁链出错: {e}")
            return None

    def _build_final_item(self, title: str, code: str, best_magnet: str,
                          max_size: float, has_chinese_sub: bool,
                          detail_data: Dict) -> Dict[str, Any]:
        """构建最终的数据项"""
        tags = detail_data["tags"].copy()
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
            "rating": detail_data["rating"],
            "tags": tags,
            "actors": detail_data["actors"],
        }

    @logger.setter
    def logger(self, value):
        self._logger = value
