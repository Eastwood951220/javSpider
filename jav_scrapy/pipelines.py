# jav_scrapy/pipelines.py
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from db.mongo import MongoDBManager
from logs.manager import get_logger

class MongoPipeline:
    """Pipeline：使用 MongoDBManager 动态 collection，仅存储 Item"""

    def __init__(self):
        self.mongo: MongoDBManager = None
        self.logger = get_logger("MongoPipeline", console=True, file=False)

    def open_spider(self, spider):
        """爬虫启动时初始化 MongoDB"""
        self.mongo = MongoDBManager()
        self.logger.info(f"🟢 爬虫启动: {spider.name}")

    def process_item(self, item, spider):
        """处理每个 Item"""
        adapter = ItemAdapter(item)
        name = adapter.get("name")
        code = adapter.get("code")

        if not name or not code:
            raise DropItem(f"❌ 缺少必要字段: {item}")

        # 动态 collection_name
        collection_name = name.replace(" ", "_")


        # 插入 MongoDB
        try:
            self.mongo.insert_if_not_exists(collection_name, dict(adapter), unique_field="code")
            self.logger.info(f"📦 新增成功: {name} | {code}")
        except Exception as e:
            self.logger.error(f"❌ 插入失败: {name} | {code} - {e}")

        return item

    def close_spider(self, spider):
        """爬虫关闭时关闭 MongoDB"""
        self.logger.info(f"🔴 爬虫结束: {spider.name}")
        if self.mongo:
            self.mongo.close()
