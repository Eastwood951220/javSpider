# utils/mongodb_utils.py
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError, ConnectionFailure
from typing import Dict, Any, Optional, Union, Mapping
from bson import ObjectId
from datetime import datetime
from logs.manager import get_logger

logger = get_logger("mongodb_utils", console=True, file=False)


class MongoDBManager:
    """简化版 MongoDB 管理工具：只保留新增、更新、删除、查重"""

    def __init__(
            self,
            uri: str = "mongodb://admin:admin123@localhost:27017/",
            db_name: str = "jav",
            connect_timeout: int = 5000
    ):
        try:
            self.client = MongoClient(uri, serverSelectionTimeoutMS=connect_timeout)
            self.client.admin.command("ping")
            self.db = self.client[db_name]
            logger.info(f"✅ 成功连接到 MongoDB: {db_name}")
        except ConnectionFailure as e:
            logger.critical(f"❌ 无法连接 MongoDB: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info("🔌 MongoDB 连接已关闭")

    # -------------------- 核心方法 -------------------- #
    def get_collection(self, name: str):
        safe_name = name.replace(" ", "_").replace(".", "_").replace("$", "_")
        return self.db[safe_name]

    def insert_if_not_exists(
            self, collection_name: str, document: Dict[str, Any], unique_field: str = "url"
    ) -> Optional[ObjectId]:
        """如果不存在则插入（基于 unique_field 去重）"""
        try:
            coll = self.get_collection(collection_name)
            coll.create_index([(unique_field, ASCENDING)], unique=True, sparse=True)
            existing = coll.find_one({unique_field: document.get(unique_field)})
            if existing:
                logger.debug(f"⚠️ 已存在文档: {existing['_id']}")
                return existing["_id"]

            document.setdefault("created_at", datetime.now())
            document.setdefault("updated_at", datetime.now())
            result = coll.insert_one(document)
            logger.debug(f"✅ 插入新文档: {result.inserted_id}")
            return result.inserted_id
        except DuplicateKeyError:
            existing = coll.find_one({unique_field: document.get(unique_field)})
            return existing["_id"] if existing else None
        except Exception as e:
            logger.error(f"❌ 条件插入失败: {e}")
            return None

    def update_one(self, collection_name: str, query: Dict[str, Any], update: Optional[Dict[str, Any]] = None,
                   upsert: bool = False) -> bool:
        """更新单个文档"""
        try:
            collection = self.get_collection(collection_name)
            if update is None:
                update = {"$set": {"updated_at": datetime.now()}}
            else:
                update.setdefault("$set", {})
                update["$set"]["updated_at"] = datetime.now()

            result = collection.update_one(query, update, upsert=upsert)
            return bool(result.modified_count or result.upserted_id)

        except Exception as e:
            logger.error(f"❌ 更新失败: {e}")
            return False

    def delete_one(self, collection_name: str, query: Dict[str, Any]) -> bool:
        """删除单个文档"""
        try:
            result = self.get_collection(collection_name).delete_one(query)
            return bool(result.deleted_count)
        except Exception as e:
            logger.error(f"❌ 删除失败: {e}")
            return False

    def find_one(self, collection_name: str, query: Dict[str, Any]) -> Mapping[str, Any] | bool | Any:
        """
        查找单个文档
        - 找到则返回文档(dict)
        - 未找到则返回 False
        """
        try:
            result = self.get_collection(collection_name).find_one(query)
            return result if result else False
        except Exception as e:
            logger.error(f"❌ 查找文档失败: {e}")
            return False
