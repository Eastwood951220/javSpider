#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
app/main.py
任务启动入口：自动加载 task.yml 中的任务并顺序运行对应 Scrapy 爬虫
"""

# -------------------------------------------------------
# 🚨 必须在导入 Scrapy 前安装正确的 reactor！
# -------------------------------------------------------
from twisted.internet import asyncioreactor
asyncioreactor.install()

import logging
import sys
import signal
import time
from pathlib import Path
from typing import List, Optional, Type

from twisted.internet import reactor, defer
from twisted.internet.task import deferLater
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings

# -------------------------------------------------------
# 🧠 设置项目根目录
# -------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from logs.manager import get_logger
from task.manager import TaskLoader, Task

# -------------------------------------------------------
# 🔗 source 与爬虫类映射
# -------------------------------------------------------
SPIDER_MAP = {
    "javdb": "jav_scrapy.spiders.javdb_spider.JavdbSpider",
    "javbus": "jav_scrapy.spiders.javbus_spider.JavbusSpider",
}


def load_spider_class(source: str) -> Type:
    """动态加载爬虫类"""
    spider_path = SPIDER_MAP.get(source.lower())
    if not spider_path:
        raise ValueError(f"未找到与 source='{source}' 对应的爬虫类映射")

    try:
        module_name, class_name = spider_path.rsplit(".", 1)
        module = __import__(module_name, fromlist=[class_name])
        return getattr(module, class_name)
    except (ImportError, AttributeError) as e:
        raise ImportError(f"加载爬虫类失败: {spider_path} - {e}") from e


class TaskRunner:
    """🕹️ 顺序执行 Scrapy 爬虫任务"""

    def __init__(self) -> None:
        self.logger = get_logger("task_runner", console=True, file=True, level=logging.INFO)
        self.runner: Optional[CrawlerRunner] = None
        self.start_time: Optional[float] = None
        self._register_signal_handlers()

    # ------------------------------------------------------------------
    # 📡 系统信号与初始化
    # ------------------------------------------------------------------
    def _register_signal_handlers(self) -> None:
        """注册退出信号以支持优雅关闭"""
        def handle_exit(signum, frame):
            self.logger.warning(f"⚠️ 捕获信号 {signum}，正在优雅退出...")
            if reactor.running:
                reactor.stop()
            sys.exit(0)

        signal.signal(signal.SIGINT, handle_exit)   # Ctrl + C
        signal.signal(signal.SIGTERM, handle_exit)  # kill

    # ------------------------------------------------------------------
    # 🧩 任务处理逻辑
    # ------------------------------------------------------------------

    def validate_tasks(self, tasks: List[Task]) -> List[Task]:
        """验证并过滤无效任务"""
        valid = []
        for task in tasks:
            if not task.name or not task.source:
                self.logger.warning(f"跳过无效任务: 缺少必要字段 - {task}")
                continue

            if task.source.lower() not in SPIDER_MAP:
                self.logger.warning(f"跳过任务 '{task.name}': 不支持的 source '{task.source}'")
                continue

            if not task.final_url or not task.final_url.startswith(("http://", "https://")):
                self.logger.warning(f"跳过任务 '{task.name}': 无效的 URL '{task.final_url}'")
                continue

            valid.append(task)

        self.logger.info(f"✅ 任务验证完成: {len(valid)}/{len(tasks)} 个有效任务")
        return valid

    # ------------------------------------------------------------------
    # 🪶 日志输出逻辑
    # ------------------------------------------------------------------
    def _log_task_start(self, task: Task, index: int, total: int):
        self.logger.info("-" * 60)
        self.logger.info(f"🚀 开始任务 [{index}/{total}]: {task.name} ({task.source})")
        self.logger.debug(f"🌐 目标: {task.final_url}")

    def _log_task_result(self, task: Task, success: bool, duration: float):
        icon = "✅" if success else "❌"
        status = "完成" if success else "失败"
        self.logger.info(f"{icon} 任务{status}: {task.name} | 耗时 {duration:.2f}s")

    def _log_final_summary(self, duration: float, success: int, failed: int):
        self.logger.info("=" * 60)
        self.logger.info("🎉 所有任务执行完毕")
        self.logger.info(f"✅ 成功任务数: {success}")
        self.logger.info(f"❌ 失败任务数: {failed}")
        self.logger.info(f"⏱️ 总耗时: {duration:.2f} 秒")
        self.logger.info("=" * 60)

    def _display_tasks_summary(self, tasks: List[Task]):
        """打印任务摘要"""
        self.logger.info("📋 即将执行以下任务：")
        self.logger.info("=" * 60)
        for i, task in enumerate(tasks, 1):
            self.logger.info(f"{i}. {task.name} [{task.source}] → {task.final_url}")
        self.logger.info("=" * 60)

    # ------------------------------------------------------------------
    # 🕷️ 爬虫执行逻辑
    # ------------------------------------------------------------------
    @defer.inlineCallbacks
    def run_spiders(self, tasks: List[Task]):
        """按顺序执行所有爬虫任务"""
        self.start_time = time.time()
        settings = get_project_settings()
        self.runner = CrawlerRunner(settings)

        success, failed = 0, 0
        total = len(tasks)
        self.logger.info(f"🎯 共 {total} 个任务，开始顺序执行...")

        for idx, task in enumerate(tasks, start=1):
            start = time.time()
            try:
                self._log_task_start(task, idx, total)
                spider_cls = load_spider_class(task.source)
                yield self.runner.crawl(spider_cls, task=task)
                self._log_task_result(task, True, time.time() - start)
                success += 1

                # 任务间延迟（默认 2 秒）
                if idx < total:
                    delay = getattr(task, "delay", 2)
                    if delay > 0:
                        self.logger.debug(f"⏳ 等待 {delay} 秒后执行下一个任务...")
                        yield deferLater(reactor, delay, lambda: None)

            except Exception as e:
                failed += 1
                self.logger.exception(f"❌ 任务执行失败: {task.name} - {e}")
                self._log_task_result(task, False, time.time() - start)
                self.logger.info("⏩ 继续执行下一个任务...")
                continue

        self._log_final_summary(time.time() - self.start_time, success, failed)
        reactor.stop()

    # ------------------------------------------------------------------
    # 🧠 主调度入口
    # ------------------------------------------------------------------
    def main(self):
        """程序主入口"""
        self.logger.info("🚀 启动任务调度程序...")

        try:
            loader = TaskLoader()
            tasks = loader.get_all_tasks()
            if not tasks:
                self.logger.warning("⚠️ 未在配置文件中找到任务")
                return

            valid_tasks = self.validate_tasks(tasks)
            if not valid_tasks:
                self.logger.error("❌ 无有效任务可执行")
                return

            self._display_tasks_summary(valid_tasks)
            self.run_spiders(valid_tasks)
            reactor.run()

        except Exception as e:
            self.logger.exception(f"💥 程序运行异常: {e}")
            if reactor.running:
                reactor.stop()
            sys.exit(1)


def main():
    """CLI 入口"""
    TaskRunner().main()

if __name__ == "__main__":
    main()
