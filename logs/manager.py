#!/usr/bin/env python3
"""
日志配置模块
提供统一的日志配置，可在整个项目中使用
"""

import sys
import logging
from pathlib import Path
from datetime import datetime

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)  # 确保日志目录存在


def setup_logging(
    name: str = "crawler",
    level: int = logging.INFO,
    console: bool = False,
    file: bool = False,
    log_dir: Path = LOG_DIR
) -> logging.Logger:
    """
    设置并返回配置好的日志记录器

    Args:
        name: 日志记录器名称
        level: 日志级别
        console: 是否输出到控制台
        file: 是否输出到文件
        log_dir: 日志文件目录

    Returns:
        配置好的日志记录器
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # 避免重复添加处理器
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        '%(asctime)s [%(name)s] [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 控制台处理器
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    # 文件处理器
    if file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"{name}_{timestamp}.log"
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_logger(
    name: str = "crawler",
    level: int = logging.INFO,
    console: bool = False,
    file: bool = False,
    log_dir: Path = LOG_DIR
) -> logging.Logger:
    """
    获取指定名称的日志记录器，如果尚未配置，则使用传入参数配置

    Args:
        name: 日志记录器名称
        level: 日志级别
        console: 是否输出到控制台
        file: 是否输出到文件
        log_dir: 日志文件目录

    Returns:
        日志记录器实例
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        return setup_logging(name=name, level=level, console=console, file=file, log_dir=log_dir)
    return logger


class LogMixin:
    """为类提供日志功能的混入类"""

    def __init__(self, logger_name: str = None, level: int = logging.INFO,
                 console: bool = False, file: bool = False, log_dir: Path = LOG_DIR):
        """
        Args:
            logger_name: 日志记录器名称，默认使用类名
            level: 日志级别
            console: 是否输出到控制台
            file: 是否输出到文件
            log_dir: 日志文件目录
        """
        self._logger = None
        self._logger_config = {
            "name": logger_name or f"crawler.{self.__class__.__name__}",
            "level": level,
            "console": console,
            "file": file,
            "log_dir": log_dir
        }

    @property
    def logger(self) -> logging.Logger:
        """获取类专用的日志记录器"""
        if self._logger is None:
            self._logger = get_logger(**self._logger_config)
        return self._logger


# 创建默认日志记录器（不输出）
default_logger = get_logger()

if __name__ == "__main__":
    # 测试日志配置
    logger1 = get_logger("test_console_file", console=True, file=True, level=logging.DEBUG)
    logger1.info("信息日志")
    logger1.debug("调试日志")
    logger1.warning("警告日志")
    logger1.error("错误日志")

    # 使用 LogMixin 的示例
    class MyClass(LogMixin):
        def __init__(self):
            super().__init__(console=True)

        def run(self):
            self.logger.info("MyClass 正在运行")

    obj = MyClass()
    obj.run()
