from typing import Optional, Dict

import scrapy

class JavbusSpider(scrapy.Spider):
    name = "javbus_spider"
    allowed_domains = ["javbus.com"]

    def __init__(self, task: Optional[Dict] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.task = task
