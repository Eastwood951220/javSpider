# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JavScrapyItem(scrapy.Item):
    # 基本信息
    name = scrapy.Field()  # 演员或任务名称
    title = scrapy.Field()  # 影片标题
    code = scrapy.Field()  # 番号
    magnet = scrapy.Field()  # 最佳磁力链接
    size = scrapy.Field()  # 文件大小 (MB)

    # 影片详情
    release_date = scrapy.Field()  # 发行日期
    director = scrapy.Field()  # 导演
    maker = scrapy.Field()  # 制作商
    series = scrapy.Field()  # 系列
    rating = scrapy.Field()  # 评分
    tags = scrapy.Field()  # 标签列表
    actors = scrapy.Field()  # 演员列表

    # 系统字段
    inserted_at = scrapy.Field()  # 插入时间
    updated_at = scrapy.Field()  # 更新时间
