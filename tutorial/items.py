# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TutorialItem(scrapy.Item):
    # define the fields for your item here like:
    collection = scrapy.Field()
    title = scrapy.Field()
    GenBank = scrapy.Field()
    context = scrapy.Field()
    timestamp=scrapy.Field()
