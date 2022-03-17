# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class DCDBEItem(scrapy.Item):
    CertType = scrapy.Field()
    CertNum = scrapy.Field()
    CompanyName = scrapy.Field()
    Address1 = scrapy.Field()
    Address2 = scrapy.Field()
    Phone = scrapy.Field()
    Fax = scrapy.Field()
    Email = scrapy.Field()
    Website = scrapy.Field()
    ContactName = scrapy.Field()
    ContactTitle = scrapy.Field()
    Description = scrapy.Field()
    CertificationAgency = scrapy.Field()
