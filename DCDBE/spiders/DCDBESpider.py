import logging
import re

import scrapy
from scrapy_splash import SplashRequest
import sqlite3
from pathlib import Path
from ..items import DCDBEItem


class DcdbespiderSpider(scrapy.Spider):
    name = 'DCDBESpider'

    def __init__(self):
        super(DcdbespiderSpider, self).__init__()
        self.db_path = Path('./DCDBE/data/responses.db')
        self.db_conn = sqlite3.connect(self.db_path)

    def start_requests(self):
        self.create_table()
        curs = self.db_conn.cursor()
        curs.execute('SELECT COUNT(*) FROM responses')
        results = curs.fetchone()
        if results[0] == 0:
            yield scrapy.Request(
                'https://supplier.wmata.com/psp/supplier/SUPPLIER/ERP/c/WT_CL_SEP_MENU.WT_CL_VDR_DIR_CMP.GBL?Page=WT_CL_VDR_DIR_PG&Action=U',
                dont_filter=True, callback=self.collect_data)
        else:
            yield scrapy.Request(
                'https://supplier.wmata.com/psp/supplier/SUPPLIER/ERP/c/WT_CL_SEP_MENU.WT_CL_VDR_DIR_CMP.GBL?Page=WT_CL_VDR_DIR_PG&Action=U',
                dont_filter=True, callback=self.process_responses)

    def collect_data(self, response):
        script = """
        function main(splash, args)
          assert(splash:go(args.url))
          assert(splash:wait(3.0))
          local pages = {}
          local btn = splash:select('#WT_CL_VDIR_WRK_SEARCH')
          assert(btn:mouse_click())
          assert(splash:wait(6.0))
          local vw = splash:select('a#WT_CL_VDRDIR_VW\\\\$hviewall\\\\$0')
          assert(vw:mouse_click())
          assert(splash:wait(1.0))
          local maxpage = splash:select('span.PSGRIDCOUNTER')
          local maxnum = maxpage:text()
          i, j = maxnum:find('%d+$')
          local maxcount = string.sub(maxnum, i ,j)
          for i=1,(tonumber(maxcount)/100)+1,1
          do
            local nextpage = splash:select('a#WT_CL_VDRDIR_VW\\\\$hdown\\\\$0')
            pages[i] = splash:html()
            if nextpage then
              assert(nextpage:mouse_click())
            end
            assert(splash:wait(1.0))
          end
          return {
            html = splash:html(),
            png = splash:png(),
            pages = pages,
          }
        end
        """
        url = response.xpath('//body/div[@id="ptifrmcontent"]//iframe/@src')[0].get()
        yield SplashRequest(url=url, endpoint='execute', dont_filter=True, args={'lua_source': script, 'timeout': 3600},
                            callback=self.store_response)

    def store_response(self, response):
        curs = self.db_conn.cursor()
        insert_query = """INSERT INTO responses (page, response) VALUES (?, ?)"""
        for k, v in response.data['pages'].items():
            curs.execute(insert_query, (k, v))
        self.db_conn.commit()
        curs.close()
        req = scrapy.Request(url='http://example.com', dont_filter=True, callback=self.process_responses)
        yield req

    def process_responses(self, response):
        curs = self.db_conn.cursor()
        select_query = """SELECT page, response FROM responses"""
        curs.execute(select_query)
        pages = curs.fetchall()
        req = scrapy.Request(url='http://example.com', dont_filter=True, meta={'page': pages}, callback=self.parse_response_page)
        yield req

    def parse_response_page(self, response):
        pages = response.meta['page']
        for page in pages:
            sel = scrapy.Selector(text=page[-1], type='html')
            rows = sel.xpath('//tr[contains(@id, "_row")]')
            curs = self.db_conn.cursor()
            # try:
            for row in rows:
                items = DCDBEItem()
                item_dict = {'cert_type': row.xpath('.//span[contains(@id, "_DESCR20")]/text()').getall(),
                             'cert_num': row.xpath('.//span[contains(@id, "_CERTSERIAL")]/text()').getall(),
                             'company': row.xpath('.//span[contains(@id, "_NAME1")]/text()').getall(), 'address1': '',
                             'address2': '', 'name': row.xpath('.//span[contains(@id, "_CONTACT_NAME")]/text()').getall(),
                             'title': row.xpath('.//span[contains(@id, "_CONTACT_TITLE")]/text()').getall(),
                             'desc': row.xpath('.//span[contains(@id, "_DESCRLONG")]/text()').getall(),
                             'cert_agency': row.xpath('.//span[contains(@id, "_CERTIF")]/text()').getall()}
                address_full = row.xpath('.//span[contains(@id, "_ADDRESS")]/text()').getall()
                item_dict['address1'], item_dict['address2'], item_dict['phone'], item_dict['fax'], \
                    item_dict['email'], item_dict['website'] = self.parse_address(address_full)
                items['CertType'] = self.clean_item(item_dict['cert_type'])
                items['CertNum'] = self.clean_item(item_dict['cert_num'])
                items['CompanyName'] = self.clean_item(item_dict['company'])
                items['Address1'] = self.clean_item(item_dict['address1'])
                items['Address2'] = self.clean_item(item_dict['address2'])
                items['Phone'] = self.clean_item(item_dict['phone'])
                items['Fax'] = self.clean_item(item_dict['fax'])
                items['Email'] = self.clean_item(item_dict['email'])
                items['Website'] = self.clean_item(item_dict['website'])
                items['ContactName'] = self.clean_item(item_dict['name'])
                items['ContactTitle'] = self.clean_item(item_dict['title'])
                items['Description'] = self.clean_item(item_dict['desc'])
                items['CertificationAgency'] = self.clean_item(item_dict['cert_agency'])
                yield items
            curs.execute("""DELETE FROM responses WHERE page = ?""", (page[0],))
            self.db_conn.commit()
            self.logger.info(f"Successfully parsed page {page[0]}.")

    def parse_address(self, address_full: list):
        address1 = address_full[0].strip()
        address2 = address_full[1].strip()
        phone = [v.replace("Phone: ", "") for v in address_full if 'Phone:' in v]
        fax = [v.replace("Fax: ", "") for v in address_full if 'Fax:' in v]
        email = [v.replace("Email: ", "") for v in address_full if 'Email:' in v]
        website = [v.replace("Website: ", "") for v in address_full if 'Website:' in v]
        return address1, address2, phone, fax, email, website

    def clean_item(self, item) -> str:
        if not item:
            return ''
        if type(item) == list:
            if len(item) == 1:
                return item[0].strip()
            elif len(item) > 1 and re.match(r'\d{6}', item[0]):
                return ";".join([v.strip() for v in item])
            else:
                return " ".join([v.strip() for v in item])
        else:
            return item.strip()

    def create_table(self):
        create_string = """
        CREATE TABLE IF NOT EXISTS responses (
            id INTEGER PRIMARY KEY,
            page INTEGER,
            response TEXT
        );
        """
        curs = self.db_conn.cursor()
        # curs.execute("""DROP TABLE responses""")
        # self.db_conn.commit()
        curs.execute(create_string)
        self.db_conn.commit()
        curs.close()
