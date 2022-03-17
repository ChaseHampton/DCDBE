import scrapy
from scrapy_splash import SplashRequest
import sqlite3
from pathlib import Path


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
          for i=1,tonumber(maxcount)/100,1
          do
            local nextpage = splash:select('a#WT_CL_VDRDIR_VW\\\\$hdown\\\\$0')
            pages[i] = splash:html()
            assert(nextpage:mouse_click())
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

    def process_responses(self):
        curs = self.db_conn.cursor()
        select_query = """SELECT page, response FROM responses"""
        curs.execute(select_query)
        pages = curs.fetchall()
        for page in pages:
            pass

    def create_table(self):
        create_string = """
        CREATE TABLE IF NOT EXISTS responses (
            id INTEGER PRIMARY KEY,
            page INTEGER,
            response TEXT
        );
        """
        curs = self.db_conn.cursor()
        curs.execute(create_string)
        self.db_conn.commit()
        curs.close()
