import json
import re
import time

from scrapy.selector import Selector
import scrapy
from scrapy_redis.spiders import RedisSpider
from scrapy_redis.utils import bytes_to_str
from scrapy_splash import SplashRequest

from tutorial.RedisUtil import redisUtil
from tutorial.items import TutorialItem
from tutorial.mongodbUtil import mongodbUtil
from tutorial.settings import SingleMONGODB_SERVER, SingleMONGODB_PORT

lua_script = '''
function main(splash, args)
  assert(splash:go(args.url))
  while not splash:select('pre') do
        assert(splash:wait(0.1))
  end
  if (splash:select('.fixedbox')) then
      while not splash:select('.fixedboxComplete') do
        assert(splash:wait(0.5))
      end
  end
  return {
    html = splash:html()
  }
end
'''


class GovSpider(RedisSpider):
    name = 'gov'

    # start_urls = ['https://www.ncbi.nlm.nih.gov/nuccore/UATI01000012.1']

    def __init__(self, redis_key, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongodbUtil = mongodbUtil(SingleMONGODB_SERVER, SingleMONGODB_PORT, "electron_spider")
        self.redis_util = redisUtil()
        self.redis_key = redis_key

    def next_requests(self):
        found = 0
        use_set = False
        fetch_one = self.server.spop if use_set else self.server.lpop
        # TODO: Use redis pipeline execution.
        splash_args = {
            "lua_source": lua_script,
            'timeout': 90,
            'resource_timeout': 10
        }
        while found < self.redis_batch_size:
            data = fetch_one(self.redis_key)
            if not data:
                # Queue empty.
                break
            url = bytes_to_str(data, self.redis_encoding)
            # print(url)
            req = yield SplashRequest(url, self.parse, endpoint='execute',
                                      args=splash_args, dont_filter=True)
            if req:
                yield req
                found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)

    def parse(self, response):
        # print(response.text)
        item = TutorialItem()
        pattern = re.compile("(?i)(<SCRIPT)[\\s\\S]*?((</SCRIPT>)|(/>))")
        page_text = re.sub(pattern, "", response.text)
        title = Selector(text=page_text).xpath('//*[@id="maincontent"]/div/div[5]/div[1]/h1//text()').get()
        GenBank = Selector(text=page_text).xpath('//*[@id="maincontent"]/div/div[5]/div[1]/p[1]//text()').get()
        context = "".join(Selector(text=page_text).xpath('//pre//text()').getall())
        item['collection'] = self.redis_key.split(":")[1] + '-' + time.strftime("%Y-%m-%d", time.localtime())
        item['title'] = title
        item['GenBank'] = GenBank
        item['context'] = context
        item['timestamp'] = int(time.time())
        yield item
