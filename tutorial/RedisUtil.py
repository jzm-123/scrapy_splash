import redis

from tutorial.settings import REMOTE_REDIS_URL, REMOTE_REDIS_PASSWORD
import logging
from apscheduler.schedulers.blocking import BlockingScheduler


class redisUtil:

    def __init__(self, db=0):
        self.r = redis.Redis(host=REMOTE_REDIS_URL, port=6379, db=db, password=REMOTE_REDIS_PASSWORD,
                             decode_responses=True)

    def inser2StartUrl(self, key, value):
        try:
            self.r.lpush(key, value)
        except IOError:
            logging.error('连接redis失败')

    def save_key_item_value(self, key, item, value):
        try:
            self.r.hset(key, item, value)
        except IOError:
            logging.error('连接redis失败')

    def get_remain_hop_num(self, key, item):
        try:
            hop_num = self.r.hget(key, item)
            if not hop_num:
                hop_num = -1  # -1 表示未插入， 需要将config中的hop_num插入
            return hop_num
        except IOError:
            logging.error('连接redis失败')

    def get_key_item_value(self, key, item):
        try:
            return self.r.hget(key, item)
        except IOError:
            logging.error('连接redis失败')

    def delete_status(self, key, item):
        try:
            self.r.hdel(key, item)
        except IOError:
            logging.error('连接redis失败')

    def insert_ncbi_url(self, key, value):
        # url_dict = {"ncbi:wtic-MB:link:start_urls": "https://www.ncbi.nlm.nih.gov/nuccore/?term=wtic-MB",
        #             "ncbi:ma15:link:start_urls": "https://www.ncbi.nlm.nih.gov/nuccore/?term=MA15",
        #             "ncbi:ma15_exon1:link:start_urls": "https://www.ncbi.nlm.nih.gov/nuccore/?term=MA15+ExoN1"
        #             }
        # for key, value in url_dict.items():
        self.inser2StartUrl(key, value)


if __name__ == '__main__':
    r = redisUtil(db=0)
    with open("ExoN16.csv", "r", encoding='utf-8') as f:
        for line in f.readlines():
            if line:
                # count += 1
                # print(count)
                r.inser2StartUrl("ncbi:ma15:detail1:start_urls", line.strip())
    # scheduler = BlockingScheduler()
    # url_dict = {"ncbi:wtic-MB:link:start_urls": "https://www.ncbi.nlm.nih.gov/nuccore/?term=wtic-MB",
    #             "ncbi:ma15:link:start_urls": "https://www.ncbi.nlm.nih.gov/nuccore/?term=MA15",
    #             "ncbi:ma15_exon1:link:start_urls": "https://www.ncbi.nlm.nih.gov/nuccore/?term=MA15+ExoN1"
    #             }
    # for key, value in url_dict.items():
    #     hour = 1
    #     scheduler.add_job(r.insert_ncbi_url, "cron", day_of_week='0-6', hour=hour, args=[key,value])
    #     hour += 2
    # scheduler.start()
    # count = 0
    # with open("ExoN16.csv", "r", encoding='utf-8') as f:
    #     for line in f.readlines():
    #         if line:
    #             count += 1
    #             print(count)
    #             r.inser2StartUrl("ncbi:ExoN1:detail7:start_urls", line)
