from pymongo import MongoClient
import traceback
import requests


class mongodbUtil:

    def __init__(self, SingleMONGODB_SERVER, SingleMONGODB_PORT, name):
        # 初始化mongodb连接
        self.MONGODB_SERVER = SingleMONGODB_SERVER
        self.MONGODB_PORT = SingleMONGODB_PORT
        self.MONGODB_DB = name
        try:
            self.client = MongoClient(host=self.MONGODB_SERVER, port=self.MONGODB_PORT)
            self.db = self.client[self.MONGODB_DB]
        except Exception as e:
            traceback.print_exc()

    def saveData(self, lis, field_list, collection):
        for item in lis:
            query = {}

            for field in field_list:
                query[field] = item.get(field, " ")
            self.db[collection].update_one(query, {"$set": item}, upsert=True)

    def get_cookie(self):
        return self.db['account'].find({"username": "Jennife06512978"})[0]

    def get_account(self, accountType, jobId):
        pipeline = [
            {
                '$match': {'accountType': accountType,
                           'status': 1}
            },
            {"$sample": {"size": 1}}
        ]
        data_iter = self.db['account'].aggregate(pipeline)  # 返回的是一个可迭代对象
        data_list = list(data_iter)
        cookies = {}
        proxy = ""
        if len(data_list) > 0:
            cookies = data_list[0].get("cookies")
            proxy = data_list[0].get("proxy")
            visits = data_list[0].get("visits")
            id = data_list[0].get("_id")
            visits = visits - 1
            query = {"_id":id}
            if visits > 0:
                self.db['account'].update_one(query, {"$set": {"visits":visits}})
            if visits == 0:
                self.db['account'].update_one(query, {"$set": {"visits":visits, "status":-2}})
                response = requests.post("http://120.55.51.51:8090/job/pauseByNoVisit?jobId="+jobId)
                print(response.text)
        return cookies, proxy  # 返回的是一个可迭代对象



if __name__ == '__main__':
    m = mongodbUtil("120.55.51.51", 27017, "electron_spider")
    x = m.get_account("twitter", "60ed6aa45da3c02a0880eee0")
    # print(x)
    # m.bind_account_proxy('adanmissc@outlook.com', '19971004.Jzmy', '127.0.0.1:10809')
