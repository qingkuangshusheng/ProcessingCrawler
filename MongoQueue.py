#coding:utf-8
from datetime import datetime,timedelta
from pymongo import MongoClient,errors
import logging

class MongoQueue:
    OUTSTANDING,PROCESSING,COMPLETE=range(3)
    def __init__(self,client=None,timeout=20):
        self.client=MongoClient("localhost",27017) if client is None else client
        self.db=self.client.cache
        self.timeout=timeout

    def __nonzero__(self):
        record=self.db.crawl_queue.find_one({"status":{"$ne":self.COMPLETE}})
        return True if record else False

    def push(self,url):
        try:
            self.db.crawl_queue.insert({"_id":url,"status":self.OUTSTANDING})
        except errors.DuplicateKeyError,e:
            #logging.error(str(e))
            pass

    def pop(self):
        record=self.db.crawl_queue.find_and_modify(query={"status":self.OUTSTANDING},
                                                   update={"$set":{"status":self.PROCESSING,"timestamp":datetime.now()}})
        if record:
            return record["_id"]
        else:
            self.repair()
        raise KeyError("crawl_queue has not url")
            # except Exception,e:
            #     logging.error(str(e))
            #     return None

    def complete(self,url):
        self.db.crawl_queue.update({"_id":url},{"$set":{"status":self.COMPLETE}})

    def repair(self):
        record=self.db.crawl_queue.find_and_modify(
            query={"timestamp":{"$lt":datetime.now()-timedelta(seconds=self.timeout)},
                   "status":{"$ne":self.COMPLETE}},
            update={"$set":{"status":self.OUTSTANDING}}
        )
        if record:
            print "released:%s"%record["_id"]



