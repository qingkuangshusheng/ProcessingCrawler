#coding:utf-8
import time
import threading
from html_downLoader import HtmlDownLoader
import ParseAlexa
import multiprocessing
from MongoQueue import MongoQueue
import sys
if sys.getdefaultencoding()!="utf-8":
    reload(sys)
    sys.setdefaultencoding("utf-8")
SLEEP_TIME=1
alexaCallback=ParseAlexa.AlexaCallback()
crawl_queue=alexaCallback("http://s3.amazonaws.com/alexa-static/top-1m.csv.zip")
max_threads=5
result={}
def threaded_crawler():
    threads=[]
    #crawl_queue=alexaCallback("http://s3.amazonaws.com/alexa-static/top-1m.csv.zip")
    dlownloader=HtmlDownLoader()
    def process_queue():
        while True:
            try:
                url=crawl_queue.pop()
                crawl_queue.complete(url)
            except Exception,e:
                print e.message
                break
            else:
                print "正在爬取%s"%url
                html=dlownloader.downLoad(url)
                result[url]=html

    while threads or crawl_queue.__nonzero__():
        while len(threads)<max_threads and crawl_queue.__nonzero__():
            thread=threading.Thread(target=process_queue)
            thread.setDaemon(True)
            thread.start()
            threads.append(thread)
            time.sleep(SLEEP_TIME)
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
    print result,'\n\n\n\n\n'

def process_crawler():
    num_cpus=multiprocessing.cpu_count()
    print "Starting {} process".format(num_cpus)
    process=[]
    for i in range(num_cpus):
        p=multiprocessing.Process(target=threaded_crawler)
        p.daemon=True
        p.start()
        # p.join()
        process.append(p)
    for p in process:
        p.join()
    # print result
if __name__ == '__main__':
    #alexaCallback=ParseAlexa.AlexaCallback()
    #threaded_crawler(alexaCallback)
    process_crawler()
    # print result




