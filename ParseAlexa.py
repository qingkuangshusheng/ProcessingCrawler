#coding:utf-8
import csv
from zipfile import ZipFile
from MongoQueue import MongoQueue
from StringIO import StringIO
from html_downLoader import HtmlDownLoader
class AlexaCallback:
    def __init__(self,maxurls=10):
        self.max_urls=maxurls
        self.seed_url="http://s3.amazonaws.com/alexa-static/top-1m.csv.zip"
        self.downloader=HtmlDownLoader()
        self.mongo_queue=MongoQueue()

    def __call__(self, url,):
        if url==self.seed_url:
            #zipped_data=self.downloader.downLoad("http://s3.amazonaws.com/alexa-static/top-1m.csv.zip")
            #zipped_data=open("top-1m.csv.zip","r").read()
            #urls=[]
            count=0
            #print zipfile.is_zipfile(StringIO(zipped_data))
            with ZipFile("top-1m.csv.zip") as zf:
                csv_filename=zf.namelist()[0]
                for _,website in csv.reader(zf.open(csv_filename)):
                    #urls.append("http://"+website)
                    url="http://"+website
                    self.mongo_queue.push(url)
                    count+=1
                    if count==self.max_urls:
                        break
            #print urls
            return self.mongo_queue
# if __name__ == '__main__':
#     alexaCallback=AlexaCallback()
#     alexaCallback("http://s3.amazonaws.com/alexa-static/top-1m.csv.zip")
