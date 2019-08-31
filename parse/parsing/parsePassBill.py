# -*- coding:utf-8 -*-
from pymongo import MongoClient

from urllib.request import urlopen
from urllib.parse import urlencode, unquote, quote_plus
import urllib
from bs4 import BeautifulSoup
import re



serviceKey = "Qdb5KydABzjhFWA4CzQ4gSgtLMnxo6C5jGrv%2FOLaQ6evALcjMQDkPllXowGQzr9DzraCGymtgDwuQmge6QJzng%3D%3D"

def getPassBill() :
    
    # 처리 의안
    # url과   parameter 값 설정
    url = "http://apis.data.go.kr/9710000/BillInfoService2/getJsictionComiteProcessList"
    
    # totalCount를 찾기 위해 임시로 파싱
    queryParams = '?' + urlencode({quote_plus('serviceKey') : serviceKey, # 서비스 키
                                   quote_plus('pageNo') : '1', # 페이지 번호
                                   quote_plus('numOfRows') : '1', # 한 페이지의 결과 수
                                   quote_plus('start_age_cd') : '20'}) # 20대 국회의원
    
    request = urllib.request.Request(url + unquote(queryParams))
    request.get_method = lambda: 'GET' # GET 방식으로 요청
    request_body = urlopen(request).read() # 웹에 있는 소스 가져오기
    soup = BeautifulSoup(request_body, 'html.parser')
    
    totalCount = int(soup.totalcount.text) # 전체 결과 수
    numOfRows = 100 # 한페이지의 결과 수
    pageSize = int(totalCount/numOfRows) + 1 # 페이지 수
    print('totalCount :', totalCount)
    print('numOfRows :', numOfRows)
    print('pageSize :', pageSize)
    
    
    ############################################################
    
    count = 0;
    for i in range(pageSize):
        print(i+1,"번째 페이지")
        
        queryParams = '?' + urlencode({quote_plus('servicekey') : serviceKey, # 서비스 키
                                       quote_plus('pageNo') : i+1, # 페이지 번호
                                       quote_plus('numOfRows') : numOfRows, # 한 페이지의 결과 수
                                       quote_plus('start_age_cd') : '20'}) # 20대 국회의원
    
        request = urllib.request.Request(url + unquote(queryParams))
#         print('request :' , url + queryParams)
        
        request.get_method = lambda: 'GET' # GET 방식으로 요청
        
        response_body = urlopen(request).read()
        
#         print('Result :', response_body)
#         print('DataType of Result Data :', type(response_body))
        
        soup = BeautifulSoup(response_body, 'html.parser')
    
#         print(soup.prettify()) # 전체  출력
        
        items = soup.find_all('item')

        for item in items:
            mongo = pyMongo()
            res = mongo.add_one(item)
            count += 1;
            print("\t", count, "개 입력중...")
        
        print("입력 완료!") 

class pyMongo(object):
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client['jcjc']
        
    def add_one(self, item):
        # 데이터 삽입
        row = {} # Dict 객체 생성
        
        for col in item:
            row[col.name] = col.text # Dict 객체에 key, value 추가
        
        return self.db.passBill.insert_one(row) # passBIll 컬렉션에 추가
    

if __name__ == '__main__':
    getPassBill()
    
