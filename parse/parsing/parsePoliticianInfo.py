# -*- coding:utf-8 -*-
from pymongo import MongoClient

from urllib.request import urlopen
from urllib.parse import urlencode, unquote, quote_plus
import urllib
from bs4 import BeautifulSoup
import re



serviceKey = "Qdb5KydABzjhFWA4CzQ4gSgtLMnxo6C5jGrv%2FOLaQ6evALcjMQDkPllXowGQzr9DzraCGymtgDwuQmge6QJzng%3D%3D"

def getPoliticianInfo() :
    
    # 국회의원 현황 조회
    # url과   parameter 값 설정
    url = "http://apis.data.go.kr/9710000/NationalAssemblyInfoService/getMemberCurrStateList"
    
    # totalCount를 찾기 위해 임시로 파싱
    queryParams = '?' + urlencode({quote_plus('serviceKey') : serviceKey, # 서비스 키
                                   quote_plus('numOfRows') : '1'}) # 한 페이지의 결과 수
    
    request = urllib.request.Request(url + unquote(queryParams))
    request.get_method = lambda: 'GET' # GET 방식으로 요청
    request_body = urlopen(request).read() # 웹에 있는 소스 가져오기
    soup = BeautifulSoup(request_body, 'html.parser')
    
    totalCount = int(soup.totalcount.text) # 전체 결과 수
    numOfRows = 300 # 한페이지의 결과 수
    print('totalCount :', totalCount)
    print('numOfRows :', numOfRows)
    
    
    ############################################################

    count = 0;
    
    queryParams = '?' + urlencode({quote_plus('servicekey') : serviceKey, # 서비스 키
                                   quote_plus('numOfRows') : numOfRows}) # 한 페이지의 결과 수

    request = urllib.request.Request(url + unquote(queryParams))
    request.get_method = lambda: 'GET' # GET 방식으로 요청
    response_body = urlopen(request).read()
    soup = BeautifulSoup(response_body, 'html.parser')
#         print(soup.prettify()) # 전체  출력
    items = soup.find_all('item')

    for item in items:
        mongo = pyMongo()
        res = mongo.add_info_one(item)
        
        count += 1;
        print("\t국회의원 현황 조회\t", count, "개 입력중...")
        
        deptcd = item.find('deptcd').text
        num = item.find('num').text
        
        # 국회의원 상세정보 조회
        detail_url = "http://apis.data.go.kr/9710000/NationalAssemblyInfoService/getMemberDetailInfoList"
        detail_queryParams = '?' + urlencode({quote_plus('servicekey') : serviceKey, # 서비스 키
                                                quote_plus('dept_cd') : deptcd,
                                                quote_plus('num') : num}) # 부서코드
        
        detail_request = urllib.request.Request(detail_url + unquote(detail_queryParams))
        detail_request.get_method = lambda: 'GET' # GET 방식으로 요청
        detail_request_body = urlopen(detail_request).read() # 웹에 있는 소스 가져오기
        detail_soup = BeautifulSoup(detail_request_body, 'html.parser')
        detail_items = detail_soup.find_all('item')
        
        for detail_item in detail_items:
            res = mongo.add_detail_one(detail_item, deptcd, num)
            print("\t국회의원 상세정보 조회\t", count, "개 입력중...")
    print("입력 완료!")



class pyMongo(object):
    def __init__(self):
        self.client = MongoClient()
        self.db = self.client['jcjc']
        
    def add_info_one(self, item):
        # 데이터 삽입
        row = {} # Dict 객체 생성
        for col in item:
            row[col.name] = col.text # Dict 객체에 key, value 추가
        
        self.db.politicianInfo.insert_one(row) # politicianInfo 컬렉션에 추가

    def add_detail_one(self, item, deptcd, num):
        # 데이터 삽입
        row = {} # Dict 객체 생성
        row['deptcd'] = deptcd
        row['num'] = num
        
        for col in item:
            row[col.name] = col.text # Dict 객체에 key, value 추가
            
        self.db.politicianDetail.insert_one(row) # politicianDetail 컬렉션에 추가
    
if __name__ == '__main__':
    getPoliticianInfo()
    
