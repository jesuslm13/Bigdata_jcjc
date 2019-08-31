# -*- coding:utf-8 -*-

import os
import cx_Oracle
from pymongo import MongoClient
import pymongo
from urllib.request import urlopen
from urllib.parse import urlencode, unquote, quote_plus
import urllib
from bs4 import BeautifulSoup


# oracle의 쿼리 결과를 Dict로 받기위한 메서드
def makeDictFactory(cursor):
    columnNames = [d[0] for d in cursor.description]
    def createRow(*args):
        return dict(zip(columnNames, args))
    return createRow



class inputNo() :
    os.environ["NLS_LANG"] = ".AL32UTF8" # UTF-8 : .AL32UTF8, CP949 : .KO16MSWIN949
    con = pymongo.MongoClient("localhost", 27017)['jcjc']
    conn = cx_Oracle.connect('bigdata/admin1234@localhost:1521/xe') # oracle 서버와 연결 (connection 맺기)
    
    def input(self):
    
        serviceKey = "Qdb5KydABzjhFWA4CzQ4gSgtLMnxo6C5jGrv%2FOLaQ6evALcjMQDkPllXowGQzr9DzraCGymtgDwuQmge6QJzng%3D%3D"

        print(self.conn.version) # connection 확인
        oracle = self.conn.cursor() # cursor 객체 얻어오기
         
        # 1. oracle의 politician 테이블에서 동명이인의 한자이름을 가져온다. (tuple)
        sql_select = """select *
        from politician
        where politician_kor_name = :name1
        or politician_kor_name = :name2"""
         
        oracle.execute(sql_select,
                               name1 = '김성태',
                               name2 = '최경환')
        
        oracle.rowfactory = makeDictFactory(oracle) # cursor의 rowfactory 메서드를 오버라이딩하여 리턴받는 데이터의 형태를 바꿀 수 있음
        items = oracle.fetchall() 
                
        for item in items:
            print("item :\t", item)
            pno = item['POLITICIAN_NO'] # 정치인 번호
            kr_name = item['POLITICIAN_KOR_NAME'] # 한글 이름
            hj_name = item['POLITICIAN_HJ_NAME'] # 한자 이름
            
            # 2. API에서 BillInfo를 한자이름으로 요청한다.
            # url과   parameter 값 설정
            url = "http://apis.data.go.kr/9710000/BillInfoService2/getBillInfoList"
                             
            # totalCount를 찾기 위해 임시로 파싱
            queryParams = '?' + urlencode({
                                        quote_plus('serviceKey') : serviceKey, # 서비스 키
                                        quote_plus('pageNo') : '1', # 페이지 번호
                                        quote_plus('numOfRows') : '1', # 한 페이지의 결과 수
                                        quote_plus('gbn') : 'dae_num_name', # 제안대수 검색 - 이름 포함
                                        quote_plus('mem_name_check') : 'G01', # 발의자 검색구분 (G10: 대표발의)
                                        quote_plus('ord') : 'A01', # 제안대수(A01)로 검색
                                        quote_plus('start_ord') : '20', # 시작 대수
                                        quote_plus('end_ord') : '20', # 마지막 대수
                                        quote_plus('proposer_kind_cd') : 'F01' # 제안종류 (F10: 의원)
                                        })
             
            queryParams2 = '&' + urlencode({
                                        quote_plus('mem_name') : kr_name, # 한글 이름
                                        quote_plus('hj_nm') : hj_name, # 한자 이름
                                        })
             
            request = urllib.request.Request(url + unquote(queryParams) + queryParams2)
            request.get_method = lambda: 'GET' # GET 방식으로 요청
            request_body = urlopen(request).read() # 웹에 있는 소스 가져오기
            soup = BeautifulSoup(request_body, 'html.parser')
             
            totalCount = int(soup.totalcount.text) # 전체 결과 수
            numOfRows = 100 # 한페이지의 결과 수
            pageSize = int(totalCount/numOfRows) + 1 # 페이지 수
            print('totalCount :', totalCount)
             
            
            count = 0
                        
            # 전체 개수만큼 파싱
            for i in range(pageSize):
                queryParams = '?' + urlencode({
                                            quote_plus('serviceKey') : serviceKey, # 서비스 키
                                            quote_plus('pageNo') : i+1, # 페이지 번호
                                            quote_plus('numOfRows') : numOfRows, # 한 페이지의 결과 수
                                            quote_plus('gbn') : 'dae_num_name', # 제안대수 검색 - 이름 포함
                                            quote_plus('mem_name_check') : 'G01', # 발의자 검색구분 (G10: 대표발의)
                                            quote_plus('ord') : 'A01', # 제안대수(A01)로 검색
                                            quote_plus('start_ord') : '20', # 시작 대수
                                            quote_plus('end_ord') : '20', # 마지막 대수
                                            quote_plus('proposer_kind_cd') : 'F01' # 제안종류 (F10: 의원)
                                            })
                 
                queryParams2 = '&' + urlencode({
                                            quote_plus('mem_name') : kr_name, # 한글 이름
                                            quote_plus('hj_nm') : hj_name, # 한자 이름
                                            })
              
                request = urllib.request.Request(url + unquote(queryParams) + queryParams2)
                request.get_method = lambda: 'GET' # GET 방식으로 요청
                request_body = urlopen(request).read() # 웹에 있는 소스 가져오기
                soup = BeautifulSoup(request_body, 'html.parser')
                 
                info_items = soup.find_all('item')
                 
                for info_item in info_items:
                    count += 1
                    bno = info_item.billno.text
                    print("\t>>>\t", count, "\t", pno, "\t", kr_name, "\t", hj_name, "\t", bno)
                     
                    # bill 테이블 update
                    sql_update = """update bill
                    set
                    politician_no = :politician_no,
                    proposer = :proposer,
                    proposer_hj = :proposer_hj
                    where
                    bill_no = :bill_no"""
                     
                    oracle.execute(sql_update,
                                   politician_no = pno,
                                   proposer = kr_name,
                                   proposer_hj = hj_name,
                                   bill_no = bno
                                   )
                     
            print("====================================================================================================")
            oracle.commit()
        
        print("update 완료")
        
if __name__ == '__main__':
    obj = inputNo()
    obj.input()
    
