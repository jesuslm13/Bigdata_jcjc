# -*- coding:utf-8 -*-

import os
import cx_Oracle
from pymongo import MongoClient
import pymongo


class inputBill():
    os.environ["NLS_LANG"] = ".AL32UTF8" # UTF-8 : .AL32UTF8, CP949 : .KO16MSWIN949
    con = pymongo.MongoClient("localhost", 27017)['jcjc'] # mongoDB Client Connection
    conn = cx_Oracle.connect('bigdata/admin1234@localhost:1521/xe') # cx_Oracle Connection
    
    def input(self):
        
        print(self.conn.version) # cx_Oracle connection 확인
        oracle = self.conn.cursor() # cx_Oracle cursor 객체 얻어오기
        
        count = 0
             
        # 1. mongoDB의 passBill 컬렉션을 조회한다.
        items = self.con['passBill'].find()
          
        for item in items:
            print("item :\t", item)
            count += 1
             
            row = {} # Dict 객체 생성
            row['bill_no'] = item.get('billno')
            row['bill_name'] = item.get('billname')
            row['proposer'] = item.get('proposer')
            row['proposer_kind'] = item.get('proposerkind')
            row['propose_dt'] = item.get('proposedt')
            row['submit_dt'] = item.get('submit_dt')
            row['committee_name'] = item.get('committeename')
            row['proc_dt'] = item.get('procdt')
            row['general_result'] = item.get('generalresult')
     
             
            # 2. mongoDB의 billInfo 컬렉션에서 summary 정보를 가져온다.
            for info_item in self.con['billInfo'].find({"billno" : row['bill_no']}):
                row['summary'] = info_item.get('summary')

            
            # politician_no
            # mongoDB에 찾는 정치인 정보가 없을 수 있기 때문에 정치인번호와 한자이름을 None으로 초기화
            row['politician_no'] = '' 
            row['proposer_hj'] = ''
            
            index = row['proposer'].find('의원') # 제안자 컬럼에서 '의원'문자열의 인덱스 위치를 찾아서 저장 
            if row['proposer_kind'] == '의원' and index > -1: # 제안자 분류가 '의원'이고 제안자 컬럼에 '의원'문자열이 있을 경우
                kor_name = row['proposer'][:index] # 0번째 인덱스부터 '의원'문자열 앞까지 잘라서 저장
                
                if kor_name != '김성태' and kor_name != '최경환': # 동명이인(김성태, 최경환) 별도 처리하기 위해 제외
                    for no_item in self.con['politicianInfo'].find({"empnm": kor_name}):
                            row['politician_no'] = no_item.get('deptcd')
                            row['proposer'] = no_item.get('empnm')
                            row['proposer_hj'] = no_item.get('hjnm')
                            print(row['politician_no'], row['proposer'], row['proposer_hj'])
              
            row['analysisCheck'] = '0'
            print("row :\t", row)

                 
            # passBill query
            sql_insert = """insert into bill
            (bill_no, bill_name,
            politician_no, proposer, proposer_hj, proposer_kind,
            propose_dt, submit_dt, committee_name,
            proc_dt, general_result, 
            summary)
            values(
            :bill_no, :bill_name,
            :politician_no, :proposer, 
            :proposer_hj, :proposer_kind,
            to_date(:propose_dt, 'YYYY-MM-DD'), 
            to_date(:submit_dt, 'YYYY-MM-DD'), 
            :committee_name,
            to_date(:proc_dt, 'YYYY-MM-DD'), 
            :general_result,
            :summary)"""
             
            # passBill execute
            oracle.execute(sql_insert,
                            bill_no = row['bill_no'],
                            bill_name = row['bill_name'],
                            politician_no = row['politician_no'],
                            proposer = row['proposer'],
                            proposer_hj = row['proposer_hj'],
                            proposer_kind = row['proposer_kind'],
                            propose_dt = row['propose_dt'],
                            submit_dt = row['submit_dt'],
                            committee_name = row['committee_name'],
                            proc_dt = row['proc_dt'],
                            general_result = row['general_result'] ,
                            summary = row['summary']                          
                            )
            
            # mongoDB Insert
            self.con['bill'].insert_one(row)
            
            print(count, "\tpassBill insert\t")
            print("===========================================================================") 
             
                 
        print("passBill 입력 완료!")
        oracle.commit() # oracle commit
        

if __name__ == '__main__':
    obj = inputBill()
    obj.input()