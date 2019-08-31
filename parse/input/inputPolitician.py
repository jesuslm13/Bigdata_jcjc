# -*- coding:utf-8 -*-
from pymongo import MongoClient
import pymongo
import cx_Oracle
import os


class input():
    
    os.environ["NLS_LANG"] = ".AL32UTF8" # UTF-8 : .AL32UTF8, CP949 : .KO16MSWIN949
    con = pymongo.MongoClient("localhost", 27017)['jcjc']
    conn = cx_Oracle.connect('bigdata/admin1234@localhost:1521/xe') # oracle 서버와 연결 (connection 맺기)
    
    def inputPolitician(self):
        
        print(self.conn.version) # connection 확인
        oracle = self.conn.cursor() # cursor 객체 얻어오기

        count = 0
                 
        # 1. mongoDB의 politicianInfo 컬렉션을 조회한다.
        items = self.con['politicianInfo'].find()

        for item in items:
            
            deptcd = item['deptcd']
            num = item['num']
            
            # 2. 조회한 정보에서 deptcd와 num을 가지고
            #    mongoDB의 politicianDetail 컬렉션을 조회한다. 
            detail_items = self.con['politicianDetail'].find({"deptcd":deptcd, "num":num})
            for detail_item in detail_items:
            
                row = {} # Dict 객체 생성

                row['politician_no'] = item.get('deptcd')
                row['politician_num'] = item.get('num')
                row['politician_kor_name'] = item.get('empnm')
                row['politician_hj_name'] = item.get('hjnm')
                row['politiican_eng_name'] = item.get('engnm')
                
                row['bth_date'] = detail_item.get('bthdate')
                row['poly_name'] = detail_item.get('polynm')
                row['orig_name'] = detail_item.get('orignm')
                row['shrt_name'] = detail_item.get('shrtnm')
                
                row['reele_gbn_name'] = item.get('reelegbnnm')
                row['election_name'] = detail_item.get('electionnum')
                row['assem_tel'] = detail_item.get('assemtel')
                row['assem_homep'] = detail_item.get('assemhomep')
                row['assem_email'] = detail_item.get('assememail')
                
                row['hbby_cd'] = detail_item.get('hbbycd')
                row['exam_cd'] = detail_item.get('examcd')
                row['politician_jpg_link'] = item.get('jpglink')
                
                
                # oracle Insert
                sql_insert = """insert into politician
                (politician_no, politician_num, 
                politician_kor_name, politician_hj_name, politician_eng_name,
                bth_date, poly_name, orig_name, shrt_name,
                reele_gbn_name, election_name,
                assem_tel, assem_homep, assem_email,
                hbby_cd, exam_cd, politician_jpg_link)
                values(
                :politician_no, :politician_num, 
                :politician_kor_name, :politician_hj_name, :politician_eng_name,
                to_date(:bth_date, 'YYYYMMDD'), 
                :poly_name, :orig_name, :shrt_name,
                :reele_gbn_name, :election_name,
                :assem_tel, :assem_homep, :assem_email,
                :hbby_cd, :exam_cd, :politician_jpg_link)"""

                oracle.execute(sql_insert,
                        politician_no = row['politician_no'],
                        politician_num = row['politician_num'],
                        politician_kor_name = row['politician_kor_name'],
                        politician_hj_name = row['politician_hj_name'],
                        politiican_eng_name = row['politiican_eng_name'],
                        bth_date = row['bth_date'],
                        poly_name = row['poly_name'],
                        orig_name = row['orig_name'],
                        shrt_name = row['shrt_name'],
                        reele_gbn_name = row['reele_gbn_name'],
                        election_name = row['election_name'],
                        assem_tel = row['assem_tel'],
                        assem_homep = row['assem_homep'],
                        assem_email = row['assem_email'],
                        hbby_cd = row['hbby_cd'],
                        exam_cd = row['exam_cd'],
                        politician_jpg_link = row['politician_jpg_link']
                        )

                # mongoDB Insert
                self.con['politician'].insert_one(row)
                
                count += 1
                print(count, "\t", row)
        
        oracle.commit()
        print("==== finish ====")



if __name__ == '__main__':
    obj = input()
    obj.inputPolitician()