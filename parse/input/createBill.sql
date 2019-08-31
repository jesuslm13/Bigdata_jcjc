# 기존에 있는 bill 테이블 삭제
# drop table bill;

# bill 테이블 생성
create table bill(
   bill_no varchar2(1000) primary key,
   bill_name varchar2(1000),
   politician_no number,
   proposer varchar2(1000),
   proposer_hj varchar2(10),
   proposer_kind varchar2(1000),
   propose_dt date,
   submit_dt date,
   committee_name varchar2(1000),
   proc_dt date,
   general_result varchar2(1000),
   summary CLOB
);


# bill 테이블 조회
select * from bill;

select * from bill where proposer = '김성태' or proposer = '최경환' 
select * from bill where proposer_kind = '의원' and politician_no is null;

### 

select count(*) from bill where politician_no is not null
select count(*) from bill where politician_no is null

select * from bill where politician_no is not null
select * from bill where politician_no is null and proposer_kind = '의원'

select proposer, instr(proposer, '의원') from bill where proposer_kind = '의원';
select proposer, substr(proposer, 0, instr(proposer, '의원')-1) as "politician_kor_name" from bill where politician_no is null and proposer_kind = '의원' and rownum <= 5;



select * from bill where proposer = '이동섭'

