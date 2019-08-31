# 기존에 있는 politician 테이블 삭제
# drop table politician;

# politician 테이블 생성
create table politician(
	politician_no number,
	politician_num number,
	politician_kor_name varchar2(100) not null,
	politician_hj_name varchar2(100) not null,
	politician_eng_name varchar2(100) not null,
	bth_date date,
	poly_name varchar2(100),
	orig_name varchar2(100),
	shrt_name varchar2(500),
	reele_gbn_name varchar2(100),
	election_name varchar2(100),
	assem_tel varchar2(100),
	assem_homep varchar2(100),
	assem_email varchar2(100),
	hbby_cd varchar2(100),
	exam_cd varchar2(100),
	politician_jpg_link varchar2(1000)
);

# politician_no를 PK로 설정
alter table politician modify politician_no primary key;

# politician 테이블의 제약조건 조회
select * from ALL_CONSTRAINTS where TABLE_NAME = 'POLITICIAN'; 

# 제약조건 삭제
alter table politician drop constraint SYS_C004004

# 윤나래 추가
insert into politician
values(0, 0, '윤나래','尹나래','lydia yoon',
'1993-04-28', '무소속', 'ajax마스터', '엔코아',
'당선횟수', '당선대수', 
'010-6311-4096', 'https://github.com/LydiaYoon', 'narae456@gmail.com',
'400 Bad Request', '404 Not Found',
'https://pbs.twimg.com/media/DIAnF29VoAACic1.jpg');

# 윤나래 확인
select * from politician where politician_kor_name = '유승민'

# politician 테이블 조회
select * from politician

# 高榕禛
update politician set politician_hj_name = '고용진' where politician_kor_name = '고용진'

# 동명이인
select * from politician where politician_kor_name = '김성태' or politician_kor_name = '최경환'

select politician_no, politician_kor_name, politician_hj_name, politician_eng_name from politician where politician_kor_name = '김성태' or politician_kor_name = '최경환'



select * from politician where politician_kor_name = '원유철'

