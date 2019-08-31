from pyspark.sql import SQLContext, Row
from pyspark.sql.types import *


# HDFS에서 csv 파일을 가져온다.
rdd1 = sc.textFile("/jcjc/billAnalysis/9771080.csv")​

# 첫번재 줄을 header로 지정한다.
headr1 = rdd1.first()

# header를 제외한 나머지 라인을 가져와서 구분자(,)로 나눈다.
rdd1 = rdd1.filter(lambda line: line != headr1).map(lambda line: line.split(","))

# 첫번째 열을 제외한 나머지 열을 float로 캐스팅한 후 List 형식으로 만든다.
rdd1 = rdd1.map(lambda line: [line[0], [float(x) for x in line[1:]]])

# 첫번재 열을 제외한 나머지 열의 합계를 구한다.
rdd1 = rdd1.map(lambda line: [line[0], sum(line[1][0:])])

# 단어 수가 많은 것 부터 내림차순으로 정렬한다.
rdd1 = rdd1.sortBy(lambda x: x[1], ascending=False)​
​

# 위와 같은 코드
rdd2 = sc.textFile("/jcjc/billAnalysis/9770592.csv")
headr2 = rdd2.first()
rdd2 = rdd2.filter(lambda line: line != header2).map(lambda line: line.split(",")).map(lambda line: [line[0], [float(x) for x in line[1:]]]).map(lambda line: [line[0], sum(line[1][0:])]).sortBy(lambda x: x[1], ascending=False)​​
​​
