# -*- coding:utf-8 -*-  
from pyspark.sql import HiveContext
import datetime
from pyspark import SparkContext 
import sys
reload(sys)
sys.setdefaultencoding("UTF-8")




if __name__ == '__main__':
    sc =  SparkContext(appName="oral_score")
    sqlContext = HiveContext(sc)
    # dt_str = '2016-09-25'

    
    for i in range(1):
        dt =  datetime.datetime.today() - datetime.timedelta(i+1)
        dt_str = dt.strftime("%Y-%m-%d")
        if dt_str > '2016-10-01':
            oral_sql = """
            insert overwrite table model.oral_score partition(dt='{dt}')
            select score.*, question.text
            from
                (select 
                    userid, practiceid, categoryid, questiondocid, 
                    regexp_extract(oraldetails, 'integrity(=|\\":)(.*?)\\,', 2) as integrity, 
                    regexp_extract(oraldetails, 'pronunciation(=|\\":)(.*?)\\,', 2) as pronunciation, 
                    regexp_extract(oraldetails, 'fluency(=|\\":)(.*?)\\,', 2) as fluency, 
                    regexp_extract(oraldetails, 'macScore(=|\\":)(.*?)\\,',2) as macscore,
                    regexp_extract(oraldetails, 'audio(=|\\":)(.*?)\\,',2) as audio
                from 
                    bdm.bdm_mongo_newhomework_process_result_new 
                where 
                    practiceid in (13,28,43,61,77,89,114) 
                    and dt = '{dt}'
                    and voicescoringmode = 'Normal'
                ) score
                left join 
                model.oral_question as question
                on question.doc_id = score.questiondocid
            where 
                text is not null
                and macscore >1
                and integrity > 1
                and fluency > 1
                and pronunciation > 1
        """.format(dt = dt_str)
        sqlContext.sql(oral_sql)

