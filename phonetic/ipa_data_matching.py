# -*- coding:utf-8 -*-  

from pyspark.sql import HiveContext
from pyspark import SparkContext 
import datetime

if __name__ == "__main__":
    sc=SparkContext(appName="phonetic")
    sqlContext = HiveContext(sc)
    
    dt =  datetime.datetime.today() - datetime.timedelta(1)
    dt_str = dt.strftime("%Y-%m-%d")
    
    sql = """
    with summary as (
    select o.* , 
    row_number() over(partition by userid, text order by o.pronunciation) as rank
    from model.oral_score o 
    where dt>date_sub('{dt}',60) and dt < '{dt}'
    )
    select s.userid, s.text as content, s.audio as url, s.pronunciation as score
    from 
    summary as s
    where s.rank = 1
    """.format(dt=dt_str)

    content_df = sqlContext.sql(sql)
    content_df.registerTempTable("content")
    word_phonetic_df = sqlContext.read.json('./oral/question_phonetic.json')
    word_phonetic_df.registerTempTable("word_phonetic")

    user_portfolio_sql = """
        select userid, content, score, score, url, phonetic from content a inner join word_phonetic b on a.content = b.word
    """

    user_portfolio_df = sqlContext.sql(user_portfolio_sql)
    user_portfolio_df.cache().registerTempTable("user_portfolio")

    base_sql = """
        select * from (select userid, content, score, url, phonetic from content a inner join word_phonetic b on a.content = b.word) t lateral view explode(split(phonetic, '\\\|')) adTable as p
    """
    base_df = sqlContext.sql(base_sql)
    base_df.registerTempTable("base")

    summary_sql = """
        select userid, p, avg(score) as score_mean, min(score) as score_min from base group by userid, p
    """
    summary_df = sqlContext.sql(summary_sql)
    summary_df.registerTempTable("summary")
    
    result_sql = """
        insert overwrite table model.student_phonetic_test partition(dt='{dt}')
        select a.userid, a.p as phonetic, b.score_mean, b.score_min, a.content, a.url from base a inner join summary b on a.userid = b.userid and a.p = b.p and a.score = b.score_min
    """.format(dt=dt_str)
    sqlContext.sql(result_sql)
    sc.stop()
