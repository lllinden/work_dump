from pyspark.sql import HiveContext 
import pyspark.sql.functions as func
import datetime
import sys
import pandas as pd
from pyspark import SparkContext 

def parent_log(dt):
    sql = """
    insert overwrite table model.parent_log_feature partition(dt='{dt_str}')
    select 
        userid, sum(flower) as flower, sum(artical) as artical, sum(walkman) as walkman
    from 
        (select 
            userid, 
            count(if (op in ('math_done_flower','en_done_flower','class_chat_new_flower_click'), 1, null)) as flower,
            count(if (op in ('o_3S1az3Yx','o_cMuQDIW0'), 1, null)) as artical,
            count(if (op in ('o_7Rt7UDIv', 'o_f5o8EwRV', 'o_p3bJAcfv', 'o_XQMBZ7v0', 'o_2rLJ1sSA','o_7Rt7UDIv', 'o_g8tKpVab', 'o_9xSoF4fp', 'o_8Buic6BH', 'o_8Buic6BH', 'o_0nZdJeML', 'o_jyVQtQfi', 'o_evLqCAFq'), 1, null)) as walkman
        from 
            vox_logs.normal_adr_parent 
        where 
            dt ='{dt_str}'
        group by 
            userid 
    
        union all 
    
        select 
            userid,
            count(if (op in ('math_done_flower','en_done_flower','class_chat_new_flower_click'), 1, null)) as flower,
            count(if (op in ('o_3S1az3Yx','o_cMuQDIW0'), 1, null)) as artical,
            count(if (op in ('o_7Rt7UDIv', 'o_f5o8EwRV', 'o_p3bJAcfv', 'o_XQMBZ7v0', 'o_2rLJ1sSA','o_7Rt7UDIv', 'o_g8tKpVab', 'o_9xSoF4fp', 'o_8Buic6BH', 'o_8Buic6BH', 'o_0nZdJeML', 'o_jyVQtQfi', 'o_evLqCAFq'), 1, null)) as walkman
        from 
            vox_logs.normal_ios_parent 
        where 
            dt='{dt_str}'
        group by 
            userid
    )a
    group by userid
    """.format(dt_str = dt)
    return sql    


if __name__ == "__main__":       
    sc = SparkContext(appName="parent_log_feature_spark")
    sqlContext = HiveContext(sc)
    for i in range(1):
        dt = datetime.datetime.today()- datetime.timedelta(i+1)
        dt_str = dt.strftime("%Y-%m-%d")
        # if dt_str >= '2016-11-02':
        parent_flower_artical_walkman = sqlContext.sql(parent_log(dt_str))
            
            



