
from pyspark.sql import HiveContext 
import pyspark.sql.functions as func
import datetime
import sys
import pandas as pd
from pyspark import SparkContext 


def parent_other(dt, y):
    sql="""
    insert overwrite table model.parent_other partition(dt='{dt_str}')
    select
        distinct parent.parent_id,
        wechat.wechat_act,
        app.app_act,
        mobile.brand
    from 
        (select 
            user_id as parent_id
        from
            vbawork_business.vb_parent_status
        where
            dt = '{y_str}'
            and min_login_day_all <= '{dt_str}'
        )parent
        left join
        (select
            user_id,
            sum(if(is_active=1,1,0)) as wechat_act
        from 
            vbawork_business.vb_parent_active_day 
        where 
            dt>=date_sub('{dt_str}',180) and dt <='{dt_str}'
            and client_type='wechat'
        group by
            user_id
        )wechat 
        on wechat.user_id = parent.parent_id
        left join
        (select 
            user_id,
            sum(if(is_active=1,1,0)) as app_act
        from 
            vbawork_business.vb_parent_active_day
        where 
            dt>=date_sub('{dt_str}',180) and dt <='{dt_str}' 
            and client_type ='app'
        group by 
            user_id
        )app
        on app.user_id =parent.parent_id
        left join
        (select 
            distinct a.user_id,
            b.brand
        from
            (select
                distinct user_id,
                model
            from
                vbawork_business.vb_parent_basic_info_app
            where 
                dt ='{dt_str}'  
            )a
            left join
            (select 
                *
            from
                vbawork_business.vb_dict_phone_brand
            )b
            on a.model = b.model
        )mobile
        on mobile.user_id = parent.parent_id
    """.format(dt_str = dt, y_str = y)
    print sql
    return sql

if __name__ == "__main__":
    sc = SparkContext(appName="parent_feature_other_spark")
    sqlContext = HiveContext(sc)
    y = datetime.datetime.today() - datetime.timedelta(1)
    y_str = y.strftime("%Y-%m-%d")
    for i in range(1):
            dt =  datetime.datetime.today() - datetime.timedelta(i+1)
            dt_str = dt.strftime("%Y-%m-%d")
            parent_wechat_app_mobile = sqlContext.sql(parent_other(dt_str, y_str))
