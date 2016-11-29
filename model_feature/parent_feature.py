# -*- coding:utf-8 -*-  
from pyspark.sql import HiveContext
import datetime
from pyspark import SparkContext 
import sys
reload(sys)
sys.setdefaultencoding("UTF-8")

if __name__ == "__main__":       
    sc = SparkContext(appName="parent_feature_spark")
    sqlContext = HiveContext(sc)

    for i in range(1):
        dt = datetime.datetime.today() - datetime.timedelta(1)
        dt_str = dt.strftime("%Y-%m-%d")
        parent_id_sql = """
            select 
                user_id as u_parent_id 
            from  
                vbawork_business.vb_parent_active_month 
            where 
                client_type = 'wechat' or client_type = 'app' 
                and dt > date_sub('{dt}',180)
        """.format(dt = dt_str)
        parent_id = sqlContext.sql(parent_id_sql)
        parent_id.registerTempTable('training_student')
        parent_id.printSchema() 
        parent_click = sqlContext.read.format("com.databricks.spark.csv").options(header=True,delimiter='\t').load('/user/xinyu.wang/temp_model/parent_click.csv')
        parent_click.registerTempTable('parent_click')
        parent_log = sqlContext.read.format("com.databricks.spark.csv").options(header=True,delimiter='\t').load('/user/xinyu.wang/temp_model/parent_log.csv')
        parent_log.registerTempTable('parent_log')
        parent_other = sqlContext.read.format("com.databricks.spark.csv").options(header=True,delimiter='\t').load('/user/xinyu.wang/temp_model/parent_other.csv')
        parent_other.registerTempTable('parent_other')
        parent_pay = sqlContext.read.format("com.databricks.spark.csv").options(header=True,delimiter='\t').load('/user/xinyu.wang/temp_model/parent_pay.csv')
        parent_pay.registerTempTable('parent_pay')
        parent_artical = sqlContext.read.format("com.databricks.spark.csv").options(header=True,delimiter='\t').load('/user/xinyu.wang/temp_model/parent_article.csv')
        parent_artical.registerTempTable('parent_artical')

        hive_sql = """select training_student.u_parent_id, parent_click.*, parent_log.*, parent_other.*, parent_pay.*, parent_artical.* from training_student left join parent_click on parent_click.click_parent_id = training_student.u_parent_id left join parent_log on parent_log.log_parent_id = training_student.u_parent_id left join parent_other on parent_other.other_parent_id = training_student.u_parent_id left join parent_pay on parent_pay.pay_parent_id=training_student.u_parent_id left join parent_artical on parent_artical.parent_id = training_student.u_parent_id""".format(dt=dt_str)
        merged = sqlContext.sql(hive_sql)


        merged = merged.drop(merged.log_parent_id)
        merged = merged.drop(merged.click_parent_id)
        merged = merged.drop(merged.other_parent_id)
        merged = merged.drop(merged.pay_parent_id)
        merged = merged.drop(merged.parent_id)
        merged.printSchema()
        merged.registerTempTable('merged')
        hive_sql = """insert overwrite table model.parent_feature partition(dt='{dt}') select * from merged""".format(dt=dt_str)
        # print hive_sql 
        parent_feature = sqlContext.sql(hive_sql)
            
            

    
