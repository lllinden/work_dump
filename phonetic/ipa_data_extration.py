from __future__ import print_function

from pyspark.sql import SparkSession
import sys
import datetime

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ipa_data_extration.py start-date end-date e.g. ipa_data_extration.py 2016-03-01 2016-04-01 ")
        sys.exit(-1)

    start_date_str = datetime.datetime.strptime(sys.argv[1], "%Y-%m-%d")
    end_date_str = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d")
    

    spark = SparkSession.builder.appName("Get content of student").enableHiveSupport().getOrCreate()

    sql = """
        select userid,get_json_object(his_info,'$.content') as content, get_json_object(his_info,'$.score') as score, get_json_object(his_info, '$.voiceUrl') as url from (select userid, split(regexp_replace(substr(detail,2,length(trim(detail)) - 2),'\\\}},\\\{{','\\\}}###\\\{{'),'###') his from fdm.fdm_mongo_english_homework_result_detail where dt>='{start_date}' and dt <='{end_date}' and practiceId in('13','28','43','61','77','89','115')) t lateral view explode(his) his_json as his_info
    """.format(start_date=start_date_str, end_date=end_date_str)
    print(sql)
    content_df = spark.sql(sql)
    content_df.select("content").distinct().coalesce(1).write.csv("temp_csv")
    spark.stop()
