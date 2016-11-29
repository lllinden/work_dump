# -*- coding:utf-8 -*-  
from pyspark.sql import HiveContext
import pyspark.sql.functions as func
import datetime
import sys
import pandas as pd
from pyspark import SparkContext 
import sys
reload(sys)
sys.setdefaultencoding("UTF-8")




def student_feature(dt):
    # insert overwrite table model.student_feature partition('{dt_str}')
    sql = """
    insert overwrite table model.student_feature partition(dt='{dt_str}')
    select
        student.student_id,
        category.A17ZYSPG,
        category.AfentiChinese,
        category.AfentiExam,
        category.AfentiMath,
        category.GreatAdventure,
        category.PetsWar,
        category.SanguoDmz,
        category.Stem101,
        category.TravelAmerica,
        category.UsaAdventure,
        category.Walker,
        category.WalkerElf,
        category.WukongPinyin,
        category.WukongShizi,
        category.category_sum,
        hw_score.c_score as chinese_hw_score,
        hw_score.m_score as math_hw_score,
        hw_score.e_score as english_hw_score,
        hw_score.c_count as chinese_hw_finish_num,
        hw_score.m_count as math_hw_finish_num,
        hw_score.e_count as english_hw_finish_num,
        hw_day.m_hw_day as math_hw_day,
        hw_day.c_hw_day as chinese_hw_day,
        hw_day.e_hw_day as english_hw_day,
        hw_day.m_hw_time as math_hw_num,
        hw_day.c_hw_time as chinese_hw_num,
        hw_day.e_hw_time as english_hw_num,
        afenti_exam.act_day as afenti_exam_act_day,
        afenti_exam.hw_sum as afenti_exam_hw_num,
        afenti_exam.right_sum as afenti_exam_right_num,
        afenti_exam.correctness as afenti_exam_correctness,
        afenti_math.act_day as afenti_math_act_day,
        afenti_math.hw_sum as afenti_math_hw_num,
        usa.act_day as usa_act_day,
        usa_status.gold_num,
        usa_status.ticket_num,
        usa_play.combat_num,
        usa_play.pass_num,
        usa_play.form_num
    from
    (select 
        distinct student_id
    from 
        vbawork_business.primary_student_status
    where 
        first_hw_time <= '{dt_str}'
    )student
    
    left join
    
    (select 
        user_id,
        sum(if(appkey ='A17ZYSPG',1,0)) A17ZYSPG,
        sum(if(appkey ='AfentiChinese',1,0)) AfentiChinese,
        sum(if(appkey ='AfentiExam',1,0)) AfentiExam,
        sum(if(appkey ='AfentiMath',1,0)) AfentiMath,
        sum(if(appkey ='GreatAdventure',1,0)) GreatAdventure,
        sum(if(appkey ='PetsWar',1,0)) PetsWar,
        sum(if(appkey ='SanguoDmz',1,0)) SanguoDmz,
        sum(if(appkey ='Stem101',1,0)) Stem101,
        sum(if(appkey ='TravelAmerica',1,0)) TravelAmerica,
        sum(if(appkey ='UsaAdventure',1,0)) UsaAdventure,
        sum(if(appkey ='Walker',1,0)) Walker,
        sum(if(appkey ='WalkerElf',1,0)) WalkerElf,
        sum(if(appkey ='WukongPinyin',1,0)) WukongPinyin,
        sum(if(appkey ='WukongShizi',1,0)) WukongShizi,
        count(distinct appkey) as category_sum
    from 
        vbawork_business.vb_primary_student_r_travelamerica_status
    where 
        dt = '{dt_str}'
    group by user_id
    ) category
    on category.user_id = student.student_id
    
    left join
    (select
        studentid, 
        avg(if(subjectid=101,avgscore,null)) as c_score,  
        avg(if(subjectid=102,avgscore,null)) as m_score, 
        avg(if(subjectid=103,avgscore,null)) as e_score, 
        count(if(subjectid=101,1,null)) as c_count,  
        count(if(subjectid=102,1,null)) as m_count, 
        count(if(subjectid=103,1,null)) as e_count  
    from 
        fdm.fdm_mongo_student_newhomework_chain 
    where 
        dp='ACTIVE' 
        and homeworkcreateat >=date_sub('{dt_str}',180)  
        and homeworkcreateat <='{dt_str}' 
    group by studentid
    ) hw_score
    on hw_score.studentid = student.student_id

    left join
    (select 
        student_id, 
        sum(if(tab1.subject = 'CHINESE', tab1.days, 0)) c_hw_day, 
        sum(if(tab1.subject = 'MATH', tab1.days, 0)) m_hw_day, 
        sum(if(tab1.subject = 'ENGLISH', tab1.days, 0)) e_hw_day,
        sum(if(tab1.subject = 'CHINESE', tab1.time, 0)) c_hw_time, 
        sum(if(tab1.subject = 'MATH', tab1.time, 0)) m_hw_time, 
        sum(if(tab1.subject = 'ENGLISH', tab1.time, 0)) e_hw_time 
    from 
        (select 
            student_id, 
            subject, 
            count(distinct dt) as days,
            count(distinct homework_id) as time
        from 
            adm.adm_student_done_homework_record 
        where 
            dt>=date_sub('{dt_str}',180) and dt <='{dt_str}' 
        group by
            student_id, subject
        ) tab1
    group by student_id
    ) hw_day
    on hw_day.student_id = student.student_id
    
    left join
    (select 
        distinct user_id,
        act_day,
        hw_sum,
        right_sum,
        right_sum/hw_sum correctness
    from
        (select 
            user_id,
            count(dt) act_day,
            sum(result_num) hw_sum,
            sum(right_num) right_sum
        from 
            vbawork_business.vb_primary_student_afenti_exam_day
        where 
            dt >='2016-05-30'  and dt <='{dt_str}'
            and user_id is not null
        group by user_id
        ) t
    ) afenti_exam
    on afenti_exam.user_id = student.student_id
    
    left join
    (select 
        user_id,
        count(dt) act_day,
        sum(result_num) hw_sum
    from 
        vbawork_business.vb_primary_student_afentimath_exam_day
    where 
        dt >=date_sub('{dt_str}',180)  and dt <='{dt_str}'
        and user_id is not null
    group by user_id
    ) afenti_math
    on afenti_math.user_id = student.student_id

    left join
    (select 
        userid,
        count(distinct dt) act_day
    from  
        vbawork_base.vender_apps_log
    where 
        dt >=date_sub('{dt_str}',180) and dt <='{dt_str}'
        and appkey='UsaAdventure'
    group by userid
    ) usa
    on usa.userid = student.student_id
    
    left join
    (select 
        distinct user_id,
        regexp_extract(get_json_object(wealth,'$.gold'),':\\"(.*)\\"',1) gold_num,
        regexp_extract(get_json_object(wealth,'$.ticket'),':\\"(.*)\\"',1) ticket_num
    from 
        fdm.fdm_mongo_player_profile_chain
     where 
        dp='ACTIVE'
    ) usa_status
    on usa_status.user_id = student.student_id

    left join
    (select 
        player_id,
        sum(if(comment like '%竞技%',1,0)) combat_num,
        sum(if(comment like '%闯关%',1,0)) pass_num,
        sum(if(comment like '%形象%',1,0)) form_num
    from 
        fdm.fdm_mongo_player_wealth_history_chain
    where 
        dp='ACTIVE'
        and to_date(created_at)>=date_sub('{dt_str}',180)  and to_date(created_at) <='{dt_str}' 
    group by player_id
    ) usa_play 
    on usa_play.player_id = student.student_id 
    """.format(dt_str = dt)
    return sql    


if __name__ == "__main__":       
    sc = SparkContext(appName="student_feature_spark")
    sqlContext = HiveContext(sc)
    for i in range(1):
        dt = datetime.datetime.today() - datetime.timedelta(i+1)
        dt_str = dt.strftime("%Y-%m-%d")
        if dt_str >= '2016-10-27':
            student_feature = sqlContext.sql(student_feature(dt_str))
            # student_feature.write.parquet('./model_feature/student_feature_'+dt_str)
            
            




