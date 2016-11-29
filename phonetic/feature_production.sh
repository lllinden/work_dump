#!/usr/bin/env bash

source /etc/profile
source /home/model_user/.bash_profile

    
if [ -n "$1" ] ; then 
    dt=$1
else
    dt=$(date  --date "1 days ago" +"%Y-%m-%d")
fi
hadoop fs -rm -r /user/xinyu.wang/temp_model_yesterday
hadoop fs -cp /user/xinyu.wang/temp_model /user/xinyu.wang/temp_model_yesterday
hadoop fs -rm -r /user/xinyu.wang/temp_model
hadoop fs -mkdir /user/xinyu.wang/temp_model

echo $dt



sql_click="select user_id as click_parent_id, sum(if(mess_tab_click is null,0,mess_tab_click)) mess_tab_sum, sum(if(grow_tab_click is null,0,grow_tab_click)) grow_tab_sum, sum(if(study_tab_click is null,0,study_tab_click)) study_tab_sum, sum(if(edu_tab_click is null,0,edu_tab_click)) edu_tab_sum, sum(if(chat_click is null,0,chat_click)) chat_sum, sum(if(edu_artical_click is null,0,edu_artical_click)) edu_article_sum, sum(if(select_artical_click is null,0,select_artical_click)) select_article_sum, sum(if(sys_mess_click is null,0,sys_mess_click)) sys_mess_sum, sum(if(growth_task_click is null,0,growth_task_click)) growth_task_sum, sum(if(develop_click is null,0,develop_click)) develop_sum from vbawork_business.vb_parent_app_click where dt>=date_sub('$dt',180) and dt<=to_date('$dt') group by user_id"
echo $sql_click    
hive -e "$sql_click" > '/home/xinyu.wang/model_feature/temp/parent_click.csv'
sed -i "1i click_parent_id\tmess_tab_sum\tgrow_tab_sum\tstudy_tab_sum\tedu_tab_sum\tchat_sum\tedu_article_sum\tselect_article_sum\tsys_mess_sum\tgrowth_task_sum\tdevelop_sum" /home/xinyu.wang/model_feature/temp/parent_click.csv
hadoop fs -put /home/xinyu.wang/model_feature/temp/parent_click.csv /user/xinyu.wang/temp_model/

sql_log="select userid as log_parent_id, sum(flower) flower, sum(artical) artical, sum(walkman) walkman from model.parent_log_feature where dt >=date_sub('$dt', 180) and dt<=to_date('$dt') group by userid"
echo $sql_log
hive -e "$sql_log" > '/home/xinyu.wang/model_feature/temp/parent_log.csv'
sed -i "1i log_parent_id\tflower\tartical\twalkman" /home/xinyu.wang/model_feature/temp/parent_log.csv
hadoop fs -put /home/xinyu.wang/model_feature/temp/parent_log.csv /user/xinyu.wang/temp_model/

sql_other="select parent_id as other_parent_id, wechat_act as wechat_act_day, app_act as app_act_day from model.parent_other where dt=date_sub('$dt',1)"
echo $sql_other
hive -e "$sql_other" > '/home/xinyu.wang/model_feature/temp/parent_other.csv'
sed -i "1i other_parent_id\twechat_act_day\tapp_act_day" /home/xinyu.wang/model_feature/temp/parent_other.csv
hadoop fs -put /home/xinyu.wang/model_feature/temp/parent_other.csv /user/xinyu.wang/temp_model/parent_other.csv


sql_pay="select parent_id as pay_parent_id, sum(if(pay_amount is null,0,pay_amount)) as pay_amount from (select distinct b1.parent_id, pay_amount from ( select user_id, sum(if(pay_amount is null,0,pay_amount)) pay_amount from vbawork_business.vb_effective_order_day where dt<=to_date('$dt') and pay_amount>0.5 group by user_id) a1 left join ( select distinct parent_id, student_id from fdm.fdm_mysql_vox_student_parent_ref_chain where dp='ACTIVE' ) b1 on a1.user_id=b1.student_id union all select distinct b2.parent_id , pay_amount from ( select user_id, sum(if(pay_amount is null,0,pay_amount)) pay_amount from vbawork_business.vb_effective_order_day where dt<=to_date('$dt') and pay_amount>0.5 group by user_id) a2 left join ( select distinct id parent_id from fdm.fdm_mysql_uct_user_chain where dp='ACTIVE' and user_type=2 ) b2 on a2.user_id=b2.parent_id ) t where parent_id is not null and parent_id!='NULL' group by parent_id"
echo $sql_pay
hive -e "$sql_pay" > '/home/xinyu.wang/model_feature/temp/parent_pay.csv'
sed -i "1i pay_parent_id\tpay_amount" /home/xinyu.wang/model_feature/temp/parent_pay.csv
hadoop fs -put /home/xinyu.wang/model_feature/temp/parent_pay.csv /user/xinyu.wang/temp_model/

sql_article="select a.user_id parent_id, count(distinct a.article_id) article_sum, count(distinct if(c.tag_name='数学',a.article_id,null)) math_article_sum, count(distinct if(c.tag_name='语文',a.article_id,null)) che_article_sum, count(distinct if(c.tag_name='英语',a.article_id,null)) eng_article_sum, count(distinct if(c.tag_name='亲子沟通',a.article_id,null)) family_article_sum, count(distinct if(c.tag_name='教育话题',a.article_id,null)) educ_article_sum, count(distinct if(c.tag_name='升学考试',a.article_id,null)) exam_article_sum,     count(distinct if(c.tag_name='兴趣培养',a.article_id,null)) intrest_article_sum, avg(if(c.tag_name='数学'and a.duration is not null,a.duration,null)) math_avg_duration, avg(if(c.tag_name='语文'and a.duration is not null,a.duration,null)) che_avg_duration, avg(if(c.tag_name='英语'and a.duration is not null,a.duration,null)) eng_avg_duration, avg(if(c.tag_name='亲子沟通' and a.duration is not null, a.duration,null)) family_avg_duration, avg(if(c.tag_name='教育话题' and a.duration is not null,a.duration,null)) educ_avg_duration, avg(if(c.tag_name='升学考试' and a.duration is not null,a.duration,null)) exam_avg_duration, avg(if(c.tag_name='兴趣培养' and a.duration is not null,a.duration,null)) intrest_avg_duration, sum(if(a.help_num is not null,a.help_num,0)) help_sum, sum(if(a.no_help_num is not null,a.no_help_num,0)) no_help_sum, sum(if(a.share is not null, a.share,0))+sum(if(a.transmit_num is not null,a.transmit_num,0)) share_sum, sum(if(a.comment_num is not null,a.comment_num,0)) comment_sum, sum(if(a.more_click is not null,a.more_click,0)) more_click_sum, sum(if(a.unfold_click is not null,a.unfold_click,0)) unfold_click_sum, sum(if(a.praise_num is not null,a.praise_num,0))praise_sum from( select * from vbawork_business.vb_parent_article_click_new where dt>='2016-09-01' and dt<=to_date('$dt')) a left join( select distinct id, title, tagid['\$numberLong'] tag_id from fdm.fdm_mongo_vox_jxt_news_chain lateral view explode(from_json(taglist,'array<map<string,string>>')) t22 as tagid where dp='ACTIVE')b on a.article_id=b.id left join( select id,tag_name from fdm.fdm_mysql_vox_jxt_news_tag_chain where dp='ACTIVE') c on b.tag_id=c.id group by a.user_id"
echo $sql_article
hive -e "$sql_article" > '/home/xinyu.wang/model_feature/temp/parent_article.csv'
sed -i "1i parent_id\tarticle_sum\tmath_article_sum\tche_article_sum\teng_article_sum\tfamily_article_sum\teduc_article_sum\texam_article_sum\tintrest_article_sum\tmath_avg_duration\tche_avg_duration\teng_avg_duration\tfamily_avg_duration\teduc_avg_duration\texam_avg_duration\tintrest_avg_duration\thelp_sum\tno_help_sum\tshare_sum\tcomment_sum\tmore_click_sum\tunfold_click_sum\tpraise_sum"  /home/xinyu.wang/model_feature/temp/parent_article.csv
hadoop fs -put /home/xinyu.wang/model_feature/temp/parent_article.csv /user/xinyu.wang/temp_model/

