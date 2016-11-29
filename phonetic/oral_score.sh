
source /etc/profile
source /home/model_user/.bash_profile

    
if [ -n "$1" ] ; then 
        dt=$1
    else
        dt=$(date  --date "1 days ago" +"%Y-%m-%d")
        ydt=$(date --date "2 days ago" +"%Y-%m-%d")
    fi
echo $dt
    

# question_sql="insert overwrite table model.oral_question partition(dt='$dt') select doc_id, practice_type_ids, text from (select distinct doc_id, practice_type_ids, get_json_object(options, '$.text') as text from (select doc_id, practice_type_ids, regexp_replace(options,'\\\[|\\\]','') as options from (select doc_id, practice_type_ids,get_json_object(content, '$.sub_contents.oral_dict.options') as options from fdm.fdm_mongo_xx_online_questions_tomato_chain where (practice_type_ids = '[13,28,43,61,77,89]' or practice_type_ids = '[114]') and dp='ACTIVE')a)aa)aaa where text is not null"


# echo $question_sql
# hive -e "$question_sql"
# hive -e "use model; alter table oral_question drop partition(dt = '$ydt');"







spark-submit --master yarn-client /home/xinyu.wang/user_value/live/ustalk/oral/oral_score.py


