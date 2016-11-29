###从口语数据中，提取所有单词读音，保留单词内容和userid
content = sqlContext.sql("select userid,get_json_object(his_info,'$.content') as content, get_json_object(his_info,'$.score') as score, get_json_object(his_info, '$.voiceUrl') as url from (select userid, split(regexp_replace(substr(detail,2,length(trim(detail)) - 2),'\\\},\\\{','\\\}###\\\{'),'###') his from fdm.fdm_mongo_english_homework_result_detail where dt='2016-06-01' and practiceId in('13','28','43','61','77','89','115')) t lateral view explode(his) his_json as his_info")


content = sqlContext.sql("userid, text, pronunciation, audio from model.oral_score where dt ='2016-11-23")
content.registerTempTable('content_selected')

###筛选评分低于80的记录
# content_select = content.filter(content.score <60)
# content_select.registerTempTable('content_selected')

###引入IPA_Converting的函数
sc.addPyFile("/home/xinyu.wang/user_value/live/ustalk/kk/gen_mods.py")
sc.addPyFile("/home/xinyu.wang/user_value/live/ustalk/kk/main.py")
sc.addPyFile("/home/xinyu.wang/user_value/live/ustalk/kk/CMU_phonetic_dictionary.txt")


from gen_mods import *
#sqlContext.registerFunction("get_kk", get_kk)
#result = sqlContext.sql('select content_selected.*, get_kk(content) as kk from content_selected')

words_list=content_select.select('content').distinct().collect()
word_phonetic_list=[]
i = 0
while i < len(words_list):
    word_phonetic_list.append((words_list[i].content,get_kk(words_list[i].content)))
    i += 1
word_phonetic_df = sqlContext.createDataFrame(word_phonetic_list,['word','phonetic'])




