regist = sqlContext.sql("select count(distinct student_id), to_date(create_datetime) as create_date from fdm.fdm_mysql_livecast_user_status_log_chain where create_datetime > '2016-09-01' and status = 1 group by to_date(create_datetime)")
distribute = sqlContext.sql("select count(distinct student_id), to_date(create_datetime) as create_date from fdm.fdm_mysql_livecast_user_distribute_log_chain where distribute_status = 2 and create_datetime >'2016-09-01' group by to_date(create_datetime)")


reg_dist = sqlContext.sql("select count(distinct student_id) from (select distinct dist.student_id from fdm.fdm_mysql_livecast_user_status_log_chain reg left join fdm.fdm_mysql_livecast_user_distribute_log_chain dist where reg.create_datetime > '2016-09-01' and reg.status= 1 and dist.create_datetime >'2016-09-01' and dist.distribute_status = 2 and dist.student_id is not null)")
