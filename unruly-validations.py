#!/usr/bin/env python
import os
import fileinput
import sys
import psycopg2
from datetime import date, timedelta

curr_date=str(date.today())
prev_date=str(date.today() - timedelta(days=1))

print "Current Date:"+curr_date
print "Previous Date:"+prev_date

## Youtube and Facebook S3 files required for validation
video_stats_file_S3='s3://cm-crawled-data/youtube/stats/video/unruly/video_stats-'+prev_date+'.gz'
fb_shares_file_S3='s3://cm-crawled-data/advocacy/unruly/fb_shares_url_'+prev_date+'.txt.gz'

fb_video_shares='s3://cm-crawled-data/unruly_facebook_daily/video_shares_'+prev_date+'.txt.gz'
fb_video_stats='s3://cm-crawled-data/unruly_facebook_daily/video_stats_'+prev_date+'.txt.gz'

## Connection to Postgres DB
db=psycopg2.connect(database="unruly", user="unruly_db_user", password="cmPa$$word", host="unrulymasterdb.cxzdsudu61x9.us-west-1.rds.amazonaws.com", port="5432")

## Validation Results file 
f_output_all='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/validation_results_'+curr_date+'.txt'

## Download latest Video stats files from S3
def get_files_from_S3_to_local(file1,file2):
    cmd1='''cd /home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/s3-files/
    s3cmd get %s
    '''

    cmd2='''cd /home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/s3-files/
    s3cmd get %s
    '''

    os.system(cmd1 %(file1))
    os.system(cmd2 %(file2))
    
    file1_name=file1.split("/")[-1]
    file2_name=file2.split("/")[-1]

    cmd3=''' cd /home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/s3-files/
    gunzip %s
    '''

    os.system(cmd3 %(file1_name))
    os.system(cmd3 %(file2_name)) 
    
    unzip1='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/s3-files/'+file1_name.split(".gz")[0]
    unzip2='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/s3-files/'+file2_name.split(".gz")[0]

    return (unzip1,unzip2)

############################## 1.) UNRULY_YT_CHANNEL_CRAWLED_LIST

f_output_yccl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yccl-error-file.txt'

def unruly_yt_channel_crawled_list():
    cur=db.cursor()
    sql1= """
    select channel_id from analyst.unruly_yt_channel_crawled_list where channel_name is NULL
    """

    sql2= """
    select channel_id from analyst.unruly_yt_channel_crawled_list where country_code is NULL
    """

    sql3= """
    select count(distinct channel_id) from analyst.unruly_yt_channel_crawled_list
    """

    sql4= """
    select count(distinct channel_id) from analyst.unruly_yt_channel_client_list;
    """

    ## channel_name NOT NULL check
    print "\nRunning testcases for UNRULY_YT_CHANNEL_CRAWLED_LIST"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
	f=open(f_output_all,'a')
	f.write("Validations for UNRULY_YT_CHANNEL_CRAWLED_LIST:\n")
	f.write("---------------------------------------------------\n")
	f.write("channel_name NOT NULL--PASSED\n")
	f.close()
    else:
	f_output_yccl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_chnl_crwld_list_error_'+curr_date+'.txt'

	f=open(f_output_all,'a')
	f.write("Validations for UNRULY_YT_CHANNEL_CRAWLED_LIST:\n")
	f.write("---------------------------------------------------\n")
	f.write("channel_name NOT NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_yccl_err+"!! \n")
	f.close()

	f1 = open(f_output_yccl_err,'a')
	f1.write("\nError records where channel_name is NULL\n")
	f1.write("channel_id's:\n")
	for i in answer1:
	    f1.write(i[0]+"\n")
    	f1.close()     
    
    ## country_code NOT NULL check
    cur.execute(sql2)
    answer2=cur.fetchall()
    if not answer2:
	f=open(f_output_all,'a')
        f.write("country_code NOT NULL--PASSED\n")
	f.close()
    else:
	f_output_yccl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_chnl_crwld_list_error_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("country_code NOT NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_yccl_err+"!! \n")
        f.close()

        f1 = open(f_output_yccl_err,'a')
        f1.write("\nError records where country_code is NULL\n")
	f1.write("channel_id's:\n")
        for i in answer2:
            f1 = open(f_output_yccl_err,'a')
            f1.write(i[0]+"\n") 
	f1.close()

    ## Count of channel_id in both tables check
    cur.execute(sql3)
    answer3=cur.fetchone()
    count_crawled_list=answer3[0]

    cur.execute(sql4)
    answer4=cur.fetchone()
    count_client_list=answer4[0]

    if(count_crawled_list == count_client_list):
	f=open(f_output_all,'a')
	f.write("Count(Channel_id) MATCHES Count(Channel_id) in unruly_yt_channel_client_list --PASSED \n")  
	f.close()
    else:
	f_output_yccl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_chnl_crwld_list_error_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("Count(Channel_id) NOT MATCHES Count(Channel_id) in unruly_yt_channel_client_list--FAILED!!\n")
	f.write("Please check error file--"+f_output_yccl_err+"!! \n")
        f.close()

        f1 = open(f_output_yccl_err,'a')
	f1.write("\nCount(Channel_id) in unruly_yt_channel_client_list and unruly_yt_channel_crawled_list NOT MATCHES:\n")
	f1.write("--------------------------------------------------------------------------------------\n")
        f1.write("COUNT(channel_id) in unruly_yt_channel_crawled_list:"+str(count_crawled_list))
	f1.write("\nCOUNT(channel_id) in unruly_yt_channel_client_list:"+str(count_client_list)+"\n")

unruly_yt_channel_crawled_list()

############################## 2.) UNRULY_FB_PAGE_CRAWLED_LIST

def unruly_fb_page_crawled_list():
    cur=db.cursor()
    sql1= """
    select page_id from analyst.unruly_fb_page_crawled_list where page_name is null
    """

    sql2= """
    select page_id from analyst.unruly_fb_page_crawled_list where country is null 
    """

    sql3= """
    select count(distinct page_id) from  analyst.unruly_fb_page_crawled_list
    """

    sql4= """
    select count(distinct page_id) from  analyst.unruly_fb_page_client_list
    """

    ## page_name NOT NULL check
    print "\nRunning testcases for UNRULY_FB_PAGE_CRAWLED_LIST"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_FB_PAGE_CRAWLED_LIST:\n")
	f.write("--------------------------------------------------\n")
        f.write("page_name NOT NULL--PASSED\n")
        f.close()
    else:
        f_output_fpcl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_pg_crwld_list_error_file_'+curr_date+'.txt'

	f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_FB_PAGE_CRAWLED_LIST:\n")
	f.write("--------------------------------------------------\n")
        f.write("page_name NOT NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_fpcl_err+"!! \n")
        f.close()

        f1 = open(f_output_fpcl_err,'a')
        f1.write("\nError records where page_name is NULL\n")
	f1.write("page_id:\n")
        for i in answer1:
            f1.write(i[0]+"\n")
        f1.close()

    ## country_code NOT NULL check
    cur.execute(sql2)
    answer2=cur.fetchall()
    if not answer2:
        f=open(f_output_all,'a')
	f.write("country_code NOT NULL--PASSED\n")
        f.close()
    else:
	f_output_fpcl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_pg_crwld_list_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("country_code NOT NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_fpcl_err+"!! \n")
        f.close()

        f1 = open(f_output_fpcl_err,'a')
        f1.write("\nError records where country_code is NULL\n")
	f1.write("page_id:\n")
        for i in answer2:
            f1.write(i[0]+"\n")
        f1.close()

    ## Count of page_id in both tables check
    cur.execute(sql3)
    answer3=cur.fetchone()
    count_page_crawled_list=answer3[0]

    cur.execute(sql4)
    answer4=cur.fetchone()
    count_page_client_list=answer4[0]

    if(count_page_crawled_list == count_page_client_list):
        f=open(f_output_all,'a')
        f.write("Count(page_id) MATCHES Count(page_id) in unruly_fb_page_client_list--PASSED !! \n")
        f.close()
    else:
	f_output_fpcl_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_pg_crwld_list_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("Count(page_id) NOT MATCHES unruly_fb_page_client_list--FAILED!!\n")
	f.write("Please check error file--"+f_output_fpcl_err+"!! \n")
        f.close()

	f1 = open(f_output_fpcl_err,'a')
	f1.write("\nCount(page_id) in unruly_fb_page_client_list and unruly_fb_page_crawled_list NOT MATCHES:\n")
	f1.write("--------------------------------------------------------------\n")
        f1.write("COUNT()channel_idge_id in unruly_fb_page_crawled_list:"+str(count_page_crawled_list))
        f1.write("\nCOUNT(channel_id) in unruly_fb_page_client_list:"+str(count_page_client_list)+"\n")

unruly_fb_page_crawled_list()
############################## 3.) UNRULY_YT_VIDEO_DETAILS

def unruly_yt_video_details():
    cur=db.cursor()
    sql1= """
    select video_id from analyst.UNRULY_YT_VIDEO_DETAILS where title is null or category is null or published_date is null or video_duration is null
    """

    sql2= """
    select count(distinct channel_id) from analyst.unruly_yt_channel_client_list
    """

    sql3= """
    select count(distinct channel_id) from analyst.unruly_yt_video_details
    """

    sql4= """
    select count(distinct video_id) from analyst.unruly_yt_video_details
    """

    ## Details NOT NULL check
    print "\nRunning testcases for UNRULY_YT_VIDEO_DETAILS"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_YT_VIDEO_DETAILS:\n")
	f.write("---------------------------------------------\n")
        f.write("Title, category, published_date, video_duration NOT NULL--PASSED\n")
        f.close()
    else:
        f_output_yvd_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_vid_dtls_error_file_'+curr_date+'.txt'
	
	f=open(f_output_all,'a')
        f.write("\nValidations for UNRULY_YT_VIDEO_DETAILS:\n")
	f.write("---------------------------------------------\n")
        f.write("Title, category, published_date, video_duration NOT NULL--FAILED\n")
	f.write("Please check the error file:"+f_output_yvd_err+"\n")
	f.close()

        f1 = open(f_output_yvd_err,'a')
        f1.write("\nError records where detail columns are NULL\n")
        f1.write("video_id:\n")
        for i in answer1:
            f1.write(i[0]+"\n")
        f1.close()

    ## Count of channel_id in unruly_yt_channel_client_list and unruly_yt_video_details check
    cur.execute(sql2)
    answer2=cur.fetchone()
    count_channel_client_list=answer2[0]

    cur.execute(sql3)
    answer3=cur.fetchone()
    count_video_details=answer3[0]

    if(count_channel_client_list == count_video_details):
        f=open(f_output_all,'a')
        f.write("Count(channel_id) MATCHES Count(channel_id) in unruly_yt_video_details--PASSED!! \n")
        f.close()
    else:
	f_output_yvd_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_vid_dtls_error_file_'+curr_date+'.txt'

	f=open(f_output_all,'a')
        f.write("Count(channel_id) NOT MATCHES Count(channel_id) in unruly_yt_video_details--FAILED!!\n")
	f.write("Please check error file--"+f_output_yvd_err+"!! \n")
	f.close()

        f1 = open(f_output_yvd_err,'a')
	f1.write("\nCount(channel_id) in unruly_yt_video_details and unruly_yt_video_details NOT MATCHES:\n")
	f1.write("----------------------------------------------------------------------------------\n")
        f1.write("COUNT(channel_id) in unruly_yt_channel_client_list:"+str(count_channel_client_list))
        f1.write("\nCOUNT(channel_id) in unruly_yt_video_details:"+str(count_video_details)+"\n")

    ## count(video_id) in latest video stats file should match count(video_id) in UNRULY_YT_VIDEO_DETAILS check
    vid_stat_file,fb_share_file = get_files_from_S3_to_local(video_stats_file_S3,fb_shares_file_S3)
    print "Files downloaded from S3 and unzipped!!\n"
    video_id_set=set()
    
    with open(vid_stat_file,'r') as ff:
        for line in ff:
	    vid_id=line.split('\x01')[0]
	    video_id_set.add(vid_id)

    ff.close()

    with open(fb_share_file,'r') as ff1:
        for row in ff1:
	    vid_id=row.split('\x01')[0]
   	    video_id_set.add(vid_id)

    ff1.close()

    cur.execute(sql4)
    answer4=cur.fetchone()
    count_video_id_from_details=answer4[0] 

    count_video_id_from_file=len(video_id_set)
    
    if(count_video_id_from_details == count_video_id_from_file):
	f=open(f_output_all,'a')
        f.write("count(video_id) in latest video stats file MATCHES count(video_id) in UNRULY_YT_VIDEO_DETAILS--PASSED!! \n")
	f.close()
    else:
	f_output_yvd_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_vid_dtls_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("\ncount(video_id) in latest video stats file NOT MATCHES count(video_id) in UNRULY_YT_VIDEO_DETAILS--FAILED!!\n")
	f.write("Please check error file--"+f_output_yvd_err+"\n")
	f.close()

        f1 = open(f_output_yvd_err,'a')
        f1.write("\ncount(video_id) in latest video stats file does NOT MATCHES count(video_id) in UNRULY_YT_VIDEO_DETAILS!! \n")
	f1.write("-------------------------------------------------------------------------------------------------------------")
	f1.write("\nCOUNT(video_id) in latest video stats file for today:"+str(count_video_id_from_file))
	f1.write("\nCOUNT(video_id) in UNRULY_YT_VIDEO_DETAILS:"+str(count_video_id_from_details)+"\n")
	
unruly_yt_video_details()    
############################## 4.) UNRULY_FB_VIDEO_DETAILS

def unruly_fb_video_details():
    cur=db.cursor()
    sql1= """
    select video_id from analyst.UNRULY_FB_VIDEO_DETAILS where title is null or content_category is null or published_date is null or video_duration is null
    """

    sql2= """
    select count(distinct page_id) from analyst.unruly_fb_page_client_list
    """

    sql3= """
    select count(distinct page_id) from analyst.unruly_fb_video_details
    """
	
    sql4= """
    select count(distinct video_id) from analyst.UNRULY_FB_VIDEO_DETAILS
    """

    ## Details NOT NULL check
    print "\nRunning testcases for UNRULY_FB_VIDEO_DETAILS"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_FB_VIDEO_DETAILS:\n")
	f.write("----------------------------------------------\n")
        f.write("Title, category, published_date, video_duration NOT NULL--PASSED\n")
        f.close()
    else:
        f_output_fvd_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_vid_dtls_error_file_'+curr_date+'.txt'

	f=open(f_output_all,'a')
        f.write("\nValidations for UNRULY_FB_VIDEO_DETAILS:\n")
	f.write("----------------------------------------------\n")
	f.write("Title, category, published_date, video_duration NOT NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_fvd_err+"\n")
	f.close()

        f1 = open(f_output_fvd_err,'a')
        f1.write("Error records where detail columns are NULL\n")
        f1.write("video_id:\n")
        for i in answer1:
            f1.write(i[0]+"\n")
        f1.close()

    ## Count of page_id in both tables check
    cur.execute(sql2)
    answer2=cur.fetchone()
    count_page_client_list=answer2[0]

    cur.execute(sql3)
    answer3=cur.fetchone()
    count_video_details=answer3[0]

    if(count_page_client_list == count_video_details):
        f=open(f_output_all,'a')
        f.write("Count(page_id) MATCHES Count(page_id) in unruly_fb_video_details--PASSED\n")
        f.close()
    else:
	f_output_fvd_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_vid_dtls_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("\nCount(page_id) NOT MATCHES Count(page_id) in unruly_fb_page_client_list--FAILED!!\n")
	f.write("Please check error file--"+f_output_fvd_err+"\n")
	f.close()

        f1 = open(f_output_fvd_err,'a')
	f1.write("\nCount(page_id) in unruly_fb_page_client_list and unruly_fb_video_details NOT MATCHES:")
	f1.write("\n----------------------------------------------------------------------------\n")
        f1.write("\nCOUNT(page_id) in unruly_fb_page_client_list:"+str(count_page_client_list))
        f1.write("\nCOUNT(page_id) in unruly_fb_video_details:"+str(count_video_details)+"\n")
 
    ## count(video_id) in latest video stats file should match count(video_id) in UNRULY_YT_VIDEO_DETAILS check
    fb_vid_share_file,fb_vid_stat_file = get_files_from_S3_to_local(fb_video_shares,fb_video_stats)
    fb_video_id_set=set()

    with open(fb_vid_share_file,'r') as ff:
        for line in ff:
            vid_id=line.split('\x01')[1]
            fb_video_id_set.add(vid_id)

    ff.close()

    with open(fb_vid_stat_file,'r') as ff1:
        for row in ff1:
            vid_id=row.split('\x01')[1]
            fb_video_id_set.add(vid_id)

    ff1.close()

    cur.execute(sql4)
    answer4=cur.fetchone()
    count_video_id_from_details=answer4[0]

    count_video_id_from_file=len(fb_video_id_set)

    if(count_video_id_from_details == count_video_id_from_file):
        f=open(f_output_all,'a')
        f.write("count(video_id) in latest video stats file MATCHES count(video_id) in UNRULY_FB_VIDEO_DETAILS--PASSED!! \n")
        f.close()
    else:
	f_output_fvd_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_vid_dtls_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("\ncount(video_id) in latest video stats file NOT MATCHES count(video_id) in UNRULY_FB_VIDEO_DETAILS--FAILED!!\n")
        f.write("Please check error file--"+f_output_fvd_err+"\n")
        f.close()

        f1 = open(f_output_fvd_err,'a')
        f1.write("\ncount(video_id) in latest video stats file does NOT MATCHES count(video_id) in UNRULY_FB_VIDEO_DETAILS!! \n")
        f1.write("-------------------------------------------------------------------------------------------------------------\n")
        f1.write("COUNT(video_id) in latest video stats file for today:"+str(count_video_id_from_file))
        f1.write("\nCOUNT(video_id) in UNRULY_YT_VIDEO_DETAILS:"+str(count_video_id_from_details)+"\n")


unruly_fb_video_details()
############################## 5.) UNRULY_YT_DAILY_CHANNEL_INSIGHTS

def unruly_yt_daily_channel_insights():
    cur=db.cursor()
    sql1= """
    select count(distinct channel_id) from analyst.unruly_yt_channel_client_list 
    """

    sql2= """
    select count(distinct channel_id) from analyst.unruly_yt_daily_channel_insights where crawled_date=now()::date - integer '1'
    """

    sql3= """
    select a.channel_id from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS a 
    Join (select channel_id,max(crawled_date) as crawled_date from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS 
    where views is null 
    group by channel_id) b
    on a.channel_id = b.channel_id
    and a.crawled_date > b.crawled_date
    where a.views is null;
    """

    sql4= """
    select a.channel_id from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS a 
    Join (select channel_id,max(crawled_date) as crawled_date from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS 
    where subscribers is null 
    group by channel_id) b
    on a.channel_id = b.channel_id
    and a.crawled_date > b.crawled_date
    where a.subscribers is null 
    """

    sql5= """
    select a.channel_id from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS a 
    Join (select channel_id,max(crawled_date) as crawled_date from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS 
    where videos is null 
    group by channel_id) b
    on a.channel_id = b.channel_id
    and a.crawled_date > b.crawled_date
    where a.videos is null
    """

    sql6= """
    select channel_id from analyst.UNRULY_YT_DAILY_CHANNEL_INSIGHTS where crawled_date is null
    """

    ## Count of channel_id in both tables check
    print "\nRunning testcases for UNRULY_YT_DAILY_CHANNEL_INSIGHTS"
    cur.execute(sql1)
    answer1=cur.fetchone()
    count_channel_client_list=answer1[0]

    cur.execute(sql2)
    answer2=cur.fetchone()
    count_channel_insights=answer2[0]

    if(count_channel_client_list == count_channel_insights):
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_YT_DAILY_CHANNEL_INSIGHTS\n")
	f.write("-------------------------------------------------------\n")
        f.write("Count(channel_id) and Count(channel_id) in unruly_yt_channel_client_list MATCHES--PASSED!! \n")
        f.close()
    else:
        f_output_ydci_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_chnl_insgts_error_file_'+curr_date+'.txt'

	f=open(f_output_all,'a')
        f.write("\nValidations for UNRULY_YT_DAILY_CHANNEL_INSIGHTS\n")
        f.write("-------------------------------------------------------\n")
	f.write("Count(channel_id) and Count(channel_id) in unruly_yt_channel_client_list NOT MATCHES--FAILED!!\n")
	f.write("Please check error file--"+f_output_ydci_err+"\n")
	f.close()

        f1 = open(f_output_ydci_err,'a')
	f1.write("\nCount(channel_id) in UNRULY_YT_DAILY_CHANNEL_INSIGHTS and unruly_yt_channel_client_list NOT MATCHES:\n")
	f1.write("--------------------------------------------------------------------------------------------\n")
        f1.write("COUNT(channel_id) in unruly_yt_channel_client_list:"+str(count_channel_client_list))
        f1.write("\nCOUNT(channel_id) in UNRULY_YT_DAILY_CHANNEL_INSIGHTS:"+str(count_channel_insights)+"\n")

    ## Views cannot be on any day NULL unless they are always NULL check
    cur.execute(sql3)
    answer3=cur.fetchall()
    if not answer3:
        f=open(f_output_all,'a')
        f.write("Views cannot be on any day NULL unless they are always NULL check--PASSED\n")
        f.close()
    else:
	f_output_ydci_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_chnl_insgts_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("\nViews cannot be on any day NULL unless they are always NULL check--FAILED!!\n")
	f.write("Please check error file--"+f_output_ydci_err+"\n")
        f.close()

        f1 = open(f_output_ydci_err,'a')
        f1.write("\nViews cannot be on any day NULL unless they are always NULL check--FAILED \n")
        f1.write("channel_id:\n")
        for i in answer3:
            f1.write(i[0]+"\n")
        f1.close()

    ## Subscribers cannot be on any day NULL unless they are always NULL check
    cur.execute(sql4)
    answer4=cur.fetchall()
    if not answer4:
        f=open(f_output_all,'a')
        f.write("Subscribers cannot be on any day NULL unless they are always NULL check--PASSED\n")
        f.close()
    else:
	f_output_ydci_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_chnl_insgts_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("\nSubscribers cannot be on any day NULL unless they are always NULL check--FAILED!!\n")
        f.write("Please check error file--"+f_output_ydci_err+"\n")
        f.close()

        f1 = open(f_output_ydci_err,'a')
        f1.write("\nSubscribers cannot be on any day NULL unless they are always NULL check--FAILED \n")
        f1.write("channel_id:\n")
        for i in answer4:
            f1.write(i[0]+"\n")
        f1.close()

    ## Videos cannot be on any day NULL unless they are always NULL check
    cur.execute(sql5)
    answer5=cur.fetchall()
    if not answer5:
        f=open(f_output_all,'a')
	f.write("Videos cannot be on any day NULL unless they are always NULL check--PASSED\n")
        f.close()
    else:
	f_output_ydci_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_chnl_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
        f.write("\nVideos cannot be on any day NULL unless they are always NULL check--FAILED!!\n")
        f.write("Please check error file--"+f_output_ydci_err+"\n")
        f.close()

        f1 = open(f_output_ydci_err,'a')
        f1.write("Views cannot be on any day NULL unless they are always NULL check--FAILED \n")
        f1.write("channel_id:\n")
        for i in answer5:
            f1.write(i[0]+"\n")
        f1.close()
 
    ## Crawled_date cannot be null for any channel_id ever
    cur.execute(sql6)
    answer6=cur.fetchall()
    if not answer6:
        f=open(f_output_all,'a')
        f.write("Crawled_date cannot be NULL for any channel_id ever check--PASSED\n")
        f.close()
    else:
	f_output_ydci_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_chnl_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
	f.write("\nCrawled_date cannot be NULL for any channel_id ever check--FAILED!!\n")
        f.write("Please check error file--"+f_output_ydci_err+"\n")
        f.close()

        f1 = open(f_output_ydci_err,'a')
	f1.write("\nCrawled_date cannot be NULL for any channel_id ever check--FAILED!!\n")
        f1.write("channel_id:\n")
        for i in answer6:
            f1.write(i[0]+"\n")
        f1.close()

unruly_yt_daily_channel_insights()
############################## 6.) UNRULY_FB_DAILY_PAGE_INSIGHTS 

def unruly_fb_daily_page_insights():
    cur=db.cursor()
    sql1= """
    select a.page_id from analyst.UNRULY_FB_DAILY_PAGE_INSIGHTS a 
    Join (select page_id,max(crawled_date) as crawled_date from analyst.UNRULY_FB_DAILY_PAGE_INSIGHTS 
    where likes is null 
    or videos is null
    group by page_id) b
    on a.page_id = b.page_id
    and a.crawled_date > b.crawled_date
    where a.likes is null 
    or a.videos is null 
    """ 

    sql2= """
    select count(distinct page_id) from analyst.unruly_fb_page_client_list
    """

    sql3= """
    select count(distinct page_id) from analyst.unruly_fb_daily_page_insights where crawled_date=now()::date - integer '1'
    """

    sql4= """
    select a.page_id from analyst.UNRULY_FB_DAILY_PAGE_INSIGHTS a where crawled_date is NULL
    """

    ## likes, videos cannot be on any day NULL unless they are always NULL
    print "\nRunning testcases for UNRULY_FB_DAILY_PAGE_INSIGHTS"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_FB_DAILY_PAGE_INSIGHTS:\n")
	f.write("-----------------------------------------------------\n")
        f.write("likes, videos cannot be on any day NULL unless they are always NULL--PASSED\n")
        f.close()
    else:
        f_output_fdpi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_pg_insgts_error_file_'+curr_date+'.txt'
	
	f=open(f_output_all,'a')
        f.write("\nValidations for UNRULY_FB_DAILY_PAGE_INSIGHTS:\n")
        f.write("-----------------------------------------------------\n")
        f.write("likes, videos cannot be on any day NULL unless they are always NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_fdpi_err+"\n")
	f.close()

        f1 = open(f_output_fdpi_err,'a')
        f1.write("\nlikes, videos cannot be on any day NULL unless they are always NULL--FAILED \n")
        f1.write("page_id:\n")
        for i in answer1:
            f1.write(i[0]+"\n")
        f1.close()


    ## Crawled_date cannot be null for any page_id ever
    cur.execute(sql4)
    answer4=cur.fetchall()
    if not answer4:
        f=open(f_output_all,'a')
        f.write("Crawled_date cannot be NULL for any page_id ever check--PASSED\n")
        f.close()
    else:
	f_output_fdpi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_pg_insgts_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
        f.write("\nCrawled_date cannot be NULL for any page_id ever check--FAILED!!\n")
	f.write("Please check error file--"+f_output_fdpi_err+"\n")
        f.close()

        f1 = open(f_output_fdpi_err,'a')
        f1.write("\nCrawled_date cannot be NULL for any page_id ever check--FAILED \n")
        f1.write("page_id:\n")
        for i in answer4:
            f1.write(i[0]+"\n")
        f1.close()

    ## Count of page_id in both tables check
    cur.execute(sql2)
    answer2=cur.fetchone()
    count_page_client_list=answer2[0]

    cur.execute(sql3)
    answer3=cur.fetchone()
    count_page_insights=answer3[0]

    if(count_page_client_list == count_page_insights):
        f=open(f_output_all,'a')
        f.write("Count(page_id) in tables unruly_fb_page_client_list and unruly_fb_daily_page_insights MATCHES!! \n")
        f.close()
    else:
	f_output_fdpi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_pg_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
        f.write("\nCount(page_id) in tables unruly_fb_page_client_list and unruly_fb_daily_page_insights NOT MATCHES--FAILED!!\n")
        f.write("Please check error file--"+f_output_fdpi_err+"\n")
        f.close()

        f1 = open(f_output_fdpi_err,'a')
	f1.write("\nCount(page_id) in tables unruly_fb_page_client_list and unruly_fb_daily_page_insights NOT MATCHES:\n")
	f1.write("----------------------------------------------------------------------------------------------------\n")
        f1.write("COUNT(page_id) in unruly_fb_page_client_list:"+str(count_page_client_list))
        f1.write("\nCOUNT(page_id) in unruly_fb_daily_page_insights:"+str(count_page_insights)+"\n")

unruly_fb_daily_page_insights()
############################## 7.) UNRULY_YT_DAILY_VIDEO_INSIGHTS 

def unruly_yt_daily_video_insights():
    cur=db.cursor()
    sql1= """
    select a.video_id from analyst.UNRULY_YT_DAILY_VIDEO_INSIGHTS_v2 a 
    Join (select video_id,max(crawled_date) as crawled_date from analyst.UNRULY_YT_DAILY_VIDEO_INSIGHTS_v2 
    where (likes is null 
    or views is null 
    or comments is null 
    or dislikes is null)
    and crawled_date=now()::date - integer '1'
    group by video_id) b

    on a.video_id = b.video_id
    and a.crawled_date > b.crawled_date
    where (a.views is null 
    or a.comments is null 
    or a.likes is null 
    or a.dislikes is null)
    and a.crawled_date=now()::date - integer '1'
    """

    sql2= """
    select a.video_id from analyst.UNRULY_YT_DAILY_VIDEO_INSIGHTS_v2 a 
    Join (select video_id,max(crawled_date) as crawled_date from analyst.UNRULY_YT_DAILY_VIDEO_INSIGHTS_v2 
    where fb_shares is null
    and crawled_date=now()::date - integer '1'
    group by video_id) b

    on a.video_id = b.video_id
    and a.crawled_date > b.crawled_date
    where fb_shares is null
    and a.crawled_date=now()::date - integer '1'
    """
 
    sql3= """
    select count(distinct video_id) from analyst.unruly_yt_daily_video_insights_v2 where crawled_date=now()::date - integer '1'
    """

    sql4= """
    select count(distinct video_id) from analyst.unruly_yt_video_details
    """

    sql5= """
    select a.video_id from analyst.UNRULY_YT_DAILY_VIDEO_INSIGHTS_v2 a where crawled_date is null
    """

    ## likes, views,comments,dislikes cannot be on any day NULL unless they are always NULL
    print "\nRunning testcases for UNRULY_YT_DAILY_VIDEO_INSIGHTS"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_YT_DAILY_VIDEO_INSIGHTS:\n")
	f.write("----------------------------------------------------\n")
        f.write("likes, views,comments,dislikes cannot be on any day NULL unless they are always NULL--PASSED\n")
        f.close()
    else:
        f_output_ydvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_vid_insgts_error_file_'+curr_date+'.txt'
	
	f=open(f_output_all,'a')
        f.write("\nValidations for UNRULY_YT_DAILY_VIDEO_INSIGHTS:\n")
        f.write("----------------------------------------------------\n")
        f.write("likes, views,comments,dislikes cannot be on any day NULL unless they are always NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_ydvi_err+"\n")
	f.close()

        f1 = open(f_output_ydvi_err,'a')
        f1.write("likes, views,comments,dislikes cannot be on any day NULL unless they are always NULL--FAILED\n")
        f1.write("video_id:\n")
        for i in answer1:
            f1.write(i[0]+"\n")
        f1.close()

    ## fb_shares cannot be on any day NULL unless they are always NULL
    cur.execute(sql2)
    answer2=cur.fetchall()
    if not answer2:
        f=open(f_output_all,'a')
        f.write("fb_shares cannot be on any day NULL unless they are always NULL--PASSED\n")
        f.close()
    else:
	f_output_ydvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_vid_insgts_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
	f.write("fb_shares cannot be on any day NULL unless they are always NULL--FAILED\n")
	f.write("Please check error file--"+f_output_ydvi_err+"\n")
        f.close()

        f1 = open(f_output_ydvi_err,'a')
	f1.write("fb_shares cannot be on any day NULL unless they are always NULL--FAILED \n")
        f1.write("video_id:\n")
        for i in answer2:
            f1.write(i[0]+"\n")
        f1.close()


    ## Crawled_date cannot be null for any video_id ever
    cur.execute(sql5)
    answer5=cur.fetchall()
    if not answer5:
        f=open(f_output_all,'a')
        f.write("Crawled_date cannot be NULL for any video_id ever check--PASSED\n")
        f.close()
    else:
	f_output_ydvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_vid_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
	f.write("Crawled_date cannot be NULL for any video_id ever check--FAILED!!\n")
        f.write("Please check error file--"+f_output_ydvi_err+"\n")
        f.close()

        f1 = open(f_output_ydvi_err,'a')
        f1.write("Crawled_date cannot be NULL for any video_id ever check--FAILED\n")
        f1.write("video_id:\n")
        for i in answer5:
            f1.write(i[0]+"\n")
        f1.close()

    ## Count of page_id in both tables check
    cur.execute(sql3)
    answer3=cur.fetchone()
    count_video_insights=answer3[0]

    cur.execute(sql4)
    answer4=cur.fetchone()
    count_video_details=answer4[0]

    if(count_video_insights == count_video_details):
        f=open(f_output_all,'a')
        f.write("Count(video_id) in unruly_yt_daily_video_insights and unruly_yt_video_details MATCHES--PASSED!! \n")
        f.close()
    else:
	f_output_ydvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/yt_dly_vid_insgts_error_file_'+curr_date+'.txt'

        f=open(f_output_all,'a')
	f.write("Count(video_id) in unruly_yt_daily_video_insights and unruly_yt_video_details --FAILED!!\n")
        f.write("Please check error file--"+f_output_ydvi_err+"\n")
        f.close()

        f1 = open(f_output_ydvi_err,'a')
	f1.write("\nCount(video_id) in unruly_yt_daily_video_insights and unruly_yt_video_details NOT MATCHES:\n")
	f1.write("-----------------------------------------------------------------------------------------------\n")
        f1.write("COUNT(video_id) in unruly_yt_daily_video_insights:"+str(count_video_insights))
        f1.write("\nCOUNT(video_id) in unruly_yt_video_details:"+str(count_video_details)+"\n")

unruly_yt_daily_video_insights()
############################## 8.) UNRULY_FB_DAILY_VIDEO_INSIGHTS

def unruly_fb_daily_video_insights():
    cur=db.cursor()
    sql1= """
    select a.video_id from analyst.UNRULY_FB_DAILY_VIDEO_INSIGHTS a 
    Join (select video_id,max(crawled_date) as crawled_date from analyst.UNRULY_FB_DAILY_VIDEO_INSIGHTS 
    where (likes is null 
    or views is null 
    or comments is null)
    and crawled_date=now()::date - integer '1'
    group by video_id) b

    on a.video_id = b.video_id
    and a.crawled_date > b.crawled_date
    where (a.views is null 
    or a.comments is null 
    or a.likes is null)
    and a.crawled_date=now()::date - integer '1'
    """

    sql2= """
    select a.video_id from analyst.UNRULY_FB_DAILY_VIDEO_INSIGHTS a 
    Join (select video_id,max(crawled_date) as crawled_date from analyst.UNRULY_FB_DAILY_VIDEO_INSIGHTS 
    where shares is null 
    and crawled_date=now()::date - integer '1'
    group by video_id) b

    on a.video_id = b.video_id
    and a.crawled_date > b.crawled_date
    where a.shares is null
    and a.crawled_date=now()::date - integer '1'
    """

    sql3= """
    select a.video_id from analyst.UNRULY_FB_DAILY_VIDEO_INSIGHTS a where crawled_date is null
    """

    sql4= """
    select count(distinct video_id) from analyst.unruly_fb_daily_video_insights where crawled_date=now()::date - integer '1'
    """

    sql5= """
    select count(distinct video_id) from analyst.unruly_fb_video_details
    """

    ## likes, views,comments cannot be on any day NULL unless they are always NULL
    print "\nRunning testcases for UNRULY_FB_DAILY_VIDEO_INSIGHTS"
    cur.execute(sql1)
    answer1=cur.fetchall()
    if not answer1:
        f=open(f_output_all,'a')
	f.write("\nValidations for UNRULY_FB_DAILY_VIDEO_INSIGHTS:\n")
	f.write("----------------------------------------------------\n")
        f.write("likes, views,comments cannot be on any day NULL unless they are always NULL--PASSED\n")
        f.close()
    else:
        f_output_fdvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_vid_insgts_error_file_'+curr_date+'.txt'
	
	f=open(f_output_all,'a')
        f.write("\nValidations for UNRULY_FB_DAILY_VIDEO_INSIGHTS:\n")
        f.write("----------------------------------------------------\n")
        f.write("likes, views,comments cannot be on any day NULL unless they are always NULL--FAILED!!\n")
	f.write("Please check error file--"+f_output_fdvi_err+"\n")
	f.close()

        f1 = open(f_output_fdvi_err,'a')
        f1.write("likes, views,comments cannot be on any day NULL unless they are always NULL--FAILED\n")
        f1.write("video_id:\n")
        for i in answer1:
            f1.write(i[0]+"\n")
        f1.close()

    ## shares cannot be on any day NULL unless they are always NULL
    cur.execute(sql2)
    answer2=cur.fetchall()
    if not answer2:
        f=open(f_output_all,'a')
        f.write("shares cannot be on any day NULL unless they are always NULL--PASSED\n")
        f.close()
    else:
	f_output_fdvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_vid_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
	f.write("shares cannot be on any day NULL unless they are always NULL--FAILED!!\n")
        f.write("Please check error file--"+f_output_fdvi_err+"\n")
        f.close()

        f1 = open(f_output_fdvi_err,'a')
        f1.write("shares cannot be on any day NULL unless they are always NULL--FAILED\n")
        f1.write("video_id:\n")
        for i in answer2:
            f1.write(i[0]+"\n")
        f1.close()

    ## Crawled_date cannot be null for any video_id ever
    cur.execute(sql3)
    answer3=cur.fetchall()
    if not answer3:
        f=open(f_output_all,'a')
        f.write("Crawled_date cannot be NULL for any video_id ever check--PASSED\n")
        f.close()
    else:
	f_output_fdvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_vid_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
	f.write("Crawled_date cannot be NULL for any video_id ever check--FAILED!!\n")
        f.write("Please check error file--"+f_output_fdvi_err+"\n")
        f.close()

        f1 = open(f_output_fdvi_err,'a')
        f1.write("Crawled_date cannot be NULL for any video_id ever check--FAILED\n")
        f1.write("video_id:\n")
        for i in answer3:
            f1.write(i[0]+"\n")
        f1.close()

    ## Count of page_id in both tables check
    cur.execute(sql4)
    answer4=cur.fetchone()
    count_video_insights=answer4[0]

    cur.execute(sql5)
    answer5=cur.fetchone()
    count_video_details=answer5[0]

    if(count_video_insights == count_video_details):
        f=open(f_output_all,'a')
        f.write("Count(video_id) in unruly_fb_daily_video_insights and unruly_fb_video_details MATCHES--PASSED!! \n")
        f.close()
    else:
	f_output_fdvi_err='/home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files/fb_dly_vid_insgts_error_file_'+curr_date+'.txt'
        
        f=open(f_output_all,'a')
	f.write("Count(video_id) in unruly_fb_daily_video_insights and unruly_fb_video_details NOT MATCHES--FAILED!! \n")
        f.write("Please check error file--"+f_output_fdvi_err+"\n")
        f.close()

        f1 = open(f_output_fdvi_err,'a')
	f1.write("\nCount(video_id) in unruly_fb_daily_video_insights and unruly_fb_video_details NOT MATCHES:\n")
	f1.write("--------------------------------------------------------------------------------------------\n")
        f1.write("COUNT(video_id) in unruly_fb_daily_video_insights:"+str(count_video_insights))
        f1.write("\nCOUNT(video_id) in unruly_fb_video_details:"+str(count_video_details)+"\n")

unruly_fb_daily_video_insights()

################################ Archiving all error files for the current run in Date-wise folder

def archive_error_files(current_date):
    cmd=''' cd /home/likewise-open/TALENTICA-ALL/mayankp/culture_machines/unruly/error-files
    mkdir %s 
    mv *.txt %s
    '''

    os.system(cmd %(current_date,current_date))

archive_error_files(curr_date)
