#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 20:32:34 2019

@author: fanjingquan
"""

import json
with open("/Users/fanjingquan/Desktop/rest.json", 'r') as f:

    sort = 0
    master_source=['Left','Default','Right','Down','Up','Barselect']
    video_play_master_list= ['ab','vid','extra','light','cid','strategy','impression_id','source','comm_cnts','score','play_list_id','author_id','category_id','author_type']
    video_play_list= ['vid','extra','light','cid','source','comm_cnts','score','play_list_id','author_id','category_id','author_type']
    client_show_list =[]
    effective_play_list =[]
    video_over_list =[]
    post_comment_list=[]
    like_list=[]
    share_click_list=[]
    share_succ_list=[]
    app_start_list=[]

    for line in f.readlines():
        dic = json.loads(line)
        
        sort += 1
        print(sort,'====================================')
#==============================================================================================================                    
        if dic['event'] == 'video_play'and dic['body']['source']in master_source:
            print (dic['event'],'         ',dic['body']['source'])
            testlist=[]
            for body_values in dic["body"]:
                testlist.append(body_values)

            for test_values in testlist:
                if test_values not in video_play_master_list:
                    print(dic['event'],dic['body']['source'],'多报',test_values)     

            for video_play_values in video_play_master_list:
                if video_play_values not in testlist:
                    print(dic['event'],dic['body']['source'],'少报',video_play_values)  
#==============================================================================================================                    
        elif dic['event'] == 'video_play'and dic['body']['source']not in master_source:
            print (dic['event'],'         ',dic['body']['source'])
            testlist=[]
            for body_values in dic["body"]:
                testlist.append(body_values)

            for test_values in testlist:
                if test_values not in video_play_list:
                    print(dic['event'],dic['body']['source'],'多报',test_values)     

            for video_play_values in video_play_list:
                if video_play_values not in testlist:
                    print(dic['event'],dic['body']['source'],'少报',video_play_values)  
                    
        else:
            print('其它事件')            
f.close()

