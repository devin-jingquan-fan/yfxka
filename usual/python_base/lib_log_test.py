#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 19 20:32:34 2019

@author: fanjingquan
"""

import json
with open("/Users/fanjingquan/Desktop/rest.json", 'r') as f:

    sort = 0
    video_play_list= ['ab','vid','extra','light','cid','strategy','impression_id','source','comm_cnts','score','play_list_id','author_id','category_id','author_type']
    
    for line in f.readlines():
        dic = json.loads(line)
#       按行读取并且将每行存成单独的json
        sort += 1
        print(sort,'====================================')
        
# =============================================================================
#         if dic['event'] == 'video_play':
# #           单独的事件纬度
#             print (dic['event'])
#             testlist=[]
#             for body_values in dic["body"]:
# #                遍历body下所有的key值
#                 testlist.append(body_values)
# #                将所有body下的字段存到list
# #                print(body_values)
#             if testlist == video_play_list :
#                     print('正确')
#             else:
#                     # print('错误',testlist)
#                     # print('video_play_list',video_play_list)
#                     if len(testlist)>len(video_play_list):
#                         print('多报')
#                     elif len(testlist)<len(video_play_list):
#                         print('少报')
# 
# =============================================================================
#===============================================================================================                        
        if dic['event'] == 'video_play':
            print (dic['event'])
            testlist=[]
            for body_values in dic["body"]:
                testlist.append(body_values)

            for test_values in testlist:
                if test_values not in video_play_list:
                    print('多报',test_values)  

            for video_play_values in video_play_list:
                if video_play_values not in testlist:
                    print('少报',video_play_values)      

# =============================================================================
#             if testlist == video_play_list :
#                     print('正确')
#             else:
#                     while testlist != video_play_list:
#                         video_play_values = video_play_list.pop()
# #                        test_values = testlist.pop()
#                         if video_play_values not in testlist:
#                             print('少报',video_play_values)
# #                        elif test_values not in video_play_list:
# #                            print('多报',test_values)
# =============================================================================
        else:
            print('其它事件')            
# =============================================================================
#                 if body_values in ('ab','vid','extra','light','cid','strategy','impression_id','source','comm_cnts','score','play_list_id','author_id','category_id','author_type'):
#                     print ('正确')    
#                 else:
#                     print (body_values)
#                     print (dic['body']['source'])
# =============================================================================
f.close()

