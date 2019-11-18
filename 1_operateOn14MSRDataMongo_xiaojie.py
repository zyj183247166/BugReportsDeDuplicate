# -*- coding: utf-8 -*-
"""
Created on Mon Nov 18 07:20:34 2019

@author: xiaojie
"""
import pymongo
import re
class MyMongo:
    def init(self):
        self.client=pymongo.MongoClient(host='localhost',port=27017)
client=pymongo.MongoClient(host='localhost',port=27017)


import pickle
import numpy as np
import os
import random
#########################################################################################################
#########辅助函数定义
#########################################################################################################
def save_to_pkl(python_content, pickle_name):
    with open(pickle_name, 'wb') as pickle_f:
        pickle.dump(python_content, pickle_f)
def read_from_pkl(pickle_name):
    with open(pickle_name, 'rb') as pickle_f:
        python_content = pickle.load(pickle_f)
    return python_content


db=client.openOffice
##(1)输出集合数目
openOfficeDuplicateBugPairsNum=db.pairs.estimated_document_count()
print (openOfficeDuplicateBugPairsNum)


##(2)尝试性输出内容
cursor=db.pairs.find()
i=0
for document in cursor:
    #print (("%d:"%(i)),_student_)
    if(i==10):
        break
    i=i+1
    bug1=document['bug1']
    bug2=document['bug2']
    print (bug1,bug2)
#    new_file_path=filepath.replace("./weixin_article/","./")
#    result=db["picture_urlMd5_filepath"].update_many({'filepath':filepath},{'$set':{'filepath':new_file_path}})

##(3)数据库去重

all_dataset_name1 = './processedData_2014MSR_xiaojie/all_pairs_id_openOffice_XIAOJIE.pkl'
duplicate_true_pair_record_path='./processedData_2014MSR_xiaojie/duplicate_true_pair_record_openOffice.txt' #openOffice的pairs集合中重复的正标签数据。
duplicate_false_pair_record_path='./processedData_2014MSR_xiaojie/duplicate_false_pair_record_openOffice.txt'#openOffice的pairs集合中重复的负标签数据。
if not os.path.exists('./processedData_2014MSR_xiaojie/'):
        os.mkdir('./processedData_2014MSR_xiaojie/')

all_data1 = {}
f1 = open(duplicate_true_pair_record_path, 'w')
duplicate_trueduplicatepair_num=0
f1.write('id1,id2,is_duplicateReport\n')
f2 = open(duplicate_false_pair_record_path, 'w')
duplicate_falseduplicatepair_num=0
f2.write('id1,id2,is_duplicateReport\n')

cursor=db.pairs.find()
for document in cursor:
    
    bug1=(int)(document['bug1'])
    bug2=(int)(document['bug2'])
    isDuplicate=(int)(document['dec'])
#    print (bug1,bug2)
    key = (bug1, bug2)
    key2= (bug2, bug1)
    if((key in all_data1.keys())or(key2 in all_data1.keys())):
        if(isDuplicate==1):
            duplicate_trueduplicatepair_num=duplicate_trueduplicatepair_num+1
            f1.write('%s,%s,%s\n'%(str(bug1),str(bug2),str(isDuplicate)))
            #写入文件
            continue
        if(isDuplicate==-1):
            duplicate_falseduplicatepair_num=duplicate_falseduplicatepair_num+1
            f2.write('%s,%s,%s\n'%(str(bug1),str(bug2),str(isDuplicate)))
            #写入文件
            continue
    all_data1[key] = isDuplicate  
save_to_pkl(all_data1, all_dataset_name1)
f1.close()
f2.close()
print('openOffice的pairs集合中重复的正标签数据{}个，openOffice的pairs集合中重复的负标签数据{}个'.format(duplicate_trueduplicatepair_num,duplicate_falseduplicatepair_num))
print('openOffice的pairs集合中最后标签数据集的大小为{}'.format(len(all_data1)))