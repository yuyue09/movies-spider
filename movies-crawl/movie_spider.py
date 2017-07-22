# -*-coding:utf8-*-

import requests
import json
import random
import pymysql
import sys
import datetime
import time
from imp import reload
from threading import Thread
from bs4 import BeautifulSoup
from agents import user_agent_list


reload(sys)

tag_list=['大陆','美国','香港','台湾','日本','韩国','英国','法国','德国','意大利','西班牙','印度','泰国','俄罗斯','伊朗','加拿大','澳大利亚','爱尔兰','瑞典','巴西','丹麦']

url_list=[]


conn=pymysql.connect(host='localhost',port=3306,user='root',password='root',db='mysql',charset='utf8')
cur=conn.cursor()
cur.execute('USE douban')
sql='INSERT INTO movies(name,directors,casts,score,rate_sum,show_date,categroies,country,summary,movie_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'

ua=random.choice(user_agent_list)

head = {
    'User-Agent': ua,
    'Cookie':'gr_user_id=6d63b754-9c85-40dc-9aac-4d8f027ff14b; bid=8iVQOyzUydQ; ll="108288"; __yadk_uid=lTZchPLK9Yx9IGLPUkJkdQGC3GysubXK; ps=y; ct=y; _ga=GA1.2.921658378.1449037673; viewed="1418999_1422079_25862578_26963321_1085799_1789837_1483923_1559310_1002299_1793132"; _vwo_uuid_v2=84CC0A494B0735B783F5A432D355417D|59bd386317900bfee9e5588f879ef3e8; dbcl2="132791543:jZOS9xiLdLY"; ck=xy9C; _pk_ref.100001.4cf6=%5B%22%22%2C%22%22%2C1500642959%2C%22https%3A%2F%2Fwww.douban.com%2Faccounts%2Flogin%3Fredir%3Dhttps%253A%252F%252Fmovie.douban.com%252Fsubject%252F1929463%252F%22%5D; ap=1; _pk_id.100001.4cf6=9984575155cac4e0.1459776502.116.1500643077.1500641147.; _pk_ses.100001.4cf6=*; __utma=30149280.921658378.1449037673.1500638652.1500642959.221; __utmb=30149280.0.10.1500642959; __utmc=30149280; __utmz=30149280.1500642959.221.183.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/accounts/login; __utmv=30149280.13279; __utma=223695111.817972453.1459776502.1500638652.1500642959.122; __utmb=223695111.0.10.1500642959; __utmc=223695111; __utmz=223695111.1500642959.122.112.utmcsr=douban.com|utmccn=(referral)|utmcmd=referral|utmcct=/accounts/login; push_noty_num=0; push_doumail_num=0',
    'Host':'movie.douban.com',
    'Referer': 'https://movie.douban.com/tag/?sort=T&range=0,10&tags',
    
}
proxies = {
'http':'http://140.240.81.16:8888',
'http':'http://185.107.80.44:3128',
'http':'http://203.198.193.3:808',
'http':'http://125.88.74.122:85',
'http':'http://125.88.74.122:84',
'http':'http://125.88.74.122:82',
'http':'http://125.88.74.122:83',
'http':'http://125.88.74.122:81',
'http':'http://123.57.184.70:8081'
}


def getsource(url):
    content=requests.session().get(url,headers=head,proxies=proxies)
    soup=BeautifulSoup(content.text,'lxml')
    name=soup.find('span',{'property':'v:itemreviewed'}).get_text()#片名
    dires=soup.findAll('a',{'rel':'v:directedBy'})
    directors=str([dire.get_text() for dire in dires]).strip('[]')#导演
    cas=soup.findAll('a',{'rel':'v:starring'})
    if len(cas)<=5:
        casts=str([ca.get_text() for ca in cas]).strip('[]')
    else:
        casts=str([ca.get_text() for ca in cas[0:5]]).strip('[]')#演员表
    try:
        score=float(soup.find('strong',{'property':'v:average'}).get_text())#评分
    except:
        score=0
    try:
        rate_sum=soup.find('a',{'class':'rating_people'}).find('span',{'property':'v:votes'}).get_text()#评分人数
    except:
        rate_sum='评分人数不足'
    try:
        show_date=soup.findAll('span',{'property':'v:initialReleaseDate'})[0].get_text()#上映日期
    except:
        show_date=None
    try:
        cates=soup.findAll('span',{'property':'v:genre'})
    except:
        cates=None
    categroies=str([cate.get_text() for cate in cates]).strip('[]')#影片类型
    try:
        country=soup.find(text='制片国家/地区:').next_element#制片地区
    except:
        country=None
    try:
        summary=soup.find('span',{'class':'all hidden'}).get_text()#影片简介
    except:
        try:
            summary=soup.find('span',{'property':'v:summary'}).get_text()
        except:
            summary=None
    movie_id=url.split('/')[-2]
    print(name,directors,casts,score,rate_sum,show_date,categroies,country,summary,movie_id)

    try:
        cur.execute(sql,(name,directors,casts,score,rate_sum,show_date,categroies,country,summary,movie_id))
        conn.commit()
        print('数据存储成功！')
    except Exception as e:
        print('数据存储出现异常：',e)
        conn.rollback()

    





def work(i,tag):
    jscontent = requests.session().get('https://movie.douban.com/j/new_search_subjects?sort=T&range=0,10&tags=电影,'+tag+'&start='+i, headers=head,proxies=proxies).text
    
    try:
        jsDict = json.loads(jscontent)
            #print(jsDict)
        if 'data' in jsDict.keys():
            datas=jsDict['data']
            for data in datas:
                url=data['url']
                #print(url)
                if url not in url_list:
                    getsource(url)
                    url_list.append(url)
                else:
                    pass
                #
                #thread.start()
                #time.sleep(2)
        else:
            print('未发现有效的数据')

    except ValueError:
        print('decoding json has failed')







if __name__=='__main__':

    for tag in tag_list:
        for i in range(0,500):
            i=20*i
            i=str(i)
            try:
                work(i,tag)
            except:
                pass

cur.close()
conn.close()

    

