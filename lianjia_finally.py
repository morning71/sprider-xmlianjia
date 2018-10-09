# -*- coding:utf-8 -*-
import requests
from bs4 import BeautifulSoup
import time
import MySQLdb
import urllib
import urllib2
import json
import cookielib
import re
import zlib
import sys
from threading import Thread
from Queue import Queue
from time import sleep
reload(sys)
sys.setdefaultencoding("utf-8")
# 获取当前时间

def getCurrentTime():
    return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

def getFangCondition():
    result = []
    for a in range(1,9):#面积
        for l in  range(1,7):#户型
            for p in  range(1,9):#总价
                cond = {}
                cond['url']='a'+str(a)+'l'+str(l)+'p'+str(p)
                cond['a']='a'+str(a)
                cond['l']='l'+str(l)
                cond['p']='p'+str(p)
                #print cond['url']
                result.append(cond)
    return result

def getFangTransCondition():
    result = []
    for a in range(1,9):#面积
        for l in  range(1,7):#户型
            cond = {}
            cond['url']='a'+str(a)+'l'+str(l)
            cond['a']='a'+str(a)
            cond['l']='l'+str(l)
            #print  cond['url']
            result.append(cond)
    return result

def getIp(url1):
    User_Agent = 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'
    header = {}
    header['User-Agent'] = User_Agent
    url1 = 'http://www.xicidaili.com/nn/1'
    req = urllib2.Request(url1, headers=header)
    res = urllib2.urlopen(req).read()
    soup = BeautifulSoup(res, 'html.parser')
    ips = soup.findAll('tr')
    proxys = []
    f = open("../pyworkpeace/ip","w+")
    for x in range(1, len(ips)):
        ip = ips[x]
        tds = ip.findAll("td")
        ip_temp = tds[1].get_text() + "\t" + tds[2].get_text() + "\n"
        ip = ip_temp.strip("\n").split("\t")
        proxy_host = "http://" + ip[0] + ":" + ip[1]
        proxy_temp = {"http": proxy_host}

        f.write(str(proxy_temp)+"\n")

    f.close()

def getURL(url, tries_num=10, sleep_time=0, time_out=10):
    headers = { 'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding': 'gzip, deflate, sdch',
                'Accept-Language':'zh-CN,zh;q=0.8',
                'Connection':'keep-alive',
                'Host':'xm.lianjia.com',
                'Referer':'http://user.lianjia.com/',
                'Upgrade-Insecure-Requests': '1',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.75 Safari/537.36 LBBROWSER'}
    proxies = {"http": "http://59.57.28.34:808", "https": "http://59.57.28.34:808", }
    sleep_time_p = sleep_time
    time_out_p = time_out
    tries_num_p = tries_num
    try:
        time.sleep(sleep_time_p)
        res = requests.Session()
        if isproxy==1:
            res = requests.get(url, headers=headers, timeout=time_out,proxies=proxies)
        else:
            res = requests.get(url, headers=headers, timeout=time_out)
        res.raise_for_status()  # 如果响应状态码不是 200，就主动抛出异常
    except requests.RequestException as e:
        sleep_time_p = sleep_time_p + 10
        time_out_p = time_out_p + 10
        tries_num_p = tries_num_p -1
        # 设置重试次数，最大timeout 时间和 最长休眠时间
        #print tries_num_p
        if tries_num_p >0 :
            time.sleep(sleep_time_p)
            print getCurrentTime(), url, 'URL Connection Error: 第', max_retry- tries_num_p, u'次 Retry Connection', e
            res = getURL(url, tries_num_p, sleep_time_p, time_out_p)
            if res.status_code == 200:
               print getCurrentTime(), url, 'URL Connection Success: 共尝试',  max_retry- tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p
            else:
               print getCurrentTime(), url, 'URL Connection Error: 共尝试',  max_retry- tries_num_p, u'次', ',sleep_time:', sleep_time_p, ',time_out:', time_out_p
               pass
    return res

def getXiaoquList(fang_url):#小区模块
    result = {}
    base_url = 'http://xm.lianjia.com'
    # res=requests.get(fang_url)
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.find_all('li',class_ = 'clear xiaoquListItem'):#获取出所有<li class='clear xiaoquListItem'>标签
        if (len(fang) > 0):
            try:
                result['xiaoqu_key'] = fang.select('.title')[0].a['href'].split("/")[4]
                result['xiaoqu_name'] = fang.select('.title')[0].get_text()
                result['xiaoqu_url'] = fang.select('.title')[0].a['href']
                result['quyu'] = fang.select('.district')[0].get_text()
                result['bankuai'] =fang.select('.bizcircle')[0].get_text()
                result['price'] = fang.select('.totalPrice')[0].get_text()
                result['age'] = fang.select('.positionInfo')[0].get_text().split('/')[1]
                result['onsale_num'] = fang.select('.totalSellCount')[0].get_text()
                result['fang_url'] = ''
                if len(fang.select('.totalSellCount')) > 0:
                    result['fang_url'] = fang.select('.totalSellCount')[0]['href']
                    #getLianjiaList(result['fang_url'])
                result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                mySQL.insertData('lianjia_fang_xiaoqu', result)
                print getCurrentTime(), u'小区：', result['xiaoqu_key'], result['xiaoqu_name'], result['age'], result[ 'quyu'], \
                                                    result['xiaoqu_url'], result['price'], result['onsale_num'], result['fang_url']
                getLianjiaList(result['fang_url'])
            except Exception, e:
                print  getCurrentTime(), u"Exception:%d: %s" % (e.args[0], e.args[1])
    return result

def getLianjiaList(fang_url):
    result = {}
    base_url = 'http://xm.lianjia.com'
    # res=requests.get(fang_url)
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.find_all('div',class_ = 'info clear'):
        if (len(fang) > 0):
            try:
                result['fang_key'] = fang.find('div',class_ = 'unitPrice')["data-hid"]
                result['fang_desc'] = fang.find('div',class_ = 'title').get_text()
                result['fang_url'] = fang.select('.title')[0].a['href'].strip()
                result['price'] = fang.select('.totalPrice')[0].get_text()
                result['price_pre'] = fang.select('.unitPrice')[0].get_text()
                result['xiaoqu'] = fang.select('.houseInfo')[0].get_text().split("|")[0]
                result['huxing'] = ""
                result['mianji'] = ""
                result['bankuai'] = fang.select('.positionInfo')[0].get_text().split("-")[1]
                result['quyu'] = soup.find('div',{'data-role':"ershoufang"}).select('.selected')[0].get_text()
                result['louceng'] = fang.select('.positionInfo')[0].get_text().split("-")[0].split(")")[0].replace('(',' ')
                result['chaoxiang'] = ""
                result['age'] = ""
                result['zhuangxiu'] = ""
                result['taxfree'] = ""
                result['haskey'] = ""
                result['col_look'] = ""
                result['xiaoqu_key'] = fang.select('.houseInfo')[0].a['href'].strip().split("/")[4]
                if ")" in fang.select('.positionInfo')[0].get_text().split("-")[0]:
                    result['age'] = fang.select('.positionInfo')[0].get_text().split("-")[0].split(")")[1]
                else:
                    result['age'] = fang.select('.positionInfo')[0].get_text().split("-")[0]
                if len(fang.select('.taxfree')) > 0:
                    result['taxfree'] = fang.select('.taxfree')[0].get_text()
                if len(fang.findAll('span',class_='haskey')) > 0:
                    result['haskey'] = fang.select('.haskey')[0].get_text()
                if len(fang.select('.houseInfo')[0].get_text().split("|")) == 4 :#车库
                    result['huxing'] = fang.select('.houseInfo')[0].get_text().split("|")[1]
                    result['mianji'] = fang.select('.houseInfo')[0].get_text().split("|")[2]
                    result['zhuangxiu'] = fang.select('.houseInfo')[0].get_text().split("|")[1]
                    result['chaoxiang'] = fang.select('.houseInfo')[0].get_text().split("|")[3]
                elif len(fang.select('.houseInfo')[0].get_text().split("|")) == 5:#普通商品房
                    result['huxing'] = fang.select('.houseInfo')[0].get_text().split("|")[1]
                    result['mianji'] = fang.select('.houseInfo')[0].get_text().split("|")[2]
                    result['zhuangxiu'] = fang.select('.houseInfo')[0].get_text().split("|")[4]
                    result['chaoxiang'] = fang.select('.houseInfo')[0].get_text().split("|")[3]
                elif len(fang.select('.houseInfo')[0].get_text().split("|")) == 6:
                    if any(char.isdigit() for char in fang.select('.houseInfo')[0].get_text().split("|")[1]) == True:#特征中含有电梯
                        result['huxing'] = fang.select('.houseInfo')[0].get_text().split("|")[1]
                        result['mianji'] = fang.select('.houseInfo')[0].get_text().split("|")[2]
                        result['zhuangxiu'] = fang.select('.houseInfo')[0].get_text().split("|")[4]
                        result['chaoxiang'] = fang.select('.houseInfo')[0].get_text().split("|")[3]
                    elif any(char.isdigit() for char in fang.select('.houseInfo')[0].get_text().split("|")[1]) == False:#别墅
                        result['huxing'] = fang.select('.houseInfo')[0].get_text().split("|")[2]
                        result['mianji'] = fang.select('.houseInfo')[0].get_text().split("|")[3]
                        result['zhuangxiu'] = fang.select('.houseInfo')[0].get_text().split("|")[5]
                        result['chaoxiang'] = fang.select('.houseInfo')[0].get_text().split("|")[4]
                result['col_look'] = fang.select('.followInfo')[0].get_text().split("/")[1]
                result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                mySQL.insertData('lianjia_fang_list', result)
                print getCurrentTime(), u'在售：', result['fang_key'], result['quyu'], result['zhuangxiu'], result['xiaoqu'], \
                                                 result['huxing'], result['price'], result['price_pre'], result['mianji']
            # fangList.append(result)
            except Exception, e:
                print  getCurrentTime(), u"Exception:%d: %s" % (e.args[0], e.args[1])
    return result

def getLianjiaTransList(fang_url):#成交数据
    result = {}
    base_url = 'http://xm.lianjia.com'
    # res=requests.get(fang_url)
    res = getURL(fang_url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    for fang in soup.find_all('div',class_ = 'info'):
        if (len(fang) > 0):
            try:
                result['fang_desc'] = fang.select('.title')[0].get_text()
                result['fang_url'] = fang.select('.title')[0].a['href'].strip()
                result['taxfree'] = ''
                result['xiaoqu'] = fang.select('.title')[0].get_text().split(" ")[0]
                result['mianji'] = fang.select('.title')[0].get_text().split(" ")[2]
                result['huxing'] = fang.select('.title')[0].get_text().split(" ")[1]
                result['fang_key']=fang.select('.title')[0].a['href'].split("/")[4].split(".")[0]
                result['chaoxiang'] = fang.select('.houseInfo')[0].get_text().split("|")[0]
                result['zhuangxiu'] = fang.select('.houseInfo')[0].get_text().split("|")[1]
                result['transaction_date'] = fang.select('.dealDate')[0].get_text()
                result['price_pre'] = fang.select('.unitPrice')[0].get_text()
                result['price'] = fang.select('.totalPrice')[0].get_text()
                result['quyu'] = fang.select('.title')[0].get_text().split(" ")[0]
                result['louceng'] = fang.select('.positionInfo')[0].get_text().split(" ")[0]
                if len(fang.select('.dealHouseTxt')) > 0 :
                    result['taxfree'] = fang.select('.dealHouseTxt')[0].get_text()
                result['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                print getCurrentTime(), u'成交：', result['transaction_date'], result['quyu'], \
                    result['fang_desc'], result['chaoxiang'], result['louceng'], result['zhuangxiu'], result[
                    'price_pre'], result['price']  # ,result['fang_url']
                mySQL.insertData('lianjia_fang_transaction', result)
            except Exception, e:
                print  getCurrentTime(), u"Exception:%d: %s" % (e.args[0], e.args[1])
    return result

def getSubRegions(fang_url, region):
    base_url = 'http://xm.lianjia.com'
    res = getURL(fang_url + region['code'])
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_plate = soup.find('div', {'data-role':"ershoufang"})
    try:
        for link in gio_plate.find_all('a'):
            district = {}
            district['link']=link.get('href')
            district['code'] = link.get('href').split("/")[2]
            district['name']=link.get_text()
            if district['code'] not in ['plate-nolimit']:
                result.append(district)
    except AttributeError:
        return result
    #print getCurrentTime(),'getSubRegions:',result
    return result

def getRegions(fang_url, region):
    base_url = 'http://xm.lianjia.com'
    url_fang = fang_url + region;
    res = getURL(fang_url + region)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, 'html.parser')
    result = []
    gio_district = soup.find('div', {'data-role':"ershoufang"})
    for link in gio_district.find_all('a'):
        district = {}
        district['link']=link.get('href')
        district['code'] = link.get('href').split("/")[2]
        district['name']=link.get_text()
        if district['code'] not in ['district-nolimit']:
            result.append(district)
    #print getCurrentTime(),'getRegions:',result
    return result

class MySQL:
    # 获取当前时间
    def getCurrentTime(self):
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    # 数据库初始化
    def _init_(self, ip, username, pwd, schema):
        try:
            self.db = MySQLdb.connect('localhost', 'root', 'root', 'fang')
            print self.getCurrentTime(), u"MySQL DB Connect Success"
            self.cur = self.db.cursor()
        except MySQLdb.Error, e:
            print self.getCurrentTime(), u"MySQL DB Connect Error :%d: %s" % (e.args[0], e.args[1])

    # 插入数据
    def insertData(self, table, my_dict):
        try:
            self.db.set_character_set('utf8')
            cols = ', '.join(my_dict.keys())
            values = '"," '.join(my_dict.values())
            sql = "REPLACE INTO %s (%s) VALUES (%s)" % (table, cols, '"' + values + '"')
            try:
                result = self.cur.execute(sql)
                insert_id = self.db.insert_id()
                self.db.commit()
                # 判断是否执行成功
                if result:
                    return insert_id
                else:
                    return 0
            except MySQLdb.Error, e:
                # 发生错误时回滚
                self.db.rollback()
                # 主键唯一，无法插入
                if "key 'PRIMARY'" in e.args[1]:
                    print self.getCurrentTime(), u"Primary Key Constraint，No Data Insert:", e.args[0], e.args[1]
                    # return 0
                elif "MySQL server has gone away" in e.args :
                    self._init_('localhost', 'root', 'root', 'fang')
                else:
                    print self.getCurrentTime(), u"Data Insert Failed: %d: %s" % (e.args[0], e.args[1])
        except MySQLdb.Error, e:
            print self.getCurrentTime(), u"MySQLdb Error:%d: %s" % (e.args[0], e.args[1])

def getTransMain():
    regions = getRegions('http://xm.lianjia.com/ershoufang/', 'siming')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://xm.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    chengjiao_url = 'http://xm.lianjia.com/chengjiao/' + subRegion['code'] + '/pg' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang = getLianjiaTransList(chengjiao_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getTransMain Scrapy Success'

def getXiaoquMain():
    regions = getRegions('http://xm.lianjia.com/ershoufang/', 'siming')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://xm.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://xm.lianjia.com/xiaoqu/' + subRegion['code'] + '/pg' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getXiaoquList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getXiaoquList Scrapy Finished'
                        break
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getXiaoquMain Scrapy Success'

def getFangMain():
    regions = getRegions('http://xm.lianjia.com/ershoufang/', 'siming')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://xm.lianjia.com/ershoufang/', region)
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), subRegion['name'], ':',  'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://xm.lianjia.com/ershoufang/' + subRegion['code']+ '/pg' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getLianjiaList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaList Scrapy Finished'
                        break
                print getCurrentTime(),subRegion['name'], ':',  'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'getFangMain Scrapy Success'

def mainAll():
    regions = getRegions('http://xm.lianjia.com/ershoufang/', 'siming')
    regions.reverse()
    #start_page = 1
    #end_page =10
    #sleep_time = 0.5
    #按行政区域爬取数据
    while regions:
        #break  #Test
        region = regions.pop()
        print getCurrentTime(), 'Region:',region['name'], ':', 'Scrapy Starting.....'
        time.sleep(sleep_time)
        subRegions = getSubRegions('http://xm.lianjia.com/ershoufang/', region)#ershoufang/siming....
        subRegions.reverse()
        while subRegions:
            try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                # time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    fang_url = 'http://xm.lianjia.com/xiaoqu/' + subRegion['code'] + '/pg' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getXiaoquList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getXiaoquList Scrapy Finished'
                        break

                for i in range(start_page, end_page):
                    fang_url = 'http://xm.lianjia.com' + subRegion['link']+ 'pg' + str(i)
                    print getCurrentTime(), region['name'], ':', subRegion['name'], fang_url
                    time.sleep(sleep_time)
                    fang = getLianjiaList(fang_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaList Scrapy Finished'
                        break

                for i in range(start_page, end_page):
                    chengjiao_url = 'http://xm.lianjia.com/chengjiao/' + subRegion['code'] + '/pg' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang2 = getLianjiaTransList(chengjiao_url)
                    if len(fang2) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
            except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
                #if "MySQL server has gone away" in e.args:
                #    mySQL._init_('localhost', 'root', 'root', 'fang')
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
    print getCurrentTime(), 'Lianjia Xianmen All Scrapy Success'

def getSubregionsThread():
    while True:
        region = regionsQueue.get()
        subRegions=getSubRegions('http://xm.lianjia.com/ershoufang/', region)
        while subRegions:
             try:
                subRegion = subRegions.pop()
                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Starting.....'
                time.sleep(sleep_time)
                for i in range(start_page, end_page):
                    chengjiao_url = 'http://xm.lianjia.com/chengjiao' + subRegion['link'] + 'pg' + str(i)
                    print getCurrentTime(), subRegion['name'], chengjiao_url
                    time.sleep(sleep_time)
                    fang = getLianjiaTransList(chengjiao_url)
                    if len(fang) < 1:
                        print getCurrentTime(), region['name'], ':', subRegion['name'], u' : getLianjiaTransList Scrapy Finished'
                        break

                print getCurrentTime(), region['name'], ':', subRegion['name'], 'Scrapy Finished'
             except Exception, e:
                print  getCurrentTime(), u"Exception:%s" % (e.message)
        print getCurrentTime(), region['name'], ':', 'Scrapy Finished'
        regionsQueue.task_done()

def getTransThread():
    regions = getRegions('http://xm.lianjia.com/ershoufang/', 'siming')
    regions.reverse()
    while regions:
       regionsQueue.put(regions.pop())
    #fork NUM个线程等待队列
    for i in range(NUM):
      t = Thread(target= getSubregionsThread)
      print i ,u'启动'
      t.setDaemon(True)
      t.start()
    #等待所有JOBS完成
    regionsQueue.join()
    print getCurrentTime(), 'getTransMain Scrapy Success'

def getFangMaxPagesMain():
     regions = getRegions('http://xm.lianjia.com/ershoufang/', 'siming')
     regions.reverse()
     maxpage=0
     while regions:
         #time.sleep(sleep_time)
         region = regions.pop()
         maxpage=getMaxPage('http://xm.lianjia.com/ershoufang/'+ region['code'])
         #print region['name'],': '+str(maxpage)

         subRegions = getSubRegions('http://xm.lianjia.com/ershoufang/', region)
         subRegions.reverse()

         while subRegions:
            #break
            #time.sleep(sleep_time)
            subRegion=subRegions.pop()
            maxpage=getMaxPage('http://xm.lianjia.com/ershoufang/'+ subRegion['code'])
            #print type(int(str(maxpage)))
            if int(str(maxpage))>=100:
                print region['name'],', '+subRegion['name'],': '+str(maxpage)
     print 'Non regions over 100 pages'

def main():
    #LianjiaLogin()
    print getCurrentTime(), 'Main Scrapy Starting'
    global mySQL, start_page, end_page, sleep_time,regionsQueue,NUM,taskQueue,max_retry,isproxy
    mySQL = MySQL()
    mySQL._init_('localhost', 'root', 'root', 'fang')
    isproxy=0 #1 内网 使用代理，0 外网不用代理
    max_retry=10
    start_page=1
    end_page=10
    sleep_time=0.1
    regionsQueue = Queue()  # q是任务队列
    taskQueue= Queue()
    url1 = 'http://www.xicidaili.com/nn/1'
    getIp(url1)
    NUM =2  #NUM是并发线程总数
    JOBS = 100 #JOBS是有多少任务
    #getSubregionsThread()
    #getTransMain()
    #getTransThread()
    getFangMain()
    #mainAll()
    #getXiaoquMain()
    #getMaxPage(url)
    #getFangMaxPagesMain()

if __name__ == "__main__":
    main()