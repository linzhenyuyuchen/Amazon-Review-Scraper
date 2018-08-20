# -*- coding: utf-8 -*
import urllib2
import re
import time
import random
import MySQLdb

#random header
def randHeader():
    head_connection = ['Keep-Alive', 'close']
    head_accept = ['text/html, application/xhtml+xml, */*']
    head_accept_language = ['zh-CN,fr-FR;q=0.5', 'en-US,en;q=0.8,zh-Hans-CN;q=0.5,zh-Hans;q=0.3']
    head_user_agent = ['Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
                       'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1500.95 Safari/537.36',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; rv:11.0) like Gecko)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070309 Firefox/2.0.0.3',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1) Gecko/20070803 Firefox/1.5.0.12',
                       'Opera/9.27 (Windows NT 5.2; U; zh-cn)',
                       'Mozilla/5.0 (Macintosh; PPC Mac OS X; U; en) Opera 8.0',
                       'Opera/8.0 (Macintosh; PPC Mac OS X; U; en)',
                       'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.12) Gecko/20080219 Firefox/2.0.0.12 Navigator/9.0.0.6',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0)',
                       'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0)',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Maxthon/4.0.6.2000 Chrome/26.0.1410.43 Safari/537.1 ',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET4.0C; .NET4.0E; QQBrowser/7.3.9825.400)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0 ',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.92 Safari/537.1 LBBROWSER',
                       'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; BIDUBrowser 2.x)',
                       'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/3.0 Safari/536.11']

    header = {
        'Connection': head_connection[0],
        'Accept': head_accept[0],
        'Accept-Language': head_accept_language[1],
        'User-Agent': head_user_agent[random.randrange(0, len(head_user_agent))]
    }
    return header

def data_import(sql):
    print 'Import into mysql :'+sql
    flag=False
    db = MySQLdb.connect(host='localhost',port=3307,user='root',passwd='',db='py_comment',charset='utf8')
    cursor = db.cursor()
    cursor.execute("SET NAMES utf8mb4;")
    try:
        cursor.execute(sql)
        db.commit()
        flag=True
    except:
        db.rollback()
        flag=False
    db.close()
    return flag

def data_select(sql):
    db = MySQLdb.connect(host='localhost',port=3307,user='root',passwd='',db='py_comment',charset='utf8')
    cursor = db.cursor()
    print('Begin select ...')
    print sql
    try:
        cursor.execute(sql)
        results=cursor.fetchall()
        return results
    except:
        print "Error: unable to fecth data"
        db.close()
        return False
    db.close()
    return results

def readurl(url):
    html = None
    maxTryNum=10
    headers=randHeader()
    data=None
    for tries in range(maxTryNum):
        try:
            print 'reading url :'+str(url)
            request = urllib2.Request(url,data,headers)
            html=urllib2.urlopen(request,timeout=12).read()
            break;
        except urllib2.HTTPError,e:
            print e.code
            if(e.code!=200 and int(tries)<2):
                continue
            else:
                break
        except:
            if tries<maxTryNum:
                time.sleep(5)
                print "Trying "+str(tries)+" time(s)..."
                continue
            else:
                print "Has tried " + str(maxTryNum) + " time(s) to access url..."
                html=None
                break
    return html

def data_handle(datas):
    datas=MySQLdb.escape_string(datas)
    return datas

def get_reviews(pid):
    j=1
    flag=False
    pid=str(pid)
    url='https://www.amazon.com/product-reviews/'+pid+'/ref=cm_cr_getr_d_paging_btm_1?filterByStar=positive&pageNumber=1'
    html = readurl(url)
    if html==None:
        sql="update products set status0=1 where pid ='%s'" %pid
        data_import(sql)
        return flag
    res_tr= r'Showing 1-10 of (.*?) reviews'
    max_page = re.search(res_tr, html)
    if max_page == None:
        max_page="1"
    else:
        max_page=max_page.group()
    if max_page==None:
        sql="update products set status0=1 where pid ='%s'" %pid
        data_import(sql)
        return flag
    max_page=max_page.replace('Showing 1-10 of ','')
    max_page=max_page.replace(' reviews','')
    max_page=max_page.replace(',','')
    max_page=(int(max_page)+9)/10
    #print html
    #res_tr= r'<div data-hook="review-collapsed" aria-expanded="false" class="a-expander-content a-expander-partial-collapse-content">(.*?)</div>'
    res_tr= r'<span data-hook="review-body" class="a-size-base review-text">(.*?)</span>'
    comments = re.findall(res_tr, html)
    res_category= r'/dp/%s(.*?)</a>' %pid
    category = re.search(res_category, html)
    if category!=None:
        category=category.group()
        res_category = r'">(.*?)</a>'
        category = re.search(res_category, category).group()
    else:
        category="none"
    category=category.replace('">','')
    category=category.replace('</a>','')
    print 'Name:    '+category
    category=data_handle(category)
    while(j<=max_page):
        print 'Page '+str(j)+' of '+str(max_page)
        if j>1:
            url2=url.replace('cm_cr_getr_d_paging_btm_1','cm_cr_getr_d_paging_btm_'+str(j))
            url2=url2.replace('pageNumber=1','pageNumber='+str(j))
            print url2
            html = readurl(url2)
            if html==None:
                break;
            comments = re.findall(res_tr, html)
        j=j+1
        if category:
            flag=True
            for i in comments:
                #print i+'\n'
                i=data_handle(i)
                #i=emoj_fillter(i)
                sql="insert into comments (comments,category) values ('%s','%s')" %(i,category)
                try:
                    #print(sql)
                    if data_import(sql):
                        flag=True
                    else:
                        flag=False
                except Exception as e:
                    print (str(e))
        else:
            flag=True
    if flag:
        print 'Get all reviews successfully !!'
        sql="update products set status0=1 where pid ='%s'" %pid
        data_import(sql)
    time.sleep(2)
    return flag
def get_products(url_category):
    num=0
    num_duplicate=0
    html = readurl(url_category)
    #print html
    res_tr= r'<a class="a-link-normal a-text-normal" href="(.*?)">'
    urls = re.findall(res_tr, html)
    if urls:
        for i in urls:
            i=data_handle(i)
            #print i
            res_dp=r'/dp/(.*)'
            ii=re.findall(res_dp,i)
            if ii:
                ii_arr=ii[0].split('/')
                pid_i=ii_arr[0]
                sql="insert into products (pid,status0) values ('%s',0)" %(pid_i)
                try:
                    if data_import(sql):
                        num=num+1
                    else:
                        num_duplicate=num_duplicate+1
                except Exception as e:
                    print (str(e))
    print 'Fetch products '+str(num)+'(new) '+str(num_duplicate)+'(duplicate) successfully !!'

def get_urls(url_category):
    page_i=1
    html = readurl(url_category)
    #print html
    res_tr= r'<span class="pagnDisabled">(.*?)</span>'
    max_page = re.findall(res_tr, html)
    if max_page:
        max_page=int(max_page[0])
    else:
        max_page=4
    #print max_page
    while (page_i<=max_page) :
    #while (page_i<=3) :
        newpage="page="+str(page_i)
        newpage2="sr_pg_"+str(page_i)
        url_category_i=url_category.replace("page=2",newpage)
        url_category_i=url_category_i.replace("sr_pg_2",newpage2)
        print 'Getting '+str(page_i)+' Page of '+str(max_page)+' Category'
        get_products(url_category_i)
        page_i=page_i+1
        time.sleep(5)
    print 'Fetch urls and products successfully !!'

def get_rw(limit):
    count=1
    #mysql select pid
    if(limit==0):
        limit=10000
    sql="select * from products where status0!=1 order by id limit "+str(limit)
    pids=data_select(sql)
    for pid in pids:
        flag=False
        id=pid[0]
        print 'Begin Get Pid: '+str(id)+' Review...'+str(count)+' of '+str(len(pids))
        if get_reviews(id):
            flag=True
        else:
            flag=False
        time.sleep(3)
        count=count+1
    return flag

def total_reviews():
    sql='select count(*) from comments'
    results=data_select(sql)
    for res in results:
        print res[0]

def left_products():
    sql='select count(*) from products where status0!=1'
    results=data_select(sql)
    for res in results:
        print res[0]
if __name__=='__main__':
    #Second Page
    #url_category='https://www.amazon.com/s/ref='
    #get_reviews(url_reviews)
    #get_urls(url_category)
    str_input1=int(input("1->Search Pid through Category;\n 2->Get Reviews;\n 3->How many reviews;\n 4->Products Left;\n"))
    if str_input1==1:
        str_input2 = raw_input("Enter the category you want to scrapy pid,\n Second Page :\n//www.amazon.com/\n")
        str_input2='https://www.amazon.com/'+str(str_input2)
        get_urls(str(str_input2))
    elif str_input1==2:
        str_input2 = input("How many category's reviews:")
        get_rw(int(str_input2))
    elif str_input1==3:
        total_reviews()
    elif str_input1==4:
        left_products()
    else:
        print 'Error...'
        print str_input1
