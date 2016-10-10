# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 19:39:50 2016

@author: hongshuang
"""

import time
import sys
import os
import re
import Queue
import json
from ftplib import FTP   
from threading import Timer  
from PyQt4.QtGui import *
from PyQt4.QtCore import *
from PyQt4.QtWebKit import *
from PyQt4.QtNetwork import *
import MySQLdb
import datetime
reload(sys)

sys.setdefaultencoding('utf8')  
   
class ChildWindow(QWidget):
    def __init__(self, parent=None):
        super(self.__class__, self).__init__(parent)
        
        self.page = QWebPage()
        self.browser = QWebView()
        self.browser.setPage(self.page)
        
        self.lineedit = QLineEdit()

        layout = QVBoxLayout()
        layout.setSpacing(0)
        layout.setMargin(0)
        layout.addWidget(self.browser)
        layout.addWidget(self.lineedit)
        self.setLayout(layout)
        self.connect(self.lineedit, SIGNAL("returnPressed()"), self.entrytext)
        
        self.browser.loadFinished.connect(self.loadFinish)
        
    def entrytext(self):
        self.browser.load(QUrl(self.lineedit.text()))
        
    def loadFinish(self, reply):
        print 'enter loadFinish' +  self.objectName()
        name = self.objectName()
        if name == 'Master':
            doFinishMaster(reply)
        if name == 'Slave':
            doFinishSlave(reply)
        
    def show(self):
        super(self.__class__, self).show()
    
    def load_url(self, url):
        self.browser.load(QUrl(url))
        
    def reload(self):
        self.browser.reload()
        
        
class MainWindow(QMainWindow):
    def __init__(self, url):
        super(MainWindow, self).__init__()
        self.toolBar = self.addToolBar("Quick")
        
        self.newAct = QAction('New', self, triggered=self.newActionWindow)
        self.newMaster = QAction('Master', self, triggered=self.newActionMaster)
        self.newSlave = QAction('Slave', self, triggered=self.newActionSlave)
        self.newPrepare = QAction('Prepare', self, triggered=self.newActionPrepare)
        self.newRun = QAction('Run', self, triggered=self.newActionRun)

        self.toolBar.addAction(self.newAct)
        self.toolBar.addAction(self.newMaster)
        self.toolBar.addAction(self.newSlave)
        self.toolBar.addAction(self.newPrepare)
        self.toolBar.addAction(self.newRun)

        self.winMaster = None
        self.winSlave = None
        
        self.mdiArea = QMdiArea()
        self.mdiArea.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.mdiArea.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        self.setCentralWidget(self.mdiArea)

        self.cookieJar = QNetworkCookieJar()
        self.netManager = QNetworkAccessManager()
        self.netManager.setCookieJar(self.cookieJar)
        
        self.slaverQueue = Queue.Queue()
        self.masterQueue = Queue.Queue()
        
        self.objectName = ''
        
            
    def newActionMaster(self):
        child = self.mdiArea.findChild(ChildWindow, QString('Master'))
        if child is None:
            self.winMaster = self.newActionWindow('Master')
        else:
            self.winMaster = child
        self.winMaster.setFocus()
        self.winMaster.lineedit.setText(cago_url)

    def newActionSlave(self):
        child = self.mdiArea.findChild(ChildWindow, QString('Slave'))
        if child is None:
            self.winSlave = self.newActionWindow('Slave')
        else:
            self.winSlave = child
        self.winSlave.setFocus()
        
    def newActionWindow(self, name='Default'):
        child = ChildWindow()
        child.page.setNetworkAccessManager(self.netManager)
        self.mdiArea.addSubWindow(child)
        
        child.setObjectName(QString(name))
        child.setWindowTitle(QString(name))        
        child.show()
        return child

    def newActionPrepare(self):
        global run_status
        self.newActionSlave()
        self.newActionMaster()
        
        gui.winMaster.load_url(home_url)
        #run_status = STATUS_PREPARE
        
    def newActionRun(self):
        global run_status
        run_status = STATUS_RUN
        gui.masterQueue.put('START')
        waterRun()
            
# prepare
def prepareRun():
    
    return
    
# after 1. New master window, login and open cago page
# Start all programe
def waterRun():
    print gui.masterQueue.qsize()
    
    if gui.slaverQueue.empty():
        # fill more detail url from next page
        # then feed to slave to spider        
        if gui.masterQueue.empty():
            print 'Master Queue Done'
            return
        print 'Let\'s goto next page'
        url = gui.masterQueue.get()
        print 'Get from master queue: %s' % url
        if url == 'START':
            gui.winMaster.reload()
        elif url == 'STOP':
            #10分钟后自动启动
            time.sleep(10 * 60)
            gui.masterQueue.put('START')
        else:
            gui.winMaster.load_url(url)
            
        return
        
    # let winSlave load the detail page url
    url = gui.slaverQueue.get()
    gui.winSlave.load_url(url)
    # done      

def updateRecord(cago_dict):
    '''
        更新记录
    '''
    code = cago_dict["record_id"]
  
    begindate = cago_dict.get(u'空船日期').encode('utf8')
    routetype = cago_dict.get(u'航线').encode('utf8')
    expiredate = cago_dict.get(u'截止日期').encode('utf8')
    startport = cago_dict.get(u'所在港').encode('utf8')
    destport = cago_dict.get(u'到达港').encode('utf8')
    type = cago_dict.get(u'类型').encode('utf8')
    remark=cago_dict.get(u'备注').encode('utf8')
    bossname = cago_dict.get('boss_name')
    linkname=''
    if bossname:
        linkname = code+".jpg"
        
    
    print linkname
    print cago_dict.get(u'联系方式')
    #解出    
    contact = cago_dict.get(u'联系方式').split("\n")
    companyname = ''
    
    linkphone=''
    linkqq=''
    if len(contact) > 1:
        companyname = contact[0]
        linkphone = contact[2]
        linkqq = contact[3]
    
    
        companyname = companyname.replace(u"公司名:","").encode('utf8')
        linkphone = linkphone.replace(u"手机:","").encode('utf8')
        linkqq = linkqq.replace(u"QQ号:","").encode('utf8')
    
    upatesql = "update tbl_ship_info set begindate = '%s',routetype='%s',expiredate='%s',\
    startport='%s',destport='%s',companyname = '%s',linkname='%s',linkphone='%s',linkqq='%s',\
    remark='%s',type='%s' where code='%s'"  \
    %(begindate,routetype,expiredate,startport,destport,\
    companyname,linkname,linkphone,linkqq,remark,type,code)
     
    print upatesql
    sql().executeSql(upatesql)
    
def createRecord(row):
    '''
        创建新纪录
    '''
    code = row[0].split("=")[1]
    volumn = row[1].encode("utf8")
    location=row[2].encode("utf8")
    dest=row[3].encode("utf8")
    pubdate=row[4].encode("utf8")
    now =  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    insertsql = "insert into tbl_ship_info(code,volumn,location,dest,pubdate,createtime) values('%s','%s','%s','%s','%s','%s')" \
    %(code,volumn,location,dest,pubdate, now)
    print insertsql
    sql().executeSql(insertsql)
    
def doFinishMaster(reply):
    print reply
    cur_url = str(gui.winMaster.browser.url().toString().toUtf8())
    print cur_url
    
    global run_status
    
    if run_status == STATUS_ZERO:
        
        print 'Start from zero...'
        print 'Auto login begin...'

        document = gui.winMaster.browser.page().mainFrame().documentElement()
        
        elem = document.findFirst('#top1_TextBox1')
        elem.setAttribute('value', 'xdslll')
        
        elem = document.findFirst('#top1_TextBox2')
        elem.setAttribute('value', '123456')
        
        elem = document.findFirst('#top1_Button1')
        elem.evaluateJavaScript("this.click()")
        
        run_status = STATUS_PREPARE
        return
        
    if run_status == STATUS_PREPARE:
        print 'Preparing...'
        gui.winMaster.load_url(cago_url)
        
        run_status = STATUS_RUN
        return
        
    if run_status != STATUS_RUN:
        print 'Something run happened...'
        return
        
    # work
    workMaster()
    # Master done
    # let water run 
    #waterRun()
    waterRunTimer.start(1000)

def doFinishSlave(reply):
    print reply
    cur_url = str(gui.winSlave.browser.url().toString().toUtf8())
    #print cur_url
    # work
    workSlave()
    # Slave done, let master go
    # let water run 
    #waterRun()
    waterRunTimer.start(1000)

# analyze Slave page

def checkEnd(code):
    '''
        检查是否达到抓取终点
    '''
    selectsql = "select code from tbl_ship_info where code = '%s' " %(code)
    result = sql().getSqlResult(selectsql)
    if(result):
        return True
    return False
    
def workSlave():
    document = gui.winSlave.browser.page().mainFrame().documentElement()
    cur_url = str(gui.winSlave.browser.url().toString().toUtf8())
    #print cur_url
    record_id = cur_url.split('=')[1]

    elem = document.findFirst('.spd-table')
    elem = elem.findFirst('tr')

    cago_name_link = ''
    boss_name_link = ''
    
    cago_dict = {}    
    cago_dict['record_id'] = record_id
    
    while not elem.isNull():
        row = []
        td = elem.findFirst('td')
        
        td_idx = 0
        while not td.isNull():
            v = str(td.toPlainText().trimmed().toUtf8()).decode('utf8')
            row.append(v)
            if td_idx == 1:
                # save to map
                cago_dict[row[0]] = row[1]
                if row[0] == u'名称':
                    img_elem = td.findFirst('img')
                    link = img_elem.attribute('src')
                    param = link.split('=')[1]
                                        
                    qimage = QImage(img_elem.geometry().size(), QImage.Format_ARGB32)
                    painter = QPainter(qimage)
                    img_elem.render(painter)
                    painter.end()
                    
                    # get cago name link
                    cago_name_link = '%s/%s' % (home_url, link)                    
                    # save to local img
                    #fpath = r'.\save_cago_img\%s.jpg' % param
                    #qimage.save(fpath)
                    # write to dict
                    #cago_dict['cago_name'] = str(param)
                    
                if row[0] == u'联系方式':
                    img_elem = td.findFirst('img')
                    link = img_elem.attribute('src')
                    if len(link.split('=')) > 1:
                        param = link.split('=')[1]
                        
                        qimage = QImage(img_elem.geometry().size(), QImage.Format_ARGB32)
                        painter = QPainter(qimage)
                        img_elem.render(painter)
                        painter.end()
                        
                        # get boss name link
                        boss_name_link = '%s/%s' % (home_url, link)
                        # save to local img
                        fpath = r'.\save_boss_img\%s.jpg' % record_id
                        
                        qimage.save(fpath)
                        # write to dict
                        cago_dict['boss_name'] = str(param)
  
            td_idx += 1
            td = td.nextSibling()
        
        elem = elem.nextSibling()
    
    print cago_dict
    print cago_name_link
    print boss_name_link
    updateRecord(cago_dict)
    #fpath = r'.\save_record\%s.txt' % record_id
    #json.dump(cago_dict, open(fpath, 'w'))
    
    # then save to file

# analyze Master page
def workMaster():
    cur_url = str(gui.winMaster.browser.url().toString().toUtf8())
    print cur_url
    
    document = gui.winMaster.browser.page().mainFrame().documentElement()
    
    elem = document.findFirst('.mar10')
    elem = elem.findFirst('a')
            
    rows = []
    while not elem.isNull():
        row = []
        url = elem.attribute('href')
        row.append(url)

        li = elem.findFirst('li')
        while not li.isNull():
            v = str(li.toPlainText().trimmed().toUtf8()).decode('utf8')
            row.append(v)
            li = li.nextSibling()
        
        rows.append(row)
        elem = elem.nextSibling()
    stop = False
    for r in rows:
        record_id = r[0].split('=')[1]
        print 'insert code :' + record_id
        if checkEnd(record_id):
            stop = True
            break;
        createRecord(r)
        link = '%s/%s' % (home_url, r[0])
        #print link

        cago_dict = {}    
        cago_dict['record_id'] = str(record_id)
        cago_dict['cago_name'] = r[1].decode('utf8')
        print cago_dict['cago_name']
        print cago_dict
    
        #fpath = r'.\save_record_master\%s.txt' % record_id
        #json.dump(cago_dict, open(fpath, 'w'))
          
        gui.slaverQueue.put(link)
        #gui.winSlave.load_url(link)
    
    if stop == False:
        next_page = ''
    
        elem = document.findFirst('#AspNetPager1').findFirst('a')
        while not elem.isNull():
            v = str(elem.toPlainText().trimmed().toUtf8()).decode('utf8')
            if v == u'下一页':
                next_page = elem.attribute('href')
                break
            elem = elem.nextSibling()
    
        link = '%s/%s' % (home_url, next_page)
        gui.masterQueue.put(link)
    else:
        gui.masterQueue.put('STOP')
class sql:
    def __init__(self,host='120.26.239.236',user='waterway',passwd='shuiyun123',db='waterway',port=3306 ):
        self.host=host
        self.user=user
        self.passwd=passwd
        self.db=db
        self.port=port
         
    def getSqlResult(self,sql):
        conn=MySQLdb.connect(self.host,self.user,self.passwd,self.db,self.port,use_unicode=True, charset="utf8")
        cur=conn.cursor()
        cur.execute(sql)
        result=cur.fetchall()
        conn.close()
        return result
     
    def executeSql(self,sql):
        conn=MySQLdb.connect(self.host,self.user,self.passwd,self.db,self.port,use_unicode=True, charset="utf8")
        cur=conn.cursor()
        cur.execute(sql)
        conn.commit()
        conn.close    
    
print 'Main start...'
print 'Step 1. Open home url in Master window'
print 'Step 2. Login in Master window'
print 'Step 3. Load cago url'
print 'Step 4. Open Slave window'
print 'Step 5. Click run'


home_url = 'http://wap.cjsyw.com'
cago_url = 'http://wap.cjsyw.com/cy_index.aspx'

# define a time, let the spider not run too fast
waterRunTimer = QTimer()
waterRunTimer.timeout.connect(waterRun)

STATUS_ZERO = 0
STATUS_PREPARE = 1
STATUS_LOGIN = 2
STATUS_RUN = 3
STATUS_STOP = 4

run_status = STATUS_ZERO

app = QApplication(sys.argv)
gui = MainWindow(home_url)
gui.show()
    
gui.newActionPrepare()
sys.exit(app.exec_())