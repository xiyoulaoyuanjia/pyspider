#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: xiyoulaoyuanjia
# Created on 2015-12-07 22:56:19
from pyspider.result import ResultWorker
from pyspider.libs import counter, utils
import os

import json
import logging
import io
import time

import cv2
from ftplib import FTP
import ftplib
import pymssql
logger = logging.getLogger("ResultWorkerWatoo")

'''
jpg 基本信息如下:
===========================
            ##图片下载地址
            'download_url':'',
            ## 图片来源地址
            'source_url' : 'www.tumblr.com',
            ## 图片来源网站名称
            'orginal_name': 'tumblr',
            ## 图片名字
            'name' : '',
=========================

            ## 图片宽度
            'pic_width' : '',
            ## 图片高度
            'pic_height' : '',
            ##图片均色
            'avg_color' : '',

'''
class ResultWorkerWatoo(ResultWorker):
    def __init__(self, resultdb, inqueue):
        super(ResultWorkerWatoo,self).__init__(resultdb,inqueue)
#        self.ftpClinet = self.ftpClinetInit();
#        self.pymssqlClinet = self.pymssqlClinetInit();
        self.data_path='./data'
        self._quit = False
        self._cnt = {
            "5m_time": counter.CounterManager(
                lambda: counter.TimebaseAverageEventCounter(30, 10)),
            "5m": counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(30, 10)),
            "1h": counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(60, 60)),

            "1day": counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(10 * 60, 24 * 6)),
            ## 1 week
            "1week": counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(10 * 60 * 7, 24 * 6)),
            ##  1 month ~ 30week
            "1month": counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(10 * 60 * 7 * 30, 24 * 6)),
            ##  1 year
            "1year": counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(10 * 60 * 7 * 30 * 12, 24 * 6)),
            "all": counter.CounterManager(
                lambda: counter.TotalCounter()),
        }   
        self._cnt['1h'].load(os.path.join(self.data_path, 'result_watoo.1h'))
        self._cnt['1day'].load(os.path.join(self.data_path, 'result_watoo.1day'))
        self._cnt['1week'].load(os.path.join(self.data_path, 'result_watoo.1week'))
        self._cnt['1month'].load(os.path.join(self.data_path, 'result_watoo.1month'))
        self._cnt['1year'].load(os.path.join(self.data_path, 'result_watoo.1year'))

        self._cnt['all'].load(os.path.join(self.data_path, 'result_watoo.all'))
        self._last_dump_cnt = 0


    def  pymssqlClinetInit(self):
        conn = pymssql.connect(host="10.163.169.69",user="newuser",password= "@123456#",database= "WatuMeiDa",charset="utf8")
        return conn

    def pymssqlClinetOp(self,sql):
        try:
            curs = self.pymssqlClinet.cursor()
            curs.execute(sql)
        except pymssql.DatabaseError, err:
            time.sleep(0.2)
            self.pymssqlClinetInit()
            curs = self.pymssqlClinet.cursor()
            curs.execute(sql)
        self.pymssqlClinet.commit()

    def ftpClinetInit(self):
        ftp=FTP()
        retry = True
        retryNum = 1
        ## retry until success
        while(retry):
             try:
                 ftp.set_debuglevel(0)
                 ftp.connect('115.28.80.218','20')
                 ftp.login('newuser','@123456#')
                 retry = False
             except:
                 import time
                 time.sleep(10*retryNum)
                 retryNum += 1

        return ftp

    def ftpClinetUpload(self, name):
        try:
            self.ftpClinet.voidcmd("NOOP")
        except ftplib.error_temp as e: #421 Connection timed out
            logger.warning("Connection timed out and retry Connection")
            self.ftpClinet = self.ftpClinetInit();

        try:
            self.ftpClinet.cwd("/"+name[0:8])
        except ftplib.error_perm as e: # 550 CWD failed.
            logger.warning("cwd failed and  retry mkdir")
            self.ftpClinet.mkd("/"+name[0:8])
            self.ftpClinet.cwd("/"+name[0:8])


        file_handler = open("pyspider/data/tmp/"+name,'rb')#以读模式在本地打开文件
        bufsize = 1024#设置缓冲块大小
    #    file = io.BytesIO(download_url_content.encode("utf-8"))
        self.ftpClinet.storbinary("STOR %s"% name , file_handler, bufsize)
        file_handler.close()

    def __get_pic_inf(self,name):
            logger.info("解析jpg图片,获取图片相关信息(长,宽,rbg均色)开始 :" + name )
            _cv2_name = "pyspider/data/tmp/" + name
            logger.debug("解析jpg图片,图片名称为 :" +  _cv2_name )
            _re_img = cv2.imread(_cv2_name)
            try:
                _b, _g, _r = cv2.split(_re_img)
            except ValueError: ##图挂了
                return ('0','0','0')
            _sum_b_g_r = sum((sum(x) for x in _b))+ sum((sum(x) for x in _r)) + sum((sum(x) for x in _g))
            _width  = len(_b)
            _height = len(_b[0])
            _avg_color = _sum_b_g_r / (_width * _height)
            logger.info("解析jpg图片,获取图片相关信息(长,宽,rbg均色) 分别为 :" + str(_height) + "," + str(_width) + "," + str(_avg_color) )
            return (str(_avg_color), str(_width), str(_height))

    def _dump_cnt(self):
        '''Dump counters to file'''
        self._cnt['1h'].dump(os.path.join(self.data_path, 'result_watoo.1h'))
        self._cnt['1day'].dump(os.path.join(self.data_path, 'result_watoo.1day'))
        self._cnt['1week'].dump(os.path.join(self.data_path, 'result_watoo.1week'))
        self._cnt['1month'].dump(os.path.join(self.data_path, 'result_watoo.1month'))
        self._cnt['1year'].dump(os.path.join(self.data_path, 'result_watoo.1year'))

        self._cnt['all'].dump(os.path.join(self.data_path, 'result_watoo.all'))

    def _try_dump_cnt(self):
        '''Dump counters every 60 seconds'''
        now = time.time()
        if now - self._last_dump_cnt > 60:
            self._last_dump_cnt = now
            self._dump_cnt()
            #self._print_counter_log()

    ## when a new result arrivel
    def _on_new_request(self, task, result):

        project = task['project']
        self._cnt['5m'].event((project, 'success'), +1)
        self._cnt['1h'].event((project, 'success'), +1)
        self._cnt['1day'].event((project, 'success'), +1)
        self._cnt['1week'].event((project, 'success'), +1)
        self._cnt['1month'].event((project, 'success'), +1)
        self._cnt['1year'].event((project, 'success'), +1)
        self._cnt['all'].event((project, 'success'), +1)

        self._try_dump_cnt()

        logger.info('new result %(project)s:%(taskid)s %(url)s', task)



    def on_result(self, task, result):
        ## I must get the result from webui
        logger.info('task ->  %s'  %  task)
        logger.info('result ->  %s'  %  result)

        avg_color, pic_width, pic_height = self.__get_pic_inf(result['name'])


        self.ftpClinetUpload(result['name'])

        _now_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        _sql_name = time.strftime('%Y%m%d/') + result['name']
        _sql = "insert into Pic_Temp_Process(Pic_Name, Pic_Width,Pic_Height, AuditStatus, UpdatedDate,CreatedDate, UserReplyCount, Audit_ErrorIndex, AvgColor, OriginalName, SourceUrl) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s') " % (_sql_name, pic_width, pic_height, '0', _now_time, _now_time , '0', '0',avg_color, result['orginal_name'], result['source_url'].encode('utf-8'))

        self.pymssqlClinetOp(_sql)
        self._on_new_request(task,result)        
        ResultWorker.on_result(self, task, result)

    def quit(self):
        '''Set quit signal'''
        self._quit = True

    def xmlrpc_run(self, port=26666, bind='127.0.0.1', logRequests=False):
            '''Start xmlrpc interface'''
            try:
                from six.moves.xmlrpc_server import SimpleXMLRPCServer
            except ImportError:
                from SimpleXMLRPCServer import SimpleXMLRPCServer

            server = SimpleXMLRPCServer((bind, port), allow_none=True, logRequests=logRequests)
            server.register_introspection_functions()
            server.register_multicall_functions()

            server.register_function(self.quit, '_quit')
            #server.register_function(self.__len__, 'size')

            def dump_counter(_time, _type):
               try:
                    return self._cnt[_time].to_dict(_type)
               except :

                    logger.exception("")

            server.register_function(dump_counter, 'counter')

            server.timeout = 0.5
            while not self._quit:
                server.handle_request()
            server.server_close()

    

    #def on_download(self, url, name):
