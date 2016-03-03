#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: xiyoulaoyuanjia
# Created on 2015-12-07 22:56:19
from pyspider.result import ResultWorker
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
        self.ftpClinet = self.ftpClinetInit();
        self.pymssqlClinet = self.pymssqlClinetInit();

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
        ftp.set_debuglevel(0)
        ftp.connect('115.28.80.218','20')
        ftp.login('newuser','@123456#')
        return ftp

    def ftpClinetUpload(self, name):
        try:
            self.ftpClinet.dir()
        except ftplib.error_temp as e: #421 Connection timed out
            logger.warning("Connection timed out and retry Connection")
            self.ftpClinet = self.ftpClinetInit();

        try:
            self.ftpClinet.cwd("/"+name[0:8])
        except ftplib.error_perm as e: # 550 CWD failed.
            logger.warning("cwd failed and  retry mkdir")
            self.ftpClinet.mkd(name[0:8])
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

        ResultWorker.on_result(self, task, result)



    #def on_download(self, url, name):
