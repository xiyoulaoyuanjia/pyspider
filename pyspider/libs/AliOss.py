# -*- coding: utf-8 -*-

import os
import sys
sys.path.append('pyspider/libs/aliyun_oss_python_sdk')
import oss2
import logging
import time

class AliyunOssPython(object):

    def __init__(self):
        self.access_key_id = u'X9X0UvXa5I2wD3LX'
        self.access_key_secret = u'WK05nZYHhfS9vOfjrxRRwB3rn1XIsN'
        self.bucket_name = u'watoo-images'
        self.endpoint = u'http://oss-cn-qingdao.aliyuncs.com'
        self.bucket = oss2.Bucket(oss2.Auth(self.access_key_id,
            self.access_key_secret), self.endpoint, self.bucket_name)

        self.tmp_file_dir = 'pyspider/data/tmp/'

    def upload(self, name, count=1):
        try:
            logging.info("try %s upload" % count)
            self.bucket.put_object_from_file(name[0:8] +'/'+ name, self.tmp_file_dir + name)
#        except oss2.exceptions.OssError:
        except:
            time.sleep(count)
            self.__init__()
            if(count > 5):
                assert False
            else:
                self.upload(name, count+1)


        os.remove(self.tmp_file_dir + name)

if __name__ == '__main__':
    a = AliyunOssPython()
    print a.tmp_file_dir
    a.upload('yuanjia')
