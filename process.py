#!/usr/bin/python3.5
# -*- coding: utf-8 -*-
#authror: malu
#备份uhf_record中记录，每个月备份一次

import sys
import time
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import timedelta
import pymysql
import os




#数据库配置
DBPARAMS = {
    "host":"xx.xx.xx.xx",
    "port":8066,
    "user":"xx",
    "password":"xxxxxxxx",
    "database":"xxxx",
    "charset": ""
}

#这里使用select into 来备份，数据校验对比记录数，一个月大概100w条数据
#radacct2015
#检查表，检查重传，备份，校验

create_table_sql = '''
CREATE TABLE `{0}` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT COMMENT '标签读取信息表',
  `position_id` varchar(24) NOT NULL DEFAULT '',
  `device_sn` varchar(32) NOT NULL,
  `device_ip` varchar(20) NOT NULL,
  `wire_id` int(11) NOT NULL COMMENT '天线编号',
  `RSSI` int(11) NOT NULL COMMENT '信号强度',
  `epc` varchar(50) DEFAULT NULL,
  `tid` varchar(25) NOT NULL COMMENT '标签tid(卡号) ',
  `time` datetime NOT NULL,
  `insert_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `record_index` (`time`,`device_sn`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8
'''


back_sql = '''
INSERT INTO {0}
SELECT *
FROM {1}
WHERE insert_time < 
   STR_TO_DATE('{2}', '%Y-%m-%d')
AND insert_time >= 
   STR_TO_DATE('{3}', '%Y-%m-%d')
'''


count_sql = """
SELECT count(*) FROM {0} WHERE 1=1 AND
insert_time < 
   STR_TO_DATE('{1}', '%Y-%m-%d')
AND insert_time >= STR_TO_DATE('{2}', '%Y-%m-%d')
"""

def getLogger():
    formatter = logging.Formatter('%(asctime)s:%(filename)s:%(funcName)s:[line:%(lineno)d] %(levelname)s %(message)s')
    CURRENT_DIR = os.path.dirname(__file__)
    LOG_FILE = os.path.abspath(os.path.join(CURRENT_DIR, "logs", "sys.log"))
    fileTimeHandler = TimedRotatingFileHandler(LOG_FILE, "D", 1, 0)


    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                        datefmt='%a, %d %b %Y %H:%M:%S',
                        filemode='a')
    fileTimeHandler.suffix = "%Y%m%d.log"
    fileTimeHandler.setFormatter(formatter)
    logging.getLogger('').addHandler(fileTimeHandler)

    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    return logging

logger = getLogger()
#date tools
def get_year(month):
    #month like 201505
    return datetime.datetime.strptime(month, "%Y%m").year


def get_month_firstday_str(month):
    return datetime.datetime.strptime(month,"%Y%m").\
                                        strftime("%Y-%m-%d")

def get_next_month_firstday_str(month):
    month_firstday = datetime.datetime.strptime(month,"%Y%m")
    monthnum = month_firstday.month
    return "{0}-{1}-{2}".format(
            month_firstday.year if monthnum < 12 else \
                                 month_firstday.year + 1,
            monthnum + 1 if monthnum < 12 else 1, 1)

def get_current_month_prev():
  #返回类似 "201809"
  now = datetime.datetime.now()
  year = now.year
  month = now.month - 1
  result = "{0}{1}".format(year, month)
  print (result)
  return result

class DBConn(object):
    __CONFIG = {
        'default': {
            'host': "",
            "port":"",
            'user': "",
            'database': "",
            'password': "",
            'charset': "",
        }
    }

    def __init__(self, connname='', connconfig={}):
        if connconfig:
            self.connconfig = connconfig
        else:
            connname = connname or 'default'
            self.connconfig = self.__CONFIG.get(connname, 'default')
        self.conn = None

    def __enter__(self):
        try:
            self.conn = pymysql.connect(
                user=self.connconfig['user'],
                db=self.connconfig['database'],
                passwd=self.connconfig['password'],
                host=self.connconfig['host'],
                port = self.connconfig['port'],
                use_unicode=True,
                charset=self.connconfig['charset'] or "utf8",
                #cursorclass=MySQLdb.cursors.DictCursor
                )

            return self.conn
        except Exception as e:
            logger.error(e)
            return None

    def __exit__(self, exe_type, exe_value, exe_traceback):
        if exe_type and exe_value:
            print ('%s: %s' % (exe_type, exe_value))
        if self.conn:
            self.conn.close()


class RadiusBackup(object):
    def __init__(self, month, conn):
        self.conn = conn
        self.cursor = conn.cursor()
        self.month = month
        self.year = get_year(month)
        self.month_firstday = get_month_firstday_str(month)
        logger.info("month_firstday：{0}".format(self.month_firstday))
        self.next_month_firstday = get_next_month_firstday_str(month)
        self.tablename = "uhf_record{0}".format(self.year)
        self.stable = "uhf_record"


    def check_table_exist(self):
        logger.info("正在查询备份表是否存在...")
        check_table_sql = "SHOW TABLES LIKE '{0}'".format(
                            self.tablename)
        self.cursor.execute(check_table_sql)
        res = self.cursor.fetchall()
        return True if len(res) > 0 else False


    def create_backup_table(self):
        logger.info("正在创建备份表...")
        sql = create_table_sql.format(self.tablename)
        self.cursor.execute(sql)
        logger.info(u"开始创建备份表 {0}".format(self.tablename))


    def check_datas_count(self, tablename):
        logger.info("正在检查备份表条目...")
        sql = count_sql.format(tablename, self.next_month_firstday,
                    self.month_firstday)
        logger.debug(sql)
        self.cursor.execute(sql)
        res = self.cursor.fetchone()
        return res[0]


    def check_before(self):
        flag = False
        #check table
        if not self.check_table_exist():
            self.create_backup_table()
            if self.check_table_exist() == False:
                logger.error(u"无法找到备份表 exit")
                return flag
        #check datas
        if self.check_datas_count(self.tablename) > 0:
            return flag
        else:
            return True


    def backup_datas(self):
        logger.info("正在进行备份...")
        sql = back_sql.format(self.tablename, self.stable,
                self.next_month_firstday, self.month_firstday)
        logger.debug(sql)
        self.cursor.execute(sql)
        self.conn.commit()


    def check_after(self):
        logger.info("正在进行备份后的检查...")
        snum = self.check_datas_count(self.stable)
        bnum = self.check_datas_count(self.tablename)
        if snum > 0 and (snum == bnum):
            logger.info(u"备份成功")
            return snum, True
        else:
            return -1, False

    def backup_handler(self):
        if self.check_before():
            logger.info(u"检查完毕，开始备份数据")
            self.backup_datas()
            logger.info(u"开始备份")
            num, flag = self.check_after()
            logger.info(u"本次备份{0} 数据 {1}条".format(self.month, num))
        else:
            logger.info(u"数据已经有备份，请检查")


if __name__ == "__main__":
    starttime = datetime.datetime.now()
    if(len(sys.argv) > 1):
      month = sys.argv[1]
    else:
      month = get_current_month_prev()
    logger.info(u"备份的月份是:{0}".format(month));
    with DBConn(connconfig=DBPARAMS) as dbconn:
        if dbconn:
            backup = RadiusBackup(month, dbconn)
            backup.backup_handler()
        else:
            logger.error("can not connect to db")

    endtime = datetime.datetime.now()
    logger.info(u"***********备份执行结束，耗时(秒):{0}******************".format((endtime - starttime).seconds))