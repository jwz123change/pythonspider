# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from douban.settings import sql_host,sql_db_name,sql_user,sql_password,sql_sheetname

class DoubanPipeline(object):
    def __init__(self):
        host = sql_host
        dbname = sql_db_name
        ruser = sql_user
        rpassword = sql_password
        self.sheetname = sql_sheetname
        # 连接数据库
        self.conn = pymysql.connect(host=host, user=ruser, password=rpassword, db=dbname, charset='utf8')
        # 创建一个游标
        self.cursor = self.conn.cursor()


    # 防止连接出现错误
    def open_spider(self, spider):
        try:
            host = sql_host
            dbname = sql_db_name
            ruser = sql_user
            rpassword = sql_password
            self.sheetname = sql_sheetname
            # 连接数据库
            self.conn = pymysql.connect(host=host, user=ruser, password=rpassword, db=dbname, charset='utf8')
            # 创建一个游标
            self.cursor = self.conn.cursor()
        except:
            self.open_spider()
        else:
            spider.logger.info('MySQL: connected')
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
            spider.cursor = self.cursor


    def process_item(self, item, spider):
        # item是从douban_spider里传出的数据
        # 先将数据转换为字典形式
        data = dict(item)
        # mysql插入数据
        keys = ','.join(data.keys())
        values = ','.join(['%s'] * len(data))
        sql = 'INSERT INTO {table}({keys}) VALUES({values})'.format(table=self.sheetname, keys=keys, values=values)
        try:
            self.cursor.execute(sql, tuple(data.values()))
            self.conn.commit()
        except Exception as e:
            print(e.args)
            self.conn.rollback()
        # self.cursor.close()
        # self.conn.close()
        # mongodb插入数据
        # self.post.insert(data)
        return item


    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
        def process_item(self, item, spider):
            return item
