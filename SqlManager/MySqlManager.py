# -*- coding: utf-8 -*-
import pymysql
import uuid

import configparser


class MySqlManager:
    def __init__(self):  # 构造方法
        cf = configparser.ConfigParser()
        cf.read("config/mysqlconf.ini")

        self.DATABASE = cf.get("app_info", "DATABASE")
        self.USER = cf.get("app_info", "USER")
        self.PASSWORD = cf.get("app_info", "PASSWORD")
        self.HOST = cf.get("app_info", "HOST")
        self.PORT = int(cf.get("app_info", "PORT"))

    def get_connect(self):
        conn = pymysql.connect(host=self.HOST, user=self.USER, passwd=self.PASSWORD, db=self.DATABASE,
                               port=self.PORT, charset='utf8')
        return conn

    # 返回1表示正常，返回-1表示异常
    def operation(self, sql):
        conn = self.get_connect()
        cur = conn.cursor()
        mark = 1
        try:
            cur.execute(sql)
            conn.commit()  # 这个对于增删改是必须的，否则事务没提交执行不成功
        except pymysql.Error as e:
            print(e)
            conn.rollback()
            mark = -1
        finally:
            conn.close()
            return mark

    # 需要同时插入两张表，返回1表示正常，返回-1表示异常
    def insert_book_info(self, dic):
        mark = 1
        metaid = dic['metaid']
        labelid = str(uuid.uuid1())
        year = 0
        if 'year' in dic.keys():
            year = dic['year']
        labels = str(dic['label']).split('&')
        sql1 = 'insert into book(metaid,labelid, year) VALUES (\'' + metaid + '\',\'' + labelid + '\',\'' + year + '\')'
        print(sql1)
        # SQL 插入语句
        sql2 = 'insert into label(labelid, name) VALUES (%s,%s)'
        lst = []
        for name in labels:
            lst.append((labelid, name))

        conn = self.get_connect()
        cursor = conn.cursor()
        try:
            cursor.execute(sql1)
            cursor.executemany(sql2, lst)
            conn.commit()  # 这个对于增删改是必须的，否则事务没提交执行不成功
        except pymysql.Error as e:
            print(e)
            conn.rollback()
        finally:
            # 关闭游标
            cursor.close()
            # 关闭数据库连接
            conn.close()
        return mark

    # 返回列名
    def query_book_id(self, dic):
        ret = []
        labels = str(dic['label']).split('&')
        str_year = ''
        if 'year' in dic.keys():
            str_year = str(dic['year'])
        sql = 'select b.metaid,count(*) as mycount  from book as b join  label as l on b.labelid = l.labelid'
        postfix = 'group by b.metaid order by mycount desc limit 500'
        str_label = ''
        # 拼接label
        if len(labels) == 0:
            return ret
        else:
            for name in labels:
                if str_label == '':
                    str_label = 'l.name=\'' + name + '\''
                else:
                    str_label += ' or l.name=\'' + name + '\''

        sql += ' where (' + str_label + ')'
        # print(sql)

        # 拼接year
        if str_year == '':
            sql += ' ' + postfix
        else:
            sql += ' and (b.year' + str_year + ')' + ' ' + postfix
        print(sql)

        # 打开数据库连接
        conn = self.get_connect()
        # 使用cursor()方法获取操作游标
        cursor = conn.cursor()
        result = []
        try:
            cursor.execute(sql)
            # 获取查询的所有记录
            rows = cursor.fetchall()
            print(type(rows))
            for res in rows:
                result.append(res[0])
        except pymysql.Error as e:
            print(e)
        finally:
            cursor.close()
            conn.close()  # 关闭连接
            return result
