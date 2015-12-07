#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = 'Finalcheat'

import MySQLdb


class mysql_obj(object):

    def __init__(self, host = 'localhost', user = 'root', password = '', port = 3306, db_name = '', charset = 'utf8'):
        self._host = host
        self._user = user
        self._password = password
        self._port = port
        self._db_name = db_name
        self._charset = charset
        self._conn = None


    def _get_cursor(self, dict_cursor = True):
        try:
            if not self._conn:
                cursor = self._conn.cursor(MySQLdb.cursors.DictCursor) if dict_cursor else self._conn.cursor()
            else:
                self._conn = MySQLdb.connect(self._host, self._user, self._password, self._db_name, self._port, charset = self._charset)
                cursor = self._conn.cursor(MySQLdb.cursors.DictCursor) if dict_cursor else self._conn.cursor()
            return self._conn, cursor
        except Exception, e:
            raise e


    def exec_sql(self, sql, args = None):
        conn, cursor = self._get_cursor()
        #  print "sql:{}".format(sql)
        #  print "args:{}".format(args)
        cursor.execute(sql, args = args)
        conn.commit()
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result


    def query(self, table_name, query_dict = {}, fields = ['*']):
        query_sql, args = self.__get_sql_by_item_dict(query_dict)
        fields_sql = ', '.join(fields)
        sql = "select {fields} from {table_name} where 1 = 1 {query_sql}".format(fields = fields_sql, table_name = table_name, query_sql = query_sql)
        print "sql:{}".format(sql)
        print "args:{}".format(args)
        return self.exec_sql(sql, args = args)


    def query_one(self, table_name, query_dict = {}, fields = ['*']):
        query_sql, args = self.__get_sql_by_item_dict(query_dict)
        fields_sql = ', '.join(fields)
        sql = "select {fields} from {table_name} where 1 = 1 {query_sql}".format(fields = fields_sql, table_name = table_name, query_sql = query_sql)
        print "sql : {}".format(sql)
        print "args : {}".format(args)
        conn, cursor = self._get_cursor()
        cursor.execute(sql, args = args)
        conn.commit()
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result


    def insert(self, table_name, item_dict = {}):
        args = item_dict.values()
        plac = ', '.join(['`{}`'.format(field) for field in item_dict.keys() ])
        values = ','.join(['%s'] * len(item_dict))
        print values
        sql = "insert into `{table_name}` ({fields}) values ({values})".format(table_name = table_name, fields = plac, values = values)
        print "sql:{}".format(sql)
        print "args:{}".format(args)
        conn, cursor = self._get_cursor()
        cursor.execute(sql, args = args)
        insert_id = conn.insert_id()
        conn.commit()
        cursor.close()
        conn.close()
        return insert_id


    def remove(self, table_name, item_dict = {}):
        query_sql, args = self.__get_sql_by_item_dict(item_dict)
        sql = "delete from `{table_name}` where 1 = 1 {query_sql}".format(table_name = table_name, query_sql = query_sql)
        print 'sql : {}'.format(sql)
        print 'args : {}'.format(args)
        conn, cursor = self._get_cursor()
        cursor.execute(sql, args = args)
        conn.commit()
        cursor.close()
        conn.close()


    def update(self, table_name, item_dict = {}, update_dict = {}):
        query_sql, _args = self.__get_sql_by_item_dict(item_dict)
        set_sql =  ",".join([ "`{field}` = %s".format(field = field) for field in update_dict.keys() ])
        sql = "update `{table_name}` set {set_sql} where 1 = 1 {query_sql}".format(table_name = table_name, set_sql = set_sql, query_sql = query_sql)
        args = update_dict.values() + _args
        print 'sql : {}'.format(sql)
        print 'args : {}'.format(args)
        conn, cursor = self._get_cursor()
        cursor.execute(sql, args = args)
        conn.commit()
        cursor.close()
        conn.close()


    def __get_sql_by_item_dict(self, item_dict):
        sql = ''
        args = []
        for (field, value) in item_dict.items():
            if isinstance(value, dict):
                for (_k, _v) in value.items():
                    op = self.__get_op_by_key(_k)
                    #  _v = dealWithValue(_v)
                    sql += " and `{field}` {op} %s ".format(field = field, op = op)
                    args.append(_v)
            else:
                #  value = dealWithValue(value)
                sql += " and `{field}` = %s ".format(field = field)
                args.append(value)

        return sql, args


    def __get_op_by_key(self, key):
        op_dict = {
            '$gt'  : '>',
            '$gte' : '>=',
            '$lt'  : '<',
            '$lte' : '<=',
            '$eq'  : '=',
            '$ne'  : '!=',
            '$in'  : 'in',
            '$nin' : 'not in',
        }
        try:
            return op_dict[key];
        except:
            raise
