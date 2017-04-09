from tkinter import *
# import _mssql
# import socket
# import decimal
# import uuid
import pymssql
import pandas_datareader.data as pd
import datetime
import numpy as np
import time


class pandas_data:

    def __init__(self, db_adr, id, pw, db_name, ticker, ticker_name, code_col, code_name_col, gubun, code_tab, start, end, during, insert_tab, many_code):
        self.db_adr = db_adr
        self.id = id
        self.pw = pw
        self.db_name = db_name
        self.ticker = ticker
        self.ticker_name = ticker_name
        self.code_col = code_col
        self.code_name_col = code_name_col
        self.code_tab = code_tab
        self.end = end
        self.start = start
        self.during = during
        self.insert_tab = insert_tab
        self.many_code = many_code
        self.gubun = gubun

        if (self.end == ''):
            self.end = datetime.datetime.now().strftime('%Y%m%d')
        if (self.start == '' and isinstance(during, int) and during > 0):
            self.start = (datetime.datetime.now() + datetime.timedelta(days=-1 * self.during)).strftime('%Y%m%d')
        elif (self.start == ''):
            self.start = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y%m%d')



    def shcode_load(self):

        shcode_list = []

        ## shcode_load
        conn = pymssql.connect(host=self.db_adr, user=self.id, password=self.pw, database=self.db_name,charset="utf8", as_dict=True)
        cur = conn.cursor()
        if (self.ticker == '' and self.ticker_name == ''):
            sql01 = """select top %d %s
                        ,case when %s = 'KOSPI' then 'KRX' else %s end as %s
                         From %s 
                        order by 1;""" % (int(self.many_code), self.code_col, self.gubun, self.gubun, "gubun", self.code_tab)
        else:
            sql01 = """select distinct %s
                        ,case when %s = 'KOSPI' then 'KRX' else %s end as %s
                         From %s 
                        where %s = '%s'
                           or %s = '%s' 
                        order by 1;""" % (self.code_col, self.gubun, self.gubun, "gubun", self.code_tab, self.code_col, self.ticker, self.code_name_col, self.ticker_name)
        cur.execute(sql01)
        for row in cur:
            shcode_list.append(list(row.values()))

        return shcode_list



    def pandas_load(self,shcode_list):
        pd_list = []
        pivot_data2 = []
        error_cnt = 0

        start_time_pandas = time.time()
        try:
            # print('%d : %s code processing' % (sh_cnt, shcode_list[sh_cnt][0]))
            data = pd.DataReader(shcode_list[0]+':' + shcode_list[1], 'google', self.start, self.end)
            date = list(data.index.strftime('%Y%m%d'))
            shcode = []
            for i in range(len(date)):
                shcode.append(shcode_list[0])
            open = list(np.nan_to_num(data['Open']))
            high = list(np.nan_to_num(data['High']))
            low = list(np.nan_to_num(data['Low']))
            close = list(np.nan_to_num(data['Close']))
            vol = list(np.nan_to_num(data['Volume']))
            pd_list = [date, shcode, open, high, low, close, vol]
            pivot_data = []
            for a in range(len(date)):
                for b in range(len(pd_list)):
                    pivot_data.append(pd_list[b][a])
                pivot_data2.append(pivot_data)
                pivot_data = []

        except:
            error_cnt = error_cnt + 1
            pass


        end_time_pandas = time.time()
        elaps_pandas = end_time_pandas - start_time_pandas


        #insert
        conn = pymssql.connect(host=self.db_adr, user=self.id, password=self.pw, database=self.db_name,charset="utf8", as_dict=True)
        cur = conn.cursor()

        # delete stage
        sql03 = """delete From %s
                         where yyyymmdd_surid between %d and %d
                           and shcode = %s;""" % (self.insert_tab, int(self.start), int(self.end), "'" + shcode_list[1] + "'")
        ret_del = cur.execute(sql03)


        insert_cnt = 0
        start_time_insert = time.time()
        for x in range(len(pivot_data2)):
            sql02 = "insert into %s select %d, %s, %d, %d, %d, %d, %d;" % (self.insert_tab, int(pivot_data2[x][0]), "'" + pivot_data2[x][1] + "'", pivot_data2[x][2],pivot_data2[x][3], pivot_data2[x][4], pivot_data2[x][5], pivot_data2[x][6])
            ret_ins = cur.execute(sql02)
            if(insert_cnt % 1000 == 0):
                time.sleep(1)
            insert_cnt = insert_cnt + 1

        conn.commit()
        conn.close()
        end_time_insert = time.time()
        elaps_insert = end_time_insert - start_time_insert


        return print("--------------------------\n%s Codes loading. \npandas-loading %d sec Elapsed. \ninsert table:%d row(s). %d sec Elapsed. \n%d rows not inserted. " % ("'" + pivot_data2[0][1] + "'", elaps_pandas, insert_cnt, elaps_insert, error_cnt))




a = pandas_data('jesus4000.ipdisk.co.kr:14333', 'jesus', 'Gkr7743s!', 'Project_GG', '', '', 'shcode', 'hname', 'gubun', 't8430','20100101', '', 0, 'pandas', 10000)
b = a.shcode_load()

for i in range(len(b)):
    a.pandas_load(b[i])