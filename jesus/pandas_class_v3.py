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


class RunIns(Frame):

    def __init__(self,master):
        Frame.__init__(self,master)
        msg = ''

        self.master = master
        self.master.title("Data Downloader")
        self.pack(fill=BOTH, expand=True)

        frame01 = Frame(self)
        frame01.pack(fill = X)
        inputLab01 = Label(frame01,text='DB Address', width = 25, justify='left')
        inputLab01.grid(row=0)
        # inputLab01.pack(side=LEFT)
        self.labEntry01 = Entry(frame01)
        self.labEntry01.grid(row=0, column=1)

        frame02 = Frame(self)
        frame02.pack(fill = X)
        inputLab02 = Label(frame02,text='DB ID', width = 25 )
        inputLab02.grid(row=1)
        self.labEntry02 = Entry(frame02)
        self.labEntry02.grid(row=1, column=1)

        frame03 = Frame(self)
        frame03.pack(fill = X)
        inputLab03 = Label(frame03,text='DB PW', width = 25 )
        inputLab03.grid(row=2)
        self.labEntry03 = Entry(frame03)
        self.labEntry03.grid(row=2, column=1)

        frame04 = Frame(self)
        frame04.pack(fill = X)
        inputLab04 = Label(frame04,text='DB NAME', width = 25 )
        inputLab04.grid(row=3)
        self.labEntry04 = Entry(frame04)
        self.labEntry04.grid(row=3, column=1)

        frame05 = Frame(self)
        frame05.pack(fill=X)
        inputLab05 = Label(frame05, text='종목코드', width = 25)
        inputLab05.grid(row=4)
        self.labEntry05 = Entry(frame05)
        self.labEntry05.grid(row=4, column=1)

        frame06 = Frame(self)
        frame06.pack(fill=X)
        inputLab06 = Label(frame06, text='종목명', width = 25)
        inputLab06.grid(row=5)
        self.labEntry06 = Entry(frame06)
        self.labEntry06.grid(row=5, column=1)

        frame07 = Frame(self)
        frame07.pack(fill=X)
        inputLab07 = Label(frame07, text='종목코드컬럼', width = 25)
        inputLab07.grid(row=6)
        self.labEntry07 = Entry(frame07)
        self.labEntry07.grid(row=6, column=1)

        frame08 = Frame(self)
        frame08.pack(fill=X)
        inputLab08 = Label(frame08, text='종목명컬럼', width = 25)
        inputLab08.grid(row=7)
        self.labEntry08 = Entry(frame08)
        self.labEntry08.grid(row=7, column=1)

        frame09 = Frame(self)
        frame09.pack(fill=X)
        inputLab09 = Label(frame09, text='KOSPI/KOSDAQ 구분', width = 25)
        inputLab09.grid(row=8)
        self.labEntry09 = Entry(frame09)
        self.labEntry09.grid(row=8, column=1)

        frame10 = Frame(self)
        frame10.pack(fill=X)
        inputLab10 = Label(frame10, text='종목코드 테이블명', width = 25)
        inputLab10.grid(row=9)
        self.labEntry10 = Entry(frame10)
        self.labEntry10.grid(row=9, column=1)

        frame11 = Frame(self)
        frame11.pack(fill=X)
        inputLab11 = Label(frame11, text='시작일자 (ex)2017-01-01', width = 25)
        inputLab11.grid(row=10)
        self.labEntry11 = Entry(frame11)
        self.labEntry11.grid(row=10, column=1)

        frame12 = Frame(self)
        frame12.pack(fill=X)
        inputLab12 = Label(frame12, text='종료일자 (ex)2017-01-01', width = 25)
        inputLab12.grid(row=11)
        self.labEntry12 = Entry(frame12)
        self.labEntry12.grid(row=11, column=1)

        frame13 = Frame(self)
        frame13.pack(fill=X)
        inputLab13 = Label(frame13, text='기간', width = 25)
        inputLab13.grid(row=12)
        self.labEntry13 = Entry(frame13)
        self.labEntry13.grid(row=12, column=1)

        frame14 = Frame(self)
        frame14.pack(fill=X)
        inputLab14 = Label(frame14, text='적재할 테이블명', width = 25)
        inputLab14.grid(row=13)
        self.labEntry14 = Entry(frame14)
        self.labEntry14.grid(row=13, column=1)

        frame15 = Frame(self)
        frame15.pack(fill=X)
        inputLab15 = Label(frame15, text='적재종목 수(종목코드 ASC)', width = 25)
        inputLab15.grid(row=14)
        self.labEntry15 = Entry(frame15)
        self.labEntry15.grid(row=14, column=1)

        frame16 = Frame(self)
        frame16.pack(fill=X)
        inputLab15 = Label(frame16, text=msg, width = 50)
        inputLab15.grid(row=15)
        # self.labEntry15 = Entry(frame16)
        # self.labEntry15.grid(row=14, column=1)


        self.runBtn = Button(root, text = "RUN", command = self.pandas_import)
        # self.runBtn.grid(row=17,column=0)
        self.runBtn.pack(side=LEFT)

        self.quitBtn = Button(root, text = "CLOSE", command = root.quit)
        # self.quitBtn.grid(row=17, column=1)
        self.quitBtn.pack(side = RIGHT)

    def pandas_import(self):
        # try:
        if(self.labEntry01.get()=='gkr1'):
            a = pandas_data('localhost', 'jesus', '3477', 'test', '', '', 'shcode', 'hname', 'gubun', 't8430','', '', 5, 'pandas', 3)
        elif(self.labEntry01.get()=='gkr2'):
            a = pandas_data('', 'jesus', '', '', '', '', 'shcode', 'hname', 'gubun', 't8430',self.labEntry11.get(), self.labEntry12.get(), self.labEntry13.get(), 'pandas',self.labEntry15.get())
        else:
            a = pandas_data(self.labEntry01.get(),self.labEntry02.get(),self.labEntry03.get(),self.labEntry04.get(),self.labEntry05.get(),self.labEntry06.get(),self.labEntry07.get(),self.labEntry08.get(),self.labEntry09.get(),self.labEntry10.get(),self.labEntry11.get(),self.labEntry12.get(),self.labEntry13.get(),self.labEntry14.get(),self.labEntry15.get())
        a.shcode_load()
        # except:
        #     print('Please check your input info')
        #     self.msg = 'Please check your input info'



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
        pd_list = []
        pivot_data2 = []




    def shcode_load(self):

        shcode_list = []
        pivot_data2 = []
        error_cnt = 0

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



        #pandas
        start_time_pandas = time.time()
        for sh_cnt in range(len(shcode_list)):
            try:
                # print('%d : %s code processing' % (sh_cnt, shcode_list[sh_cnt][0]))
                data = pd.DataReader(shcode_list[sh_cnt][1]+':' + shcode_list[sh_cnt][0], 'google', self.start, self.end)
                date = list(data.index.strftime('%Y%m%d'))
                shcode = []
                for i in range(len(date)):
                    shcode.append(shcode_list[sh_cnt][0])
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



        ########### delete stage
        #
        #
        # sql03 = """delete From %s
        #                  where yyyymmdd_surid = %d
        #                    and shcode = %s;""" % (
        #     self.insert_tab, int(pivot_data2[x][3]), "'" + pivot_data2[x][1] + "'")
        ##########


        insert_cnt = 0
        start_time_insert = time.time()
        for x in range(len(pivot_data2)):
            sql02 = "insert into %s select %d, %s, %d, %d, %d, %d, %d;" % (self.insert_tab, int(pivot_data2[x][0]), "'" + pivot_data2[x][1] + "'", pivot_data2[x][2],pivot_data2[x][3], pivot_data2[x][4], pivot_data2[x][5], pivot_data2[x][6])
            # row_start_time = time.time()
            ret_ins = cur.execute(sql02)
            # row_end_time = time.time()
            # elaps_by_row = row_end_time - row_start_time
            # elaps_accu_row = row_end_time - start_time_insert
            # if(insert_cnt == 0):
            #     sec_per_ins = float(0)
            # else:sec_per_ins = elaps_accu_row / float(insert_cnt)
            # print('Insert row per %d (sec). Insert speed is %0.1f (unit/sec)' % (elaps_by_row, sec_per_ins))
            print('%d / %s inserted.' % (int(pivot_data2[x][0]), "'" + pivot_data2[x][1] + "'"))
            if(insert_cnt % 1000 == 0):
                time.sleep(1)
            insert_cnt = insert_cnt + 1

        conn.commit()
        conn.close()
        end_time_insert = time.time()
        elaps_insert = end_time_insert - start_time_insert

        return print("--------------------------\n%d Codes loading. \npandas-loading %d sec Elapsed. \ninsert table:%d row(s). %d sec Elapsed. \n%d rows not inserted. " % (int(self.many_code), elaps_pandas, insert_cnt, elaps_insert, error_cnt))


root = Tk()
b = RunIns(root)
root.mainloop()
