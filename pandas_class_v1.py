import pymssql
import pandas_datareader.data as pd
import datetime

class pandas_data:

    def __init__(self, db_adr, id, pw, db_name, ticker, ticker_name, code_col, code_name_col, code_tab, end, start, during, insert_tab):
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
        if(self.end == '' ):
            self.end = datetime. datetime.now().strftime('%Y%m%d')
        if isinstance (during, int):
            self.start = (datetime. datetime. now() + datetime.timedelta(days=-1*self.during)).strftime('%Y%m%d')
        elif (self.start == ''):
            self.start = (datetime.datetime.now() + datetime.timedelta(days=-1 )).strftime('%Y%m%d')
        pd_list = []
        pivot_data2 = []



    def shcode_load(self):
        shcode_list = []

        try:
            conn = pymssql.connect(host=self.db_adr, user=self.id, password=self.pw, database=self.db_name, charset="utf8", as_dict=True)
            cur = conn.cursor()
            if (self.ticker == '' and self.ticker_name == '' ):
                sql01 = """select top 2 %s
                             From %s 
                            order by 1;""" % (self.code_col, self.code_tab)
            else:
                sql01 = """select distinct %s 
                             From %s 
                            where %s = '%s'
                               or %s = '%s' 
                            order by 1;""" % (self.code_col, self.code_tab, self.code_col, self.ticker, self.code_name_col, self.ticker_name)
            cur.execute(sql01)

            for row in cur:
                shcode_list = shcode_list + list(row.values())

            conn.close()
            return shcode_list

        except:
            conn.close()
            print('ticker loading error')


    def pandas_load(self, code_list):
        shcode_list = code_list 
        pivot_data2 = []
        
        try:
            for sh_cnt in range(len(shcode_list)):
                data = pd.DataReader('KRX:'+shcode_list[sh_cnt], 'google' , self.start, self.end)
                date = list(data.index.strftime('%Y%m%d'))
                shcode = []
                for i in range(len(date)):
                    shcode.append(shcode_list[sh_cnt])
                open = list(data['Open'])
                high = list(data['High'])
                low = list(data['Low'])
                close = list(data['Close'])
                vol = list(data['Volume'])
                pd_list = [date, shcode, open, high, low, close, vol]
                pivot_data = []

                for a in range(len(date)):
                    for b in range(len(pd_list)):
                        pivot_data.append(pd_list[b][a])
                    pivot_data2.append(pivot_data)
                    pivot_data = []

            return pivot_data2

        except:
            print('pandas datareader error')



    def shcode_insert(self, pivot_list):

        pivot_data2 = pivot_list 
        insert_cnt = 0
    
        try:
            conn = pymssql.connect(host=self.db_adr, user=self.id, password=self.pw, database=self.db_name, charset='utf8', as_dict=True)
            cur = conn.cursor()
            for x in range(len(pivot_data2)):
                sql02 = "insert into %s select %d, %s, %d, %d, %d, %d, %d;" % (self.insert_tab, int(pivot_data2[x][0]), "'"+pivot_data2[x][1]+"'", pivot_data2[x][2], pivot_data2[x][3], pivot_data2[x][4], pivot_data2[x][5], pivot_data2[x][6])
                sql03 = """delete From %s 
                                 where yyyymmdd_surid = %d 
                                   and shcode = %s;""" % (self.insert_tab, int(pivot_data2[x][3]), "'"+ pivot_data2[x][1] + "'")
                ret_del = cur.execute(sql03)
                ret_ins = cur.execute(sql02)
                insert_cnt = insert_cnt + 1

            conn.commit()
            conn.close()
            return print("insert table: %d row(s)" % insert_cnt)

        except:
            conn.close()
            print("insert error")



a0 = pandas_data('jesus4000.iptime.org:14333','jesus','Gkr7743s!','Project_GG','','','shcode','hname','t8430','','',10,'pandas')
a1 = a0.shcode_load()
print(a1)
a2 = a0.pandas_load(a1)
print(a2)
a0.shcode_insert(a2)