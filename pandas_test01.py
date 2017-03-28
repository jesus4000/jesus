import pymssql
import pandas_datareader.data as web

def shcode_load(db_adr,id,pw,db_name):
    global shcode_list
    shcode_list = []

    if(db_adr == ''):
        db_adr = 'localhost'

    try:
        conn = pymssql.connect(host=db_adr, user=id, password=pw, database=db_name,charset='utf8',as_dict=True)
        cur = conn.cursor()
        sql01 = 'select top 2 shcode from t8430 order by 1'
        cur.execute(sql01)

        for row in cur:
            shcode_list = shcode_list + list(row.values())

        conn.close()
        return shcode_list

    except:
        conn.close()
        print('DB접속정보 확인요망')


print(shcode_load('localhost','jesus','3477','test'))


# def pandas_load():
START = "2016-03-07"
END =  "2016-03-10"
shcode_list = '000020'
kospi = web.DataReader("KRX:" + shcode_list, "google", START, END)

date  = list(kospi.index.strftime('%Y%m%d'))
open  = list(kospi['Open'])
high  = list(kospi['High'])
low   = list(kospi['Low'])
close = list(kospi['Close'])
vol   = list(kospi['Volume'])



# print(kospi.index)
#
# target_list = []
#
# target_list.append(date[0])
# target_list.append(high[0])
#
# print(date[0])
# + open[0] + high[0] + low[0] + close[0] + vol[0])
#
# print(target_list)
#
# print(data.keys())
# print(data.Open.values())


