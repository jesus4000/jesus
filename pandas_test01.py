import pymssql
import pandas_datareader.data as pd
import datetime

today = datetime.datetime.now().strftime('%Y%m%d')
yesterday = (datetime.datetime.now() + datetime.timedelta(days=-8)).strftime('%Y%m%d')



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


# print(shcode_load('localhost','jesus','3477','test'))






#################################

# def pandas_load(start,end):

shcode_load('localhost','jesus','3477','test')

start = ''
end = ''

if(start == ''):
    start = yesterday
if(end == ''):
    end = today


## for 문 시작
pd_data = ()

for i in range(len(shcode_list)):
    pd_data = pd.DataReader("KRX:" + shcode_list[i], "google", start, end)
    date  = list(pd_data.index.strftime('%Y%m%d'))
    open  = list(pd_data['Open'])
    high  = list(pd_data['High'])
    low   = list(pd_data['Low'])
    close = list(pd_data['Close'])
    vol   = list(pd_data['Volume'])

    pd_list = [date,open, high, low, close, vol]




    # for j in range(len(date)):
    #     date[j]




print(shcode_list[0])
print(pd_list)

test = []
test2 = []

for a in range(len(date)):
    for b in range(6):    #date ~ vol 까지 컬럼
        test.append(pd_list[b][a])
        test2 = test2,test


# print(pd_list[0][0])
print(test)
print(test2)



    # return x



# print(pd_data.index)
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


