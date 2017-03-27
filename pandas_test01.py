import pymssql
import pandas_datareader.data as web




def shcode_load(num):
    global shcode_list
    shcode_list = []

    if(num==''):
        num = 3000

    conn = pymssql.connect(host='jesus4000.ipdisk.co.kr:14333', user='jesus', password='Gkr7743s!', database='Project_GG',charset='utf8',as_dict=True)
    cur = conn.cursor()
    sql01 = 'select top %d shcode from t8430 order by 1' % num
    cur.execute(sql01)

    for row in cur:
        shcode_list = shcode_list + list(row.values())

    conn.close()
    return shcode_list


print(shcode_load(2))
