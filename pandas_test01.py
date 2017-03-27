import pymssql
import pandas_datareader.data as web




def shcode_load(db_adr,id,pw,db_name):
    global shcode_list
    shcode_list = []

    if(db_adr == ''):
        db_adr = 'localhost'

    conn = pymssql.connect(host=db_adr, user=id, password=pw, database=db_name,charset='utf8',as_dict=True)
    cur = conn.cursor()
    sql01 = 'select top 2 shcode from t8430 order by 1'
    cur.execute(sql01)

    for row in cur:
        shcode_list = shcode_list + list(row.values())

    conn.close()
    return shcode_list


print(shcode_load())
