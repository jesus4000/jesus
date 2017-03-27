import pymssql
import pandas_datareader.data as web




# def shcode_load(db_adr,id,pw,db_name):
#     global shcode_list
#     shcode_list = []
#
#     if(db_adr == ''):
#         db_adr = 'localhost'
#
#     try:
#         conn = pymssql.connect(host=db_adr, user=id, password=pw, database=db_name,charset='utf8',as_dict=True)
#         cur = conn.cursor()
#         sql01 = 'select top 2 shcode from t8430 order by 1'
#         cur.execute(sql01)
#
#         for row in cur:
#             shcode_list = shcode_list + list(row.values())
#
#         conn.close()
#         return shcode_list
#
#     except:
#         conn.close()
#         print('DB접속정보 확인요망')
#
#
# print(shcode_load())


# def pandas_load():
START = "2016-03-07"
END =  "2016-03-07"
shcode = '000020'
data = web.DataReader("KRX:" + shcode, "google", START, END)



# print(data.keys())
# print(data.Open.values())
#
