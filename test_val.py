pd_list = [['20170320', '20170321', '20170322'], [13750.0, 13850.0, 16161.0]]

test = []
test2 = []

a=0
for b in range(2):
    print('b' + str(b))
    test.append(pd_list[b][a])
    print(test)

test2 = test2 + test

a=1
for b in range(2):
    print('b' + str(b))
    test.append(pd_list[b][a])
    print(test)

test2 = test2 + test


print(test2)


# for a in range(3):
#     print('a'+str(a))
#
#     for b in range(2):
#         print('b'+str(b))
#         test.append(pd_list[b][a])
#         print(test)
#
#     test2[a][b] = test
#     print(test2)


# print(test2)
# print(test2[1][1])
# print(test2[1][1])