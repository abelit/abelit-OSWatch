# from database import oracle

# tablesrc = 'A_QYZT'
# tabledst = 'A_QYZT'
# ownersrc = 'GZGS_GY'
# ownerdst = 'GZGS_HZ'
# condition = ""
# tablesrc_pk = oracle.Table().primarykey(tablesrc, ownersrc)
# tablesrc_field = oracle.Table().field(tablesrc, ownersrc)
# tabledst_pk = oracle.Table().primarykey(tabledst, ownerdst)
# tabledst_field = oracle.Table().field(tabledst, ownerdst)
# print(tabledst_pk[0])
# sql_diff = '''select {0} from (select * from {1} {2} minus select * from {3} {4})'''
# sql_diff_result = oracle.Oracle().select(sql_diff.format(tablesrc_pk[0][0],ownersrc + '.' + tablesrc, condition, ownerdst + '.' + tabledst, condition))
# #sql_diff_result = sql_diff_result[0]

#  # Generate variable string
# sql_diff_str = "("
# for i in range(len(sql_diff_result) - 1):
#     sql_diff_str = sql_diff_str + "'" + sql_diff_result[i][0] + "'" + ","
# sql_diff_str = sql_diff_str + sql_diff_result[len(sql_diff_result) - 1][0] + ")"

# condition = condition + ' ' + 'WHERE' + ' ' + tablesrc_pk[0][0] + ' ' + 'IN' + ' ' + sql_diff_str
                    
# # print(condition)
# bb = []
# aa = ['abelit','chenying','panpan','raochan']
# aa.remove('panpan') 
# ee = [bb.append('src.'+i) for i in aa]
# cc = bb
# dd = tuple(bb)
# condition = ""
# #condition = condition + ' WHERE ' + 'nbxh' + ' IN ' + str(bb)
# print(condition)
# strings = "(" + ",".join(bb) + ")"
# ff = "I am a boy"+ \
# "\n, and I am 27 years old"
# print(strings)
# print(ff)
# 

# def hello():
#     ee = 1
#     ff = "I am a boy"+ \
#         "\n, and I am 27 years old"
#     if ee is not None:
#         print(ff+str(ee))
#     return 1

# if __name__ == '__main__':
#     hello()


aa = ['abelit','chenying','panpan','raochan']
aa = []
bb = 'where nbxh in '
print(bb+str(tuple(aa)))
aa = ','.join(aa)

print(aa)