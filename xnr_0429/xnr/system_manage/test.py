#_*_coding:utf-8_*_
import sqlite3


#show all users account
#连接数据库,获取账户列表
def get_user_account_list():
    cx = sqlite3.connect("/home/xnr1/xnr_0429/xnr/flask-admin.db") 
    #cx = sqlite3.connect("/home/ubuntu8/yuanhuiru/xnr/xnr1/xnr/flask-admin.db")
    cu=cx.cursor()
    #cu.execute("select email from user") 
    cu.execute("select * from user where email='test'") 
    user_info = cu.fetchall()
    cx.close()
    return user_info


def get_table(db_file):
    try:
        conn = sqlite3.connect(db_file)
        cur = conn.cursor()
        cur.execute("select name from sqlite_master where type='table' order by name")
        #print '所有表：',cur.fetchall()
    except sqlite3.Error, e:
            print e
    cur.execute("PRAGMA table_info(user)")
    print '表结构',cur.fetchall()


if __name__ == "__main__":
    #get_table("/home/xnr1/xnr_0429/xnr/flask-admin.db")
    user_table = get_user_account_list()
    print user_table
