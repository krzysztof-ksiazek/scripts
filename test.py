#!/usr/bin/python
import MySQLdb
import threading
import sys
import pdb
from random import randint


class threader(threading.Thread):
    def __init__(self,db,dosetvar):
        threading.Thread.__init__(self)
        self.db = db
    self.dosetvar = dosetvar
    def run(self):
        run_insert(self.db, self.dosetvar)


def run_insert(db,dosetvar):
    var = randint(1,100000)
    print "thread executing variable:%s"%(var)
    try:
        cursor = db.cursor()
        cursor.execute("SELECT @@hostname")
    for row in cursor.fetchall() :
        print row[0]
    if ( dosetvar == 1 ):
       cursor.execute("SET @var:="+str(var))
       print "yeyeyeyeye"
    cursor.execute("SELECT @var")
    for row in cursor.fetchall() :
        print "Variable: %s, result: %s"%(var,row[0])
    cursor.execute("SELECT SLEEP(30)")
#        cursor.close()
#        db.commit()
    except MySQLdb.Error, e:
        print "insert failed"
    print "Error %d: %s"%(e.args[0], e.args[1])
    return




def init_thread():
    backgrounds = []
    i = 0
    for db in connections:
    if ( i == 0 ):
        dosetvar = 1
    else:
        dosetvar = 0
    i = i + 1
        print "connection: %s, dosetvar= %i"%(db, dosetvar)
        background = threader(db, dosetvar)
        background.start()
        backgrounds.append(background)

    for background in backgrounds:
        background.join()

def main():
#    try:
    init_thread()
#    except threading.Error, e:
 #       print "failed to initiate threads"
 #       print "Error %d: %s"%(e.args[0], e.args[1])
    sys.exit(0)

if __name__ == "__main__":
    mysql_host = "10.0.0.200"
    mysql_user = "myuser"
    mysql_pass = "pass"
    mysql_port=int(4008)
    mysql_db = "test"
    threads = 10



    connections = []
    for thread in range(threads):
        try:
            connections.append(MySQLdb.connect(host=mysql_host, user=mysql_user, passwd=mysql_pass, db=mysql_db, port=mysql_port))
            #pdb.set_trace()

        except MySQLdb.Error, e:
            print "Error %d: %s"%(e.args[0], e.args[1])
            pdb.set_trace()
            sys.exit (1)

    main()
