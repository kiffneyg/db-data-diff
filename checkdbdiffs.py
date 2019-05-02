helptext="""
checkdbdiffs.py 

compares tables in two database schemas for row. The schemas do not have to have the same name,
but they should be similar else this will throw a lot of errors.

It checks all tables within a schema 
for rowcounts, latest timestamps, and a hash sum of ALL timestamps.

If there is a more common field name for timestamps (like LAST_MODIFIED_DATE in LYNXX), edit the large
SQL named "dsql" and put that field name in this part:

                    ORDER BY
                       CASE
                          WHEN column_name LIKE 'ADDED_DATE' THEN 2
                          WHEN column_name LIKE 'LAST_MODIFIED_DATE' THEN 1
                          ELSE 3

giving the date field most commonly used as order 1, the second choice as 2.  Otherwise
the script will randomly choose a date field in the table.						  


"""

import cx_Oracle as cx
import Queue
import threading
import argparse
import time
import datetime
import pprint
parser = argparse.ArgumentParser(description=helptext)
parser.add_argument('--daysback', help= 'number of days back to end date range - LYNXX specific')
parser.add_argument('--threadcount', type=int, default = 4, help='number of threads to create')
parser.add_argument('--sourceschema', required = True, help = 'source schema to compare')
parser.add_argument('--targetschema', required = True, help = 'target schema to compare')
parser.add_argument('--sourcedb', required = True, help = 'source database')
parser.add_argument('--targetdb', required = True, help = 'target database')
parser.add_argument('--sourceuser', required = True, help = 'source db username')
parser.add_argument('--sourcepw', required = True, help = 'source db password')
parser.add_argument('--targetuser', required = True, help = 'target db username')
parser.add_argument('--targetpw', required = True, help = 'target db password')
args=parser.parse_args()
#print args.threadcount


#targetdb = 'PRLYNXX'
#targetdb = 'PR1GLIMS'
#sourcedb = 'PRLYNXX'
targetdb = args.targetdb
sourcedb = args.sourcedb

pbconnd = dict(user=args.sourceuser, password=args.sourcepw, dsn=sourcedb, threaded = True)
exconnd = dict(user=args.targetuser, password=args.targetpw, dsn=targetdb, threaded = True)
pbconn = cx.connect(**pbconnd)
pbconn.current_schema = args.sourceschema
countd = {}
#databases = ['QA1DB' , 'PRPBWEB']
conns = {targetdb : exconnd, sourcedb : pbconnd}

# 2/2014 reworked to start up two threads with own queue to run same query against all target dbs.

counts = {}

dsql = """select 'SELECT /*+ parallel ('|| dt.table_name ||') */ count(1) ct ' ||
case when DT.COLUMN_NAME is not null
then ', max(' || dt.column_name || ') maxdate,
sum(ora_hash(' || dt.column_name ||')) orahash '
else ', null orahash,
null maxdate ' end
|| ' from ' || dt.table_name s, owner, table_name, column_name from ( 
  SELECT dt.owner,
         dt.table_name,
         dtc.column_name,
         dtc.data_type
    FROM dba_tables dt
         LEFT JOIN
         (SELECT column_name,
                 data_type,
                 table_name,
                 owner,
                 ROW_NUMBER ()
                 OVER (
                    PARTITION BY dtc.table_name
                    ORDER BY
                       CASE
                          WHEN column_name LIKE 'ADDED_DATE' THEN 2
                          WHEN column_name LIKE 'LAST_MODIFIED_DATE' THEN 1
                          ELSE 3
                       END)
                    rn
            FROM dba_tab_columns dtc
           WHERE     (dtc.data_type = 'DATE' OR dtc.data_type LIKE 'TIMESTAMP%')
--                 AND (SELECT num_nulls
--                        FROM dba_tab_col_statistics dtcs
--                       WHERE     dtcs.owner = dtc.owner
--                             AND dtcs.table_name = dtc.table_name
--                             AND dtc.column_name = dtcs.column_name) = 0
                 AND dtc.owner IN
                        ('{schema}') and table_name not like 'BIN$%') dtc
            ON     dt.owner = dtc.owner
               AND dt.table_name = dtc.table_name
               AND dtc.rn = 1
   WHERE     dt.owner IN
                ('{schema}') 
                --and rownum < 10
ORDER BY dt.owner, dt.table_name) dt""".format(schema=args.sourceschema)


def queue_runner(q):
        # print "In queue_runner\n"
        curlist = []
        for i in conns.items():
            connect = cx.connect(**i[1])
            if i[0] == sourcedb:
                connect.current_schema = args.sourceschema
            else:
                connect.current_schema = args.targetschema
            curs = connect.cursor()
            curlist.append([i[0],curs])
        while q.qsize() > 0:
                # print "Thread is running\n"
                r = q.get()
                lock = threading.Lock()
                with lock:
                    print "queue.get(): ", r
                vsql, owner, table_name, column_name = r[0], r[1], r[2], r[3]
                if args.daysback is not None and vsql.find('LAST_MODIFIED_DATE') > 0:
                    vsql = vsql + " where LAST_MODIFIED_DATE <= trunc(sysdate) - " + args.daysback
                #print '\nvsql: ', vsql, '\n'
                for curs in curlist:
                    rc = 0
                    db = curs[0]
                    #print 'db: ', db, '\n'
                    try:
                        curs[1].execute(vsql)
                        rc, maxdate, orahash = curs[1].fetchone()  # first element of tuple is count
                        #print 'rowcount: ' , rc
                    except cx.DatabaseError, exc:
                        error, = exc.args
                        print error.code, error.message
                        rc = error.message
                    dkey = owner +'.' + table_name
                    lock = threading.Lock()
                    #lock.acquire()
                    smaxdate = maxdate
                    if maxdate is not None:
                            smaxdate = maxdate.strftime('%m/%d/%Y %H:%M:%S %f')
                    with lock:
                        if not countd.has_key(dkey):
                            countd[dkey] = {db : [rc, column_name, smaxdate, orahash] }
                        if not countd[dkey].has_key(db):
                            countd[dkey][db] = [rc, column_name, smaxdate, orahash]
                    #print 'task done'
                q.task_done()

def main():
        pbcurs = pbconn.cursor()  #get list of tables from source database (not the target)
        print dsql
        pbcurs.execute(dsql)
        myqueue = Queue.Queue()
        
        for s, owner, table_name, column_name in pbcurs:
                #vsql = 'select count(1) from {owner}.{table_name}'
                #vsql = vsql.format(owner = owner, table_name = table_name)
                # print vsql
                #tabname = owner +'.' + table_name
                myqueue.put((s, owner, table_name, column_name))
        for i in range(1, args.threadcount):
                worker = threading.Thread(target = queue_runner, args = (myqueue,))
                #worker.setDaemon(True)
                worker.start()
        first_run_time = time.time()            
        while myqueue.qsize() > 0:
            print ">> queuesize %s, threadcount %s" % (myqueue.qsize(), threading.activeCount())
            time.sleep(5)
            print "Minutes in run: %s " % (round((time.time() - first_run_time)/60, 2))
        #myqueue.join()
        # print "countd:\n"
        # pprint.pprint( countd )
        print "diffs:\n"
        diffs = {}
        same = {}
        for i in countd.iterkeys():
                try:
                        source = countd[i][sourcedb]
                except KeyError:
                        print "For ", i, sourcedb, " no key"
                try:
                        target = countd[i][targetdb]
                except KeyError:
                        print "For ", i, targetdb, " no key"
                try:
                        if (countd[i][sourcedb] != countd[i][targetdb]):
                                # print i, sourcedb, countd[i][sourcedb], targetdb, countd[i][targetdb]
                                diffs[i] = countd[i]
                        else:
                                same[i] = countd[i]
                except KeyError:
                        pass
        print "Total time: %s " % (round((time.time() - first_run_time)/60, 2))
        s = pprint.PrettyPrinter()
        sout = s.pformat(diffs)
        print sout
        #print s
        f = open('diff_' + datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S_%f') + '.txt','w')
        f.write("""
There are {0} tables that differ between {1} and {2}.
List of differences:
Format is
SCHEMA.TABLE: {{DATABASE:        [ROWCOUNT,
                                DATE_FIELD NAME,
                                LATEST_TIMESTAMP,
                                HASH OF ALL TIMESTAMPS IN LATEST_TIMESTAMP]}}

""".format(len(diffs), sourcedb, targetdb))
        f.write(sout)
        f.write("""
There are {0} tables that are the same between {1} and {2}.
List of tables that are the same:
""".format(len(same), sourcedb, targetdb))
        sout = s.pformat(same)
        f.write(sout)
        f.close()        
        #print "Total time: %s " % (round((time.time() - first_run_time)/60, 2))

if __name__ == "__main__":
  main()           
