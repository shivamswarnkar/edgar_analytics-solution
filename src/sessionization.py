import csv
import sys
import argparse
import datetime

#Object contains required information for every active user session
class User:
    '''
    Purpose : Store every active user session's info
    Methods : None
    Attributes:
        ip : IP address to identify user; type(str) 
        prev_t : Time of latest access; type(datetime.datetime) 
        acc_t : Time of first access; type(datetime.datetime)
        num_acc : Number of times user accessed a link; type(int)
    '''
    def __init__(self, ip, prev_t, num_acc):
        self.ip = ip
        self.prev_t = prev_t
        self.acc_t = prev_t
        self.num_acc = num_acc


def gap(t1, t2):
    '''
    Purpose : Calculate time difference between two time points
    Parameters : 
        t1 : ending time point; type(datetime.datetime)
        t2 : starting time point; type(datetime.datetime)
    Output (return) : 
        type: int
        val : Time diff from t2 to t1 in seconds
    '''
    return int((t1-t2).total_seconds())

def expired(database, current_time, inact_period, output_fh, flush_all=False):
    '''
    Purpose : Find which user sessions have expired, 
              write expired session info to output file
              and remove expired session from database of active sessions   
    Parameters : 
        database : all active user sessions; type(dictionary)
        current_time : time point to determine expired sessions; type(datetime.datetime)
        inact_period : inactivity period (in seconds) to determine expired sessions; type(int) 
        output_fh : file handler to write output info; type(file object)
        flush_all : Expire all user sessions or not; type(bool) 
    Output:
         return : None
         file : expired session info written to output_fh
    '''
    exp_ip = []     #keeps ip addresses of all expired sessions

    for ip in database:
        if(flush_all or gap(current_time, database[ip].prev_t)>=inact_period):
            
            output_fh.write(ip+","+datetime.datetime.strftime(database[ip].acc_t,'%Y-%m-%d %H:%M:%S' )\
                +","+datetime.datetime.strftime(database[ip].prev_t,'%Y-%m-%d %H:%M:%S' )\
                +","+str(gap(database[ip].prev_t,database[ip].acc_t,)+1)+\
                ","+str(database[ip].num_acc)+'\n')
            
            exp_ip.append(ip)

    #delete all expired sessions from database
    for key in exp_ip:
        del database[key]


def sessionization(log_file, output_file,inact_period):
    '''
    Purpose : Reads log_file for sessions, adds every session to database
              Calls expired function when time changes 
    Parameters : 
        log_file : name of input file; type(str)
        output_file :name of output file; type(str)
        inact_period : inactivity period (in seconds); type(int) 
         
    Output:
         return : None
         file : expired session info written to output_fh
    '''
  
    log_fh = open(log_file, 'r')
    
    csv_log_fh = csv.reader(log_fh)
    output_fh = open(output_file, 'w')

    user_list = []

    database = {}
    prev_time = None
    next(csv_log_fh, None)
    for query in csv_log_fh:

        ip = query[0].strip()
        date = query[1].strip()
        time = query[2].strip()
        
        #convert str date and time value to datetime.datetime object 
        date_time = datetime.datetime.strptime(date+" "+time,'%Y-%m-%d %H:%M:%S' )
        data = database.get(ip)
        
        #if data is none then it's a new user
        if(data==None):
            database[ip] = User(ip=ip, prev_t=date_time, num_acc = 1) 

        #else, update current user session
        else:
            database[ip].prev_t = date_time
            database[ip].num_acc += 1

        #set prev_time val when running first time     
        if(not prev_time):
            prev_time = date_time

        #if all queries are read for current time then run expired 
        if(date_time != prev_time):
            expired(database, prev_time, inact_period, output_fh)
            prev_time = date_time

    #flush all database since session has ended
    expired(database, date_time, inact_period, output_fh, flush_all=True)
    log_fh.close()
    output_fh.close()
    
def main():

    parser = argparse.ArgumentParser(description='sessionization script')
    parser.add_argument('--log_file', type=str, default='../input/log.csv',
        help='path of the log file')
    parser.add_argument('--inactivity_period_file', type=str, default='../input/inactivity_period.txt',
        help='path of the file containing inactivity period')

    parser.add_argument('--output_file', type=str, default='../output/sessionization.txt', help='path for the output file')

    args = parser.parse_args()
   
    fh = open(args.inactivity_period_file, 'r')
    inactivity_period = int(fh.readline().strip())
    fh.close()
    
    sessionization(args.log_file, args.output_file,inactivity_period)



if __name__ == '__main__':
    main()
