#!/usr/bin/python

from __future__ import print_function
from datetime import date, datetime, timedelta
import os
import mysql.connector
import csv, sys, pprint
import MySQLdb
import time
import sys
import logging
import logging.config
import json 

reload(sys)
sys.setdefaultencoding("utf-8")
def any(iterable):
    for element in iterable:
        if element:
            return element
    return False


def trim_last_character(instvalues):
    if len(instvalues) > 0:
       if instvalues[-1:] == ",":
          #instvalues =  instvalues.encode('utf-32')
          instvalues = instvalues[:-1]
    return instvalues
    
def execute_sql(query, cnx):

    cursor = cnx.cursor()
    cursor.execute(query)
    cnx.commit()
    return cursor.rowcount


def generate_sql(jsonObject, connection, limit = 0, skipUpdate = False, exceptionNeeded = True):

    sql=""
    instvalues = ""

    x = 0   
    
    #print(instvalues)             
    sqlHeader = "INSERT INTO " + data[2] + " ( "
    cursor = connection.cursor()
    headers = jsonObject[0]
    
    #headers for
    newHeaders = []  
    
    #Define header need to be removed
    removedHeader = []  
    addedHeader = []
    exception = {
        'LastModifiedById' : 'LastModifiedBy',
        'CreatedById' : 'CreatedBy',
        'OwnerId' : 'Owner'
    }
    
    
    
    for header in headers:

        qShowColumns = "show columns FROM " + data[2] + " where field = '" +  header + "' "

        if(exceptionNeeded == True):
            qShowColumns += "or field = '" +  header[:-2] + "'"

        cursor.execute(qShowColumns)
        row = cursor.fetchone()

        if row != None:
            if(header in exception and exceptionNeeded == True):
                sqlHeader += " " + exception[header] + ","
            else:
                 sqlHeader += " " + header + ","
            addedHeader.append(header)


    sqlHeader = trim_last_character(sqlHeader) + ") VALUES ("
    
    success = 0
    failed = 0
    duplicate = 0
    failedId = []
    updated = 0
    for row in jsonObject:
        x += 1
        if(x == limit):
            return {
                'failed' : failed, 
                'success' : success, 
                'duplicate' : duplicate,
                'failedId' : failedId,
                'updated' : updated,
                'tableName' : data[2]
            }   
            exit()
        if(x % 1000 == 0):
            print(x)
        instvalues += "("
        #check whether primary key exists or not
        
        qCheckId = "select id from " + data[2] + " where id = '" + row['Id'] + "' "
        cursor.execute(qCheckId)
        currentRow = cursor.fetchone() 
        removed = ""
        
        try:
            if currentRow == None:
                instvalues = ""
                for header in addedHeader:
                    if(row[header] == None ):
                        instvalues += "null,"
                    else:
                        instvalues += "'" + MySQLdb.escape_string(str(row[header])) + "',"
                instvalues = trim_last_character(instvalues) + ");"
                sql = sqlHeader + instvalues + "\n"
                if execute_sql(sql, cnx = connection) == 1 :
                    success += 1
                else:
                    failed += 1

            else:
                sql = "UPDATE  " + data[2] + " SET "
                for header in addedHeader:
                    if(header in exception and exceptionNeeded == True):
                        realHeader = exception[header]
                    else:
                        realHeader = header


                    if(row[header] == None):
                        sql += "`" + realHeader + "` = null," 
                    else:
                        sql += "`" + realHeader + "` = '" + MySQLdb.escape_string(str(row[header])) + "',"
                sql = trim_last_character(sql)
                sql += " WHERE ID = '" + row['Id'] + "'"
                if(execute_sql(sql, connection) == 1): updated += 1

                
        except Exception, e:
            print (e)

            #continue
            failedId.append(row['Id'])
            failed += 1
    #print (sql)            


    return {
        'failed' : failed, 
        'success' : success, 
        'duplicate' : duplicate,
        'failedId' : failedId,
        'updated' : updated,
        'tableName' : data[2]
    }   

def saveLog(logsObject, connection):
    sqlLog = "INSERT INTO CommImportLog(log_value, rows_imported, rows_failed, rows_updated, created_at)" \
            " VALUES ('SF API Import via Temp Json " + logsObject['tableName'] + "'," \
            + str(logsObject['success']) + "," + str(logsObject['failed']) + "," + str(logsObject['updated']) +",NOW())"
    execute_sql(sqlLog, connection)
    cursor = connection.cursor()
    for id in logsObject['failedId']:
        qCheckId = "select FailedId from CommImportFailed where FailedId = '" + id + "' "
        cursor.execute(qCheckId)
        currentRow = cursor.fetchone() 
        if(currentRow == None):
            sqlFailed = "INSERT INTO CommImportFailed (`FailedTableName`, `FailedId`, `Action`, `Fixed`, `CreatedDate`, `LastModifiedDate`) VALUES (" + \
                "'"+ MySQLdb.escape_string(logsObject['tableName']) + "','" + MySQLdb.escape_string(str(id)) + "',0,0,NOW(),NOW())"
            execute_sql(sqlFailed, connection)

#READ CSV FILE
data = sys.argv 
cnx = mysql.connector.connect(user='root', database='element-spa', port=3306, buffered = True, host="127.0.0.1", password="")
with open(data[1], 'rb') as jsonfile:
    jsonObject = json.load(jsonfile)
    skipUpdate = data[3]
    exceptionNeeded = data[4]
    result = generate_sql(jsonObject=jsonObject, connection = cnx, limit = 10000000, skipUpdate = skipUpdate, exceptionNeeded = exceptionNeeded)    
    #saveLog(result, connection = cnx)
    print(result)
    


