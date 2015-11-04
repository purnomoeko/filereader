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


def generate_sql(csvfile, spamreader, connection, limit = 0, skipUpdate = False):

    sql=""
    instvalues = ""

    x = 0   
    
    #print(instvalues)             
    sqlHeader = "INSERT INTO " + data[2] + " ("
    cursor = connection.cursor()
    
    #reset position file
    csvfile.seek(0) 
    headers = spamreader.next()
    
    #headers for
    newHeaders = []  
    
    #Define header need to be removed
    removedHeader = []  
    addedHeader = []

    
    for header in headers:
        qShowColumns = "show columns FROM " + data[2] + " where field = '" +  header + "' or field = '" +  header[:-2] + "'"
        cursor.execute(qShowColumns)
        row = cursor.fetchone()

        if row != None:
            sqlHeader += " " + row[0] + ","
            addedHeader.append({
                'name' : row[0], 
                'index': headers.index(header)
            })
        else:
            removedHeader.append({
                'name' : header, 
                'index': headers.index(header)
            })

    success = 0
    failed = 0
    duplicate = 0
    failedId = []
    updated = 0
    for row in spamreader:
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
        
        qCheckId = "select id from " + data[2] + " where id = '" + row[0] + "' "
        cursor.execute(qCheckId)
        currentRow = cursor.fetchone() 
        removed = ""
        
        try:
            if currentRow == None:
                instvalues = ""
                for header in addedHeader:
                    if(row[header['index']] == ''):
                        instvalues +=  "null,"
                    else:
                        valueText = row[header['index']]
                        instvalues +=  "'"+ MySQLdb.escape_string(valueText) 
                        instvalues += "'" if len(addedHeader) == addedHeader.index(header) + 1 else "',"
                instvalues = trim_last_character(instvalues) + ");"
                sql = trim_last_character(sqlHeader) + ") VALUES (" + instvalues + "\n"
                if execute_sql(sql, cnx = connection) == 1 :
                    success += 1
                    #print (success)
                else:
                    failed += 1
                    print (row[0])
                    #print (sql)
            else:
                if(skipUpdate == True): continue
                duplicate += 1
                updateSQL = "Update " + data[2] + " SET "
                instvalues = ''
                for header in addedHeader:
                    if(row[header['index']] == ''):
                        instvalues +=  header['name'] + " = null"
                        instvalues += " " if len(addedHeader) == addedHeader.index(header) + 1 else ","
                    else:
                        instvalues +=  header['name'] + " = '"+ MySQLdb.escape_string(row[header['index']])
                        instvalues += "'" if len(addedHeader) == addedHeader.index(header) + 1 else "',"
                updateSQL = updateSQL + instvalues + " WHERE Id = '" + row[0] + "'"
                
                if(execute_sql(updateSQL, cnx = connection) == 1): updated += 1
                #print (str(row[0]) + " Exists");
        except Exception, e:
            print (e)
            #continue
            failedId.append(row[0])
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
            " VALUES ('CSV Import " + logsObject['tableName'] + "'," \
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
with open(data[1], 'rb') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    skipUpdate = data[3]
    result = generate_sql(csvfile,spamreader, connection = cnx, limit = 10000000, skipUpdate = skipUpdate)    
    saveLog(result, connection = cnx)
    print(result)
    


