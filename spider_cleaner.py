#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import sched
import spider_worker
from threading import Timer
import datetime
import sqlite3
import os
import json
import requests

TABLENAME="PAPER"
GAPTIME = 3

def send_records(payload):
  url = "http://localhost:8081/import/list/paper"
  headers = {
      'Content-Type': "application/json",
      'Cache-Control': "no-cache"
  }
  response = requests.request("POST", url, data=payload.encode('utf-8'), headers=headers)
  # print(response.text)

def read_records(db_name , offset):
  record_count = 0
  connect = sqlite3.connect(db_name)
  c = connect.cursor()
  try:
    records = c.execute("SELECT url, title, authors, abstract  FROM " + TABLENAME + " LIMIT 100 OFFSET " + str(offset))
    results=[]
    for record in records:
      result={}
      result["url"]=record[0]
      result["title"]=record[1]
      result["authors"]=eval(record[2])
      result["abstractContent"]=record[3]
      results.append(result)
      record_count+=1
    json_str = json.dumps(obj=results,ensure_ascii=False)
    send_records(json_str)
  except sqlite3.OperationalError:
    pass
  return record_count

def remove_db(db_name):
  if os.path.exists(db_name):
    os.remove(db_name)
    print("cleaned up")

def main(db_name,offset):
  read_record = read_records(db_name,offset)
  if read_record != 0:  
    Timer(GAPTIME, main,(db_name,offset + read_record)).start()
  else:
    # all records read
    remove_db(db_name)
    # pass

if __name__ == '__main__':
  main("余景寰.db",0)