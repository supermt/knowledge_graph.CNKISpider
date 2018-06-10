#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import sched
import spider_worker
import threading
from threading import Timer
import datetime
import spider_cleaner
from configparser import ConfigParser
import spider_queue

LIMIT = 2
thread_count = 0
total_count = 0
MAX_COUNT = 10
lock = threading.Lock()


def loop_thread(keywordval):
  global thread_count
  start = str(int(time.time()*1000))+keywordval+".db"
  worker = threading.Thread(target=spider_worker.worker_thread,args=(start,keywordval))
  worker.setDaemon(True)
  worker.start()

  lock.acquire()
  try:
    thread_count-=1
  finally:
    lock.release()

def loop_starter(keyword,searchlocation):
  search_origin =  str(searchlocation)+':'+str(keyword)
  origin_thread = threading.Thread(target=loop_thread,args=(search_origin,))
  origin_thread.setDaemon(True)
  origin_thread.start()
  origin_thread.join()

  global thread_count
  global total_count
  while (total_count < MAX_COUNT):
    current_name = spider_queue.queue_operator.get_record()
    if current_name != None and thread_count <= LIMIT:
      current_name = current_name.decode('utf-8')
      new_thread = threading.Thread(target=loop_thread,args=(str(searchlocation)+":"+str(current_name),))
      new_thread.setDaemon(True)
      new_thread.start()
      new_thread.join()
      total_count+=1
      lock.acquire()
      try:
        thread_count+=1
      finally:
        lock.release()
    else:
      pass

if __name__ == '__main__':
  cf = ConfigParser()
  cf.read("Config.conf", encoding='utf-8')
  keyword = cf.get('base', 'keyword')# 关键词
  searchlocation = cf.get('base', 'searchlocation') #搜索位置
  spider_queue.queue_operator.reboot(keyword)
  loop_starter(keyword,searchlocation)