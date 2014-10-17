#!/usr/local/bin/python3
# encoding: utf-8

import os, time, re
import csv as CSV
import json as JSON
import itertools
import gzip
import time
from functools import wraps
import functools
import collections as CL
import time
import cProfile
import random as RND
import calendar
import dateutil
from dateutil.parser import *
from redis import StrictRedis as Redis



DB_PARAMS = {
	'events_db_id': 1, 
	'dupes_db_id': 2,
	'rethink_db': 'high_value_users', 
	'rethink_table': 'user_activity'
}


#----------------------- filter & transform functions --------------------------#

def is_duplicate( line):
	'''
	returns: nothing
	pass in: line as python dict
	'''
	val = line.get('eventId')
	line['eventId'] = None
	line = JSON.dumps(line)
	key = hash(line)
	# if the hash of the current line is in the deque, then add it to 
	# the 'dupes' list and do not return it
	if key in dq:
		dupes.append(val)
		return 1
	else:
		dq.appendleft(key)
		return 0


def iso8601_to_unixtime(t):
    try:
        x1 = parse(t).astimezone(dateutil.tz.tzutc())
        return calendar.timegm(x1.utctimetuple())
    except ValueError:
        print("logged!")


#----------------------- co-routine wrapper --------------------------#

def coroutine(fn):
	'''
	fn decorator used to wrap all of the co-routine functions 
	in this module, given that coroutines must be 'primed' 
	by first calling 'next'; in effect, this decorator 
	advances execution to the first yield expression, ie, 
	'line=(yield)'
	'''
	def start(*args, **kwargs):
		g = fn(*args, **kwargs)
		next(g)
		return g
	return start


#---------------------------- source fn ------------------------------#

def opener(fh, target, num_rec):
	'''
	not a co-routine, only a source into the 
	first two-way worker co-routine
	'''
	c = 0
	while c < num_rec:
		line = fh.readline()
		if not line:
			time.sleep(.1)
			continue
		target.send(line)
		c += 1
	fh.close()

	
	
#----------------------- two-way worker coroutines ---------------------#
	
@coroutine
def grep1(target):
	try:
		while 1:
			line = (yield)
			if line.strip():
				line = JSON.loads(line)
				target.send(line)
	except GeneratorExit:
		target.close()		


@coroutine
def grep2(target):
	try:
		while 1:
			line = (yield)
			# line = JSON.loads(line)
			if is_duplicate(line):
				pass
			else:
				target.send(line)
	except GeneratorExit:
		target.close()



#--------------------- one-way worker coroutines ('sinks') -------------------#

@coroutine
def aggregate(db_params):
	cxn1 = Redis(db=db_params['events_db_id'] )
	cxn1.hset('H1', 'counter', 0)
	while 1:
		line = (yield)
		cxn1.hincrby('H1', 'counter', 1)
		
			
@coroutine
def persist(db_params):
	cxn1 = Redis(db=db_params['events_db_id'])
	while 1:
		line = (yield)
		k = "{}:{}".format(iso8601_to_unixtime(line.get('eventDate')), 
			line.get('eventId'))
		v = line.get('userId')
		pipe = cxn1.pipeline()
		pipe.set(k, v)
		pipe.execute()


@coroutine
def persist_rethink(db_params):
	'''
	'''
	ua = RTH.db(db_params['rethink_db']).table(db_params['rethink_table'])
	
	while 1:
		line = (yield)
		ua.insert({
			"userID": line.get('userId'),
			"eventID": line.get('eventId'),
			"appID": line.get('appId')
		}).run()
		
		

if __name__ == '__main__':
	
	# dfile = '~/Projects/unified-ETL-pipeline-/data/appEventSample.txt.gz'
	dfile = '~/Desktop/9_18_appevent_dump_full.txt.gz'
	NUM_REC = 100000
	dupes = []
	dq = CL.deque([], maxlen=1500)
	cxn1 = Redis(db=DB_PARAMS['events_db_id'])
	cxn1.flushall()
	fh = gzip.open(os.path.expanduser(dfile), 'rt', encoding='utf-8')
	# opener(fh, grep1(persist(DB_PARAMS)))
	# opener(fh, grep1(persist_rethink(DB_PARAMS)))
	# opener(fh, grep1(aggregate(DB_PARAMS)))
	
	opener( fh, grep1(grep2(persist(DB_PARAMS))), num_rec=2*NUM_REC )
	# opener(fh, grep1(persist(DB_PARAMS)))
	
	
	
	print("number of records persisted: {}".format(len(cxn1.keys('*'))))
		
	
	print('file handle closed')

	print("dupes: {}".format(len(dupes)))
	print("deque: {}".format(len(dq)))
	
	print("% duplicates: {}".format((100*len(dupes)/NUM_REC)))
	

