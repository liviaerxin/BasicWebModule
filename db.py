# -*- coding: utf-8 -*-
'''
@author: siyao

Mysql database's interface, it can be called by multiple thread independently.
It can execute single SQL manipulation in one connection with auto connect/close. 
It can execute multiple SQL manipulation in one connection with one connect/close in the same thread.
'''
import threading, logging
import mysql.connector
import functools

class Dict(dict):
	def __init__(self, names=(), value=()):
		for k, v in zip(names, value):
			self[k] = v



engine = None
local_connection = threading.local()



class _db_engine(object):
	def __init__(self, user, password, database,host):
		self.params = dict()
		self.params['user'] = user
		self.params['password'] = password
		self.params['database'] = database
		self.params['host'] = host
	def connection(self):
		return mysql.connector.connect(**self.params)


def create_db(user,password,database,host="127.0.0.1"):
	global engine
	engine = _db_engine(user, password, database,host)

class _connection_ctx():
	def __init__(self):
		self.connect = engine.connection()
		print "get a new connect <id:%s>" % hex(id(self.connect))
	def connect(self):
		return self.connect
	def cursor(self):
		return self.connect.cursor()
	def commit(self):
		return self.connect.commit()
	def close(self):
		return self.connect.close()
	def rollback(self):
		return self.connect.rollback()

	def connect_exist(self):
		if self.connect.is_connected():
			return True
		else:
			return False

class _connection_auto(object):
	def __enter__(self):
		self.should_close_connection = False
		if not hasattr(local_connection,'ctx') or not local_connection.ctx.connect_exist():
			logging.info("connect object is not existed")
			logging.info("init a _connection_ctx")
			ctx = _connection_ctx()
			logging.info("save the _connection_ctx into local_connection.ctx")
			local_connection.ctx = ctx
			self.should_close_connection = True
		else:
			logging.info("connect <id:%s> is connected:%s" % (hex(id(local_connection.ctx.connect)), local_connection.ctx.connect_exist()))
	def __exit__(self, type, value, traceback ):
		if self.should_close_connection:
			logging.info("close the connect <id:%s>" % hex(id(local_connection.ctx.connect)))
			local_connection.ctx.close()
			logging.info("connect is connected: %s" % (local_connection.ctx.connect_exist()))

def with_connection_auto(func):
	@functools.wraps(func)
	def _wrap(*argv, **args):
		with _connection_auto():
			return func(*argv, **args)
	return _wrap

@with_connection_auto
def select(sql, *argv):
	##select("select * from pet where name=%s", ["Slim",])
	logging.info("current threading <name:%s>" % threading.current_thread().name)
	cursor = local_connection.ctx.cursor()
	logging.info("current connect object's cursor id:%s" % hex(id(cursor)))
	logging.info(sql)
	try:
		cursor.execute(sql,argv)
		names = [x[0] for x in cursor.description]
		values = cursor.fetchall()
		return [Dict(names, value) for value in values]
		#return values
		#print values
	finally:
		if cursor:
			logging.info("close the cursor id:%s" % hex(id(cursor)))
			cursor.close()
@with_connection_auto   #test select 2 times with only one connection open
def select2times():     #in the same connection object
	select("select * from pet where name='Slim'")	
	select("select * from pet where name='Buffy'")

def selectAll(sql, *argv):   #select all
	results = select(sql, *argv)
	if results:
		return results
	else:
		return None
def selectOne(sql, *argv):
	results = select(sql, *argv)
	if results:
		return results[0]
	else:
		return None


@with_connection_auto	
def update(sql, *argv):
	#update("insert into person (id, name) values (%s, %s)", 1006, 'yang')
	logging.info("current threading <name:%s>" % threading.current_thread().name)
	cursor = local_connection.ctx.cursor()
	logging.info("current connect object's cursor id:%s" % hex(id(cursor)))
	logging.info(sql)
	try:
		cursor.execute(sql,argv)
		print cursor.rowcount
		local_connection.ctx.commit()
		return	cursor.rowcount
	finally:
		if cursor:
			logging.info("close the cursor id:%s" % hex(id(cursor)))
			cursor.close()

def insert(table, **kw):
    '''
    u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    insert('user', **u1)
    '''
    cols, argv = zip(*kw.iteritems())
    sql = 'insert into `%s` (%s) values (%s)' % (table, ','.join(['`%s`' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    return update(sql, *argv)

if __name__ == "__main__":
	logging.basicConfig(level=logging.DEBUG)
	print "init database"
	create_db("root", "","test", "127.0.0.1")
	#select2times()
	#select("select * from pet where name='Slim'")
	#select("select * from pet where name='Buffy'")
	#sql = "select * from user"
	#update("insert into person (id, name) values (%s, %s)", 1006, 'yang')
	#update("drop table if exists temp1")
	print selectOne("select * from person where name=%s and id=%s", "yang", 1005)
	#test multiple thread runnig independently
	'''
	t1 = threading.Thread(target = select, args=(sql,))
	t2 = threading.Thread(target = select, args=(sql,))
	t3 = threading.Thread(target = select, args=(sql,))
	t1.start()
	t2.start()
	t3.start()
	t1.join()
	t2.join()
	t3.join()
	'''

