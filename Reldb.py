#!/usr/bin/env python

import ctypes
import struct
import collections
import sqlite3

def pack( source, target, rel ):
	return struct.pack( "<QQH", source, target, rel )

def unpack( key ):
	#print "unpack: len(key)=", len(key)
	return struct.unpack( "<QQH", key )


class DummyStorage( object ):
	def __init__( self ):
		pass
	
	def load( self, db ):
		pass
	
	def save( self, dbname, command, *args ):
		pass


class SqliteStorage( object ):
	def __init__( self, dbfn ):
		self.dbfn = dbfn
		self.sql = sqlite3.connect( dbfn )
		
		self.initdb()
	
	def initdb( self ):
		test = False
		cur = self.sql.cursor()
		try:
			cur.execute('SELECT * FROM reldb_metadata' )
			test = True
		except sqlite3.OperationalError:
			test = False
		if not test:
			cur.execute( 'CREATE TABLE reldb_metadata (id integer PRIMARY KEY, key text, value text )' )
			cur.execute( 'CREATE TABLE reldb_types (id integer PRIMARY KEY, type text, key integer )' )
			cur.execute( 'CREATE TABLE reldb_databases (id integer PRIMARY KEY, name text )' )
			cur.execute( 'CREATE TABLE reldb_relations (id integer PRIMARY KEY,database text, source integer, target integer, type integer, weight real )' )
			cur.execute( 'INSERT INTO reldb_metadata (key, value) VALUES (?, ?)', ("created", "true") )
			self.sql.commit()
	
	def load( self, db ):
		db.suppress = True
		cur = self.sql.cursor()
		cur.execute( 'SELECT * FROM reldb_databases' )
		rows = cur.fetchall()
		for row in rows:
			db.create_db( row[1] )
		
		cur.execute( 'SELECT * FROM reldb_types' )
		rows = cur.fetchall()
		for row in rows:
			db._set_rel_key( row[1], row[2] )
		
		cur.execute( 'SELECT * FROM reldb_relations' )
		rows = cur.fetchall()
		for row in rows:
			#print row
			(id, dbname, source, target, rel_type, weight ) = row
			
			db.select_db(dbname)
			db.insert( source, target, db.rev_types[rel_type], weight )
			
		db.suppress = False
	
	def save( self,	dbname, command, *args ):
		if command == Reldb.NEW_REL_TYPE:
			name = args[0]
			key = args[1]
			cur = self.sql.cursor()
			cur.execute( 'INSERT INTO reldb_types (type, key) VALUES( ?, ?)', (name, key ) )
			self.sql.commit()
		elif command == Reldb.CREATE_DB:
			name = args[0]
			cur = self.sql.cursor()
			cur.execute( 'INSERT INTO reldb_databases (name) VALUES (?)', (name,) )
			self.sql.commit()
		
		elif command == Reldb.DESTROY_DB:
			name = args[0]
			cur = self.sql.cursor()
			cur.execute( 'DELETE FROM reldb_databases WHERE name = ?', (name,) )
			cur.execute( 'DELETE FROM reldb_relations WHERE database = ?', (name, ) )
			self.sql.commit()
		
		elif command == Reldb.INSERT:
			(source, target, rel_type, weight) = args
			cur = self.sql.cursor()
			cur.execute( 'INSERT INTO reldb_relations (database, source, target, type, weight) VALUES (?, ?, ?, ?, ?)', (dbname, source, target, rel_type, weight) )
			self.sql.commit()
		
		elif command == Reldb.REMOVE:
			(source, target, rel_type) = args
			cur = self.sql.cursor()
			cur.execute( 'DELETE FROM reldb_relations WHERE database = ? AND source = ? AND target = ? AND type = ?', (dbname, source, target, rel_type) )
			self.sql.commit()
		
		
		
		
class KeyVector( ctypes.Structure ):
	_fields_ = [ ("N", ctypes.c_ulonglong), ("data", ctypes.POINTER(ctypes.POINTER(ctypes.c_char)) ) ]


class Reldb( object ):
	NEW_REL_TYPE = 1
	INSERT = 2
	REMOVE = 3
	CREATE_DB = 4
	DESTROY_DB = 5
	
	def __init__( self, storage = DummyStorage() ):
		
		self.storage = storage
		
		self.lib = ctypes.cdll.LoadLibrary( "./libreldb.so.1" )
		
		self.lib.create_db.retype = ctypes.c_void_p
		
		self.lib.destroy_db.argtypes = [ctypes.c_void_p]
		
		self.lib.reldb_insert.argtypes = [ctypes.c_void_p, ctypes.c_ulonglong, ctypes.c_ulonglong, ctypes.c_ushort, ctypes.c_double]
		
		self.lib.reldb_remove.argtypes = [ctypes.c_void_p, ctypes.c_ulonglong, ctypes.c_ulonglong, ctypes.c_ushort]
		
		self.lib.reldb_get.restype = ctypes.POINTER( KeyVector )
		self.lib.reldb_get.argtypes = [ctypes.c_void_p, ctypes.c_ulonglong]
		
		self.lib.reldb_reverse_get.restype = ctypes.POINTER( KeyVector )
		self.lib.reldb_reverse_get.argtypes = [ctypes.c_void_p, ctypes.c_ulonglong]
		
		self.lib.reldb_get_weight.restype = ctypes.c_double
		self.lib.reldb_get_weight.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
		
		self.lib.reldb_free_key_vector.argtypes = [ctypes.POINTER( KeyVector )]
		
		self.dbs = {}
		self.db = None
		self.dbname = None
		self.types = {}
		self.rev_types = {}
		
		self.types_counter = 0
		self.Relation = collections.namedtuple( "Relation", ['source', 'target', 'type', 'weight'] )
		self.suppress = False
		
		self.storage.load( self )
		
	def has_db( self, dbname ):
		return dbname in self.dbs
	
	def _set_rel_key( self, rel_type, rel_key ):
		
		self.types[rel_type] = rel_key
		self.rev_types[rel_key] = rel_type
		if rel_key > self.types_counter:
			self.types_counter = rel_key
		
		
	def _get_rel_key(self, rel_type ):
		if rel_type in self.types:
			return self.types[rel_type]
		else:
			self.types_counter += 1
			self.types[rel_type] = self.types_counter
			self.rev_types[self.types_counter] = rel_type
			
			if not self.suppress:
				self.storage.save( self.dbname, Reldb.NEW_REL_TYPE, rel_type, self.types_counter ) 
			
			return self.types_counter
		
	def create_db( self, dbname ):
		self.dbs[dbname] = self.lib.create_db()
		if not self.suppress:
			self.storage.save( None, Reldb.CREATE_DB, dbname )
	
	def destroy_db( self, dbname ):
		self.lib.destroy_db( self.dbs[dbname] )
		if not self.suppress:
			self.storage.save( None, Reldb.DESTROY_DB, dbname )
	
	def select_db( self, dbname ):
		self.db = self.dbs[dbname]
		self.dbname = dbname
		
	def insert( self, source, target, rel_type, weight ):
		rel_key = self._get_rel_key( rel_type )
		self.lib.reldb_insert( self.db, source, target, rel_key, weight )
		if not self.suppress:
			self.storage.save( self.dbname, Reldb.INSERT, source, target, rel_key, weight )
		 
	
	def remove( self, source, target, rel_type ):
		rel_key = self._get_rel_key( rel_type )
		self.lib.reldb_remove( self.db, source, target, rel_key, weight )
		if not self.suppress:
			self.storage.save( self.dbname, Reldb.REMOVE, source, target, rel_key )
		
	def get( self, source ):
		results = self.lib.reldb_get( self.db, source )
		data = results.contents.data[:results.contents.N]
		out = []
		for entry in data:
			(source, target, rel_key) = unpack( entry[:18] )
			weight = self.lib.reldb_get_weight( self.db, entry[:18] )
			out.append( self.Relation( source, target, self.rev_types[rel_key], weight ) )
		
		self.lib.reldb_free_key_vector( results )
		
		return out
			
	def reverse_get( self, target ):
		results = self.lib.reldb_reverse_get( self.db, target )
		data = results.contents.data[:results.contents.N]
		out = []
		for entry in data:
			(target, source, rel_key) = unpack( entry[:18] )
			weight = self.lib.reldb_get_weight( self.db, entry[:18] )
			out.append( self.Relation( source, target, self.rev_types[rel_key], weight ) )
		
		self.lib.reldb_free_key_vector( results )
		
		return out	

class ReldbQuery( object ):
		def __init__( self, db, cursor = [] ):
				self.db = db
				self.cursor = cursor
		
		def start( self, source ):
				self.cursor = [source]

		def forward( self, rel_types ):
				if not isinstance( rel_types, list ) and not isinstance( rel_types, tuple ):
						rel_types = [rel_types]
				results = set()
				for entry in self.cursor:
						conns = self.db.get( entry )
						for conn in conns:
								if conn.type in rel_types:
										results.add( conn.target )
				
				return ReldbQuery( self.db, list( results ) )

		def backward( self, rel_types ):
				if not isinstance( rel_types, list ) and not isinstance( rel_types, tuple ):
						rel_types = [rel_types]
				results = set()
				for entry in self.cursor:
						conns = self.db.reverse_get( entry )
						for conn in conns:
								if conn.type in rel_types:
										results.add( conn.source )
				
				return ReldbQuery( self.db, list( results ) )

		def union( self, other ):
				setA = set( self.cursor )
				setB = set( other.cursor )
				new_set = setA.union( setB )
				return ReldbQuery( self.db, list( new_set ) )
	
		def difference( self, other ):
				setA = set( self.cursor )
				setB = set( other.cursor )
				new_set = setA.difference( setB )
				return ReldbQuery( self.db, list( new_set ) )

		def intersection( self, other ):
				setA = set( self.cursor )
				setB = set( other.cursor )
				new_set = setA.intersection( setB )
				return ReldbQuery( self.db, list( new_set ) )

		def getRelated( self, rel_types, forward = True ):
				if not isinstance( rel_types, list ) and not isinstance( rel_types, tuple ):
						rel_types = [rel_types]

				visited = set()
				stack = self.cursor

				while len( stack ) > 0:
						entry = stack.pop()
						if entry in visited:
								continue
						
						visited.add( entry )
						if forward:
								conns = self.db.get( entry )
								for conn in conns:
										if conn.type in rel_types:
												stack.append( conn.target )
						else:
								conns = self.db.reverse_get( entry )
								for conn in conns:
										if conn.type in rel_types:
												stack.append( conn.source )
	
				return ReldbQuery( self.db, list( visited ) )
		
		def getResults( self ):
				return self.cursor 





#db = Reldb()
#
#db.create_db( "debug" )
#db.select_db( "debug" )
#
#db.insert( 0, 1, "t", 0 )
#db.insert( 0, 2, "t", 0 )
#db.insert( 0, 3, "f", 0 )
#db.insert( 1, 10, "t", 0 )
#db.insert( 1, 11, "f", 0 )
#import pprint
#
#pprint.pprint( db.get( 0 ) ) 

#query = ReldbQuery( db )
#query.start( 0 )
#query = query.getRelated( "t" )
#print query.getResults()






