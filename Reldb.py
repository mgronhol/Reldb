#!/usr/bin/env python

import ctypes
import struct
import collections

def pack( source, target, rel ):
	return struct.pack( "<QQH", source, target, rel )

def unpack( key ):
	#print "unpack: len(key)=", len(key)
	return struct.unpack( "<QQH", key )




class KeyVector( ctypes.Structure ):
	_fields_ = [ ("N", ctypes.c_ulonglong), ("data", ctypes.POINTER(ctypes.POINTER(ctypes.c_char)) ) ]


class Reldb( object ):
	def __init__( self ):
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
		self.types = {}
		self.rev_types = {}
		
		self.types_counter = 0
		self.Relation = collections.namedtuple( "Relation", ['source', 'target', 'type', 'weight'] )
	
	def _get_rel_key(self, rel_type ):
		if rel_type in self.types:
			return self.types[rel_type]
		else:
			self.types_counter += 1
			self.types[rel_type] = self.types_counter
			self.rev_types[self.types_counter] = rel_type
			return self.types_counter
		
	def create_db( self, dbname ):
		self.dbs[dbname] = self.lib.create_db()
	
	def destroy_db( self, dbname ):
		self.lib.destroy_db( self.dbs[dbname] )
	
	def select_db( self, dbname ):
		self.db = self.dbs[dbname]
	
	def insert( self, source, target, rel_type, weight ):
		rel_key = self._get_rel_key( rel_type )
		self.lib.reldb_insert( self.db, source, target, rel_key, weight )
	
	def remove( self, source, target, rel_type ):
		rel_key = self._get_rel_key( rel_type )
		self.lib.reldb_remove( self.db, source, target, rel_key, weight )
	
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


db = Reldb()

db.create_db( "debug" )
db.select_db( "debug" )

#db.insert( 0, 1, "access", 0.2 )
#db.insert( 2, 1, "access", 0.3 )
#db.insert( 3, 1, "access", 0.4 )

#print db.get( 0 )

#print db.reverse_get( 0 )
#print db.reverse_get( 1 )

import time, random

N = 300000

t0 = time.time()

for i in range(N):
	db.insert( random.randint( 0, 10000 ), random.randint( 0, 10000 ), "access", 0.123 )

t1 = time.time()

print "It took %.3fs to insert %i entries, (%.2f kinserts/sec)"%( t1-t0, N, N/(t1-t0)/1000 )

t0 = time.time()
cnt = 0
for i in range( N ):
	tmp = db.get( random.randint( 0, 10000 ) )
	cnt += len( tmp )
t1 = time.time()

print "It took %.3fs to do  %i gets, (%.2f kgets/sec), fetched %i results -> %.3f kr/s"%( t1-t0, N, N/(t1-t0)/1000, cnt, cnt/(t1-t0)/1000 )


db.destroy_db( "debug" )

#ptr = db.lib.create_db()
#print ptr
#
#db.lib.reldb_insert( ptr, 0, 1, 1, 0.2 )
#db.lib.reldb_insert( ptr, 2, 1, 1, 0.3 )
#db.lib.reldb_insert( ptr, 3, 1, 1, 0.4 )
#
#results = db.lib.reldb_reverse_get( ptr, 1 )
#print results.contents.N
#print unpack(results.contents.data[0][0:19])
#entries = results.contents.data[:results.contents.N]
#print "len(entries)=",len(entries)
#print entries, [unpack(x.contents[0:19]) for x in entries]
#print [unpack(x[0:18]) for x in entries]

#db.lib.destroy_db( ptr )



