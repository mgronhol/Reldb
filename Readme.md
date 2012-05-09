# Reldb

Reldb is a relation database, meaning that it stores and manages relationships between entities.

## Installation

Linux:

	make linux

OSX:

	make osx

## Data model

Relations are stored as 4-tuples in Reldb: (source, target, relation_type, weight )

	Source: 64 bit unsigned integer
	
	Target: 64 bit unsigned integer

	Relation_type: 16 bit enumerated integer (Reldb handles mapping between labels such as strings and enumerated integers transparently )
	
	Weight: Double precision floating point number



## Example

Using Reldb directly:

	import Reldb
	import pprint
	
	# In-memory db for example purposes

	db = Reldb.Reldb()
	
	# Lets create a db
	db.create_db( "example" )
	db.select_db( "example" )
	
	# Some user ids
	users = {}
	users['Ernesto'] = 1
	users['Svante']  = 2
	users['Jaques']  = 3
	
	# reverse mappings too

	reverse_users = {}
	for (user, userid) in users.items():
		reverse_users[userid] = user

	# Insert some relations
	db.insert( users['Ernesto'], users['Svante'], "knows", 1.0 )
	db.insert( users['Ernesto'], users['Jaques'], "knows", 1.0 )
	db.insert( users['Svante'],  users['Jaques'], "knows", 1.0 )
	db.insert( users['Jaques'],  users['Svante'], "knows", 1.0 )
	
	# Who Ernesto knows?
	pprint.pprint( db.get( users['Ernesto'] ) )

	# Who knows Jaques?

	pprint.pprint( db.reverse_get( users['Jaques'] ) )
	
	# Remove database at the end 
	
	db.destroy_db( "example" )	

Using ReldbQuery:
	
	import Reldb
	import pprint

	# same as in previous example

	# Now, lets use Reldb query for querying relations

	query = Reldb.ReldbQuery( db )
	query.start( users['Ernesto'] )
	
	# Who Ernesto knows?

	pprint.pprint( query.forward( "knows" ).getResults() )

	# Who is known by both Jaques and Ernesto?
	
	query.start( users['Ernesto'] )
	
	ernesto_knows = query.forward( "knows" )
	
	query.start( users['Jaques'] )
	
	jaques_knows = query.forward( "knows" )
	
	both_know = ernesto_knows.intersection( jaques_knows )
	
	pprint.pprint( [ reverse_users[uid] for uid in both_know.getResults() ] )
	
	# And so on...
	
	 

## Persistence

Using non-persistent db:

	import Reldb
	
	db = Reldb.Reldb()

	# ...

Using in persistent mode (sqlite storage handler):

	import Reldb

	storage = Reldb.SqliteStorage( '/path/to/database.db' )

	db = Reldb.Reldb( storage )

	# ...

