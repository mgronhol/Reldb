#include <map>
#include <sys/types.h>
#include <stdint.h>
#include <new>
#include <iterator>

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include <time.h>
#include <sys/time.h>

#define KEY_SIZE 18

double get_time(){
	struct timeval tv;
	gettimeofday( &tv, NULL );
	return tv.tv_sec + tv.tv_usec * 1e-6;
	}


std::string pack( uint64_t source, uint64_t target, uint16_t type ){
	std::string out;
	char buffer[16];
	
	memcpy( buffer, &source, sizeof( uint64_t ) );
	out += std::string( buffer, sizeof( uint64_t ) );
	
	memcpy( buffer, &target, sizeof( uint64_t ) );
	out += std::string( buffer, sizeof( uint64_t ) );
	
	memcpy( buffer, &type, sizeof( uint16_t ) );
	out += std::string( buffer, sizeof( uint16_t ) );
	
	
	return out;
	}

uint64_t unpack_source( std::string str ){
	uint64_t out;
	memcpy( &out, str.c_str(), sizeof( uint64_t ) );
	return out;
	}

uint64_t unpack_target( std::string str ){
	uint64_t out;
	memcpy( &out, str.c_str() + sizeof( uint64_t ), sizeof( uint64_t ) );
	return out;
	}
	
uint16_t unpack_type( std::string str ){
	uint16_t out;
	memcpy( &out, str.c_str() + 2*sizeof( uint64_t ), sizeof( uint16_t ) );
	return out;
	}

class Reldb{
	public:
		Reldb();
		void insert( uint64_t, uint64_t, uint16_t, double );
		void remove( uint64_t, uint64_t, uint16_t );
		std::vector<std::string> get( uint64_t );
		std::vector<std::string> reverse_get( uint64_t );
		double get_weight( std::string );
	private:
		std::map<std::string, double> entries;
		std::map<std::string, double> rev_entries;
	};
	
Reldb :: Reldb(){}

void Reldb :: insert( uint64_t source, uint64_t target, uint16_t type, double weight ){
	std::string key = pack( source, target, type );
	std::string rev_key = pack( target, source, type );
	
	entries[key] = weight;
	rev_entries[rev_key] = weight;
	
	}

void Reldb :: remove( uint64_t source, uint64_t target, uint16_t type ){
	std::map<std::string, double>::iterator it;
	std::string key = pack( source, target, type );
	std::string rev_key = pack( target, source, type );
	
	it = entries.find( key );
	if( it != entries.end() ){
		entries.erase( it );
		}
	
	it = rev_entries.find( rev_key );
	if( it != rev_entries.end() ){
		rev_entries.erase( it );
		}
	
	}

std::vector< std::string > Reldb :: get( uint64_t source ){
	std::vector< std::string > out;
	std::string key = pack( source, 0, 0 );
	std::map<std::string, double>::iterator it = entries.lower_bound( key );
	for( ; it != entries.end() && unpack_source( it->first ) == source ; it++ ){
		out.push_back( it->first );
		}
	return out;
	}
	
std::vector< std::string > Reldb :: reverse_get( uint64_t target ){
	std::vector< std::string > out;
	std::string key = pack( target, 0, 0 );
	std::map<std::string, double>::iterator it = rev_entries.lower_bound( key );
	for( ; it != rev_entries.end() && unpack_source( it->first ) == target ; it++ ){
		out.push_back( it->first );
		}
	return out;
	}

double Reldb :: get_weight( std::string key ){
	std::map<std::string, double>::iterator it = entries.find( key );
	
	if( it == entries.end() ){ return 0.0; }
	
	return it->second;
	}


extern "C" {
	
	typedef struct KeyVector {
		uint64_t N;
		
		char **data;
		
		} key_vector_t;
	
	void* create_db(){
		return (void*)( new Reldb() );
		}
	
	void destroy_db(void *ptr){
		delete (Reldb*)ptr;
		}
	
	void reldb_insert( void *ptr, uint64_t source, uint64_t target, uint16_t type, double weight ){
		Reldb *db = reinterpret_cast<Reldb*>(ptr);
		db->insert( source, target, type, weight );
		} 
	
	void reldb_remove( void *ptr, uint64_t source, uint64_t target, uint16_t type){
		Reldb *db = reinterpret_cast<Reldb*>(ptr);
		db->remove( source, target, type );
		} 
	
	key_vector_t* reldb_get( void *ptr, uint64_t source ){
		Reldb *db = reinterpret_cast<Reldb*>(ptr);
		std::vector<std::string> keys = db->get( source );
		key_vector_t *out;
		
		out = (key_vector_t *)malloc( sizeof( key_vector_t*) );
		
		out->N = keys.size();
		out->data = (char**)malloc( out->N*sizeof( char* ) );
		
		for( size_t i = 0 ; i < out->N ; ++i ){
			out->data[i] = (char*)malloc( KEY_SIZE * sizeof( char ) );
			memcpy( out->data[i], keys[i].c_str(), KEY_SIZE );
			}
		return out;
		}
	
	key_vector_t* reldb_reverse_get( void *ptr, uint64_t target ){
		Reldb *db = reinterpret_cast<Reldb*>(ptr);
		std::vector<std::string> keys = db->reverse_get( target );
		key_vector_t *out;
		
		out = (key_vector_t *)malloc( sizeof( key_vector_t*) );
		
		out->N = keys.size();
		
		out->data = (char**)malloc(  out->N * sizeof( char* ) );
		//printf( "data size: %lu\n", sizeof( uint64_t ) * 2 + sizeof( uint16_t ) );
		for( size_t i = 0 ; i < out->N ; ++i ){
			out->data[i] = (char*)malloc( KEY_SIZE * sizeof( char ) );
			memcpy( out->data[i], keys[i].c_str(), KEY_SIZE );
			}
		return out;
		}

	double reldb_get_weight( void *ptr, char *key ){
		Reldb *db = reinterpret_cast<Reldb*>(ptr);
		return db->get_weight( std::string( key, KEY_SIZE ) );
		}

	void reldb_free_key_vector( key_vector_t *vec ){
		for( size_t i = 0 ; i < vec->N ; ++i ){
			free( vec->data[i] );
			}
		free( vec->data );
		free( vec );
		}
	
	}

	
/*
int main(){
	
	std::string tmp = pack( 1, 2, 3 );
	
	std::cout << "source:" << unpack_source( tmp ) << std::endl;
	std::cout << "target:" << unpack_target( tmp ) << std::endl;
	std::cout << "type  :" << unpack_type( tmp ) << std::endl;
	
	Reldb db;
	size_t N = 3000000;
	double t0, t1;
	
	t0 = get_time();
	
	for( size_t i = 0 ; i < N ; ++i ){
		db.insert( i % 10000, i + 1, 0, 0 );
		}
	
	
	t1 = get_time();
	
	
	
	std::cout << "It took " << t1 - t0 << "secs to insert " << N << " entries." << std::endl;
	std::cout << "This means " << N/(t1-t0) / 1000.0 << " kinserts/sec." << std::endl;
	
	std::cout << std::endl;
	
	std::vector<std::string> keys = db.reverse_get( 110000 );
	
	for( size_t i = 0 ; i < keys.size() ; ++i ){
		std::cout << "target:" << unpack_target( keys[i] ) << " type: " << unpack_type( keys[i] ) << std::endl;
		}
	
	return 0;
	}
*/
