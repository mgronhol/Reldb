CC = g++
FLAGS = -O3 -Wall -pedantic

all: linux

linux:
	$(CC) $(FLAGS) -shared -fPIC libreldb.cpp -lc -Wl,-soname,libreldb.so -o libreldb.so.1

osx:
	$(CC) $(FLAGS) -shared -fPIC libreldb.cpp -lc -Wl,-install_name,libreldb.so -o libreldb.so.1
