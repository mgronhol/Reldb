CC = g++
FLAGS = -O3 -Wall -pedantic


#all: libgraph

all: python

python:
	$(CC) $(FLAGS) -shared -fPIC libreldb.cpp -lc -Wl,-soname,libreldb.so -o libreldb.so.1
