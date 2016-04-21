CSROOT=$(HOME)/cassandra
CSINC=$(CSROOT)/include
CSLIB=$(CSROOT)/lib64
UVROOT=$(HOME)/libuv
UVINC=$(UVROOT)/include
UVLIB=$(UVROOT)/lib

CFLAGS=-Wall -g -I$(UVINC) -I$(CSINC)
LFLAGS=-L$(CSLIB) -lcassandra -L$(UVLIB) -luv -lpthread

.PHONY: all clean

csbench: csbench.o
	$(CC) -o csbench $(CFLAGS) csbench.o $(LFLAGS)

clean:
	rm -f *.o csbench *~

%.cc: Makefile

%.o: %.cc 
	$(CXX) $(CFLAGS) -c $*.cc
