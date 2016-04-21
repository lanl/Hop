RAMCLOUD=$(HOME)/work/RAMCloud
RCSRC=$(RAMCLOUD)/src
RCLIB=$(RAMCLOUD)/obj.master
GTESTDIR=$(RAMCLOUD)/gtest
CFLAGS=-Wall -g -I$(RCSRC) -I$(RCLIB) -I$(GTESTDIR)/include
CXXFLAGS=$(CFLAGS) -std=c++11
LFLAGS=-L$(RCLIB) -lramcloud -lboost_program_options -lboost_filesystem -lboost_system -lpthread


.PHONY: all clean

rcloud: rcloud.o $(RCLIB)/OptionParser.o $(RCLIB)/libramcloud.a
	g++ $(CXXFLAGS) -o rcloud rcloud.o $(RCLIB)/OptionParser.o $(LFLAGS)

clean:
	rm -f *.o rcloud *~

%.c: Makefile

%.o: %.c 
	$(CC) $(CFLAGS) -c $*.c

%.o: %.cc 
	$(CXX) $(CXXFLAGS) -c $*.cc

