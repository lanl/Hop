MEMCROOT=/users/lionkov/memcached
MEMCINC=$(MEMCROOT)/include
MEMCLIB=$(MEMCROOT)/lib
CFLAGS=-Wall -g -I$(MEMCINC)
LFLAGS=-L$(MEMCLIB) -lmemcached -lpthread


FS=\
	npfs\
	ramfs\
	nullfs\
	ramfs2\
	echofs\

ifeq ($(SYSNAME),Linux)
	RDMADEPS:=\
		$(and $(foreach dir,$(LIBDIRS),$(wildcard $(dir)/libibverbs.*)), \
		$(foreach dir,$(LIBDIRS), $(wildcard $(dir)/librdmacm.*)))
	ifneq ($(strip $(RDMADEPS)),)
		FS += rdmanpfs
       endif
endif

.PHONY: all clean

memc: memc.o
	$(CC) -o memc $(CFLAGS) memc.o $(LFLAGS)

clean:
	rm -f *.o memc *~

%.c: Makefile

%.o: %.c 
	$(CC) $(CFLAGS) -c $*.c

