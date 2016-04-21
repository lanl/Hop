DSSDROOT=/opt/dssd
DSSDINC=$(DSSDROOT)/include
DSSDLIB=$(DSSDROOT)/lib64
CFLAGS=-Wall -g -I$(DSSDINC)
LFLAGS=-L$(DSSDLIB) -lflood -lpthread


.PHONY: all clean

dssd: dssd.o
	$(CC) -o dssd $(CFLAGS) dssd.o $(LFLAGS)

clean:
	rm -f *.o dssd *~

%.c: Makefile

%.o: %.c 
	$(CC) $(CFLAGS) -c $*.c

