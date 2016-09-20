SYSNAME:=${shell uname}
SYSNAME!=uname
HFILES=include/hop.h include/rmt.h
CFLAGS=-Wall -g -I . -D$(SYSNAME) -Iinclude/
LFLAGS=-g -L. -lhop -lcrypto -lpthread

OFILES=\
	clnt.o\
	conv.o\
	fmt.o\

all: libhop.a

libhop.a: $(OFILES)
	ar rc lib/libhop.a $(OFILES)
	ranlib lib/libhop.a

$(CMDS): libhop.a

clean:
	rm -f *.o *.a *~ core.* $(CMDS) lib/* include/*~

$(OFILES): Makefile $(HFILES)

%.o: %.c
	$(CC) $(CFLAGS) -c $*.c
