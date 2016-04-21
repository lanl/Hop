SYSNAME:=${shell uname}
SYSNAME!=uname
HFILES=hop.h rmt.h
CFLAGS=-Wall -g -I . -D$(SYSNAME)
LFLAGS=-g -L. -lhop -lcrypto -lpthread

OFILES=\
	clnt.o\
	conv.o\
	fmt.o\

all: libhop.a

libhop.a: $(OFILES)
	ar rc libhop.a $(OFILES)
	ranlib libhop.a

$(CMDS): libhop.a

clean:
	rm -f *.o *.a *~ core.* $(CMDS)

$(OFILES): Makefile $(HFILES)

%.o: %.c
	$(CC) $(CFLAGS) -c $*.c
