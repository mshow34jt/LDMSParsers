CC=gcc
INCDIRS = -I/usr/include/mysql 
#INCDIRS = -I/usr/include/mysql 
#LDFLAGS = -g -L/usr/lib64/mysql -L/usr/local/lib
LDFLAGS = -g -L/usr/lib64/mysql -L/usr/local/lib

all: parse_torque parse_nodecount update_account

parse_torque:	parse_torque.o
	$(CC) parse_torque.c -o parse_torque $(INCDIRS) $(LDFLAGS) -lmysqlclient

parse_nodecount:   parse_nodecount.o
	$(CC) parse_nodecount.c -o parse_nodecount $(INCDIRS) $(LDFLAGS) -lmysqlclient

update_account:   update_account.o
	$(CC) update_account.c -o update_account $(INCDIRS) $(LDFLAGS) -lmysqlclient

parse_torque.o:	parse_torque.c
	$(CC) -c parse_torque.c $(INCDIRS)

parse_nodecount.o: parse_nodecount.c
	$(CC) -c parse_nodecount.c $(INCDIRS)

update_account.o: update_account.c
	$(CC) -c update_account.c $(INCDIRS)

install:
	cp parse_torque /opt/isc/bin

clean:
	rm -f *.o
	rm parse_torque
	rm parse_nodecount
	rm update_account
