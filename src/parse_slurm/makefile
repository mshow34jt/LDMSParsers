CC=gcc
#INCDIRS = -I/ascldap/users/mtshowe/mysql/include 
INCDIRS = -I/opt/rh/rh-mysql57/root/usr/include/mysql
LDFLAGS = -g -L/usr/lib64/mysql -L/ascldap/users/mtshowe/isc/src/parse_slurm
#LDFLAGS = -g -L/ascldap/users/mtshowe/mysql/lib

all: parse_slurm

parse_slurm:	parse_slurm.o
#	$(CC) -g parse_slurm.c -o parse_slurm $(INCDIRS) $(LDFLAGS) 
	$(CC) parse_slurm.c -o parse_slurm $(INCDIRS) $(LDFLAGS) -lmysqlclient


parse_slurm.o:	parse_slurm.c
	$(CC) -c parse_slurm.c $(INCDIRS)


install:
	cp parse_slurm  ../parse_slurm

clean:
	rm -f *.o
	rm parse_slurm
