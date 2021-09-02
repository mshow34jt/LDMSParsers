CC=gcc
#INCDIRS = -I/ascldap/users/mtshowe/mysql/include 
INCDIRS = -I/usr/include/mysql
LDFLAGS = -g -L/usr/lib64/mysql 
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
