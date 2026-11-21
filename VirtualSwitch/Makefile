CC = gcc

CFLAGS = -std=c11 -Wall -Wextra -Werror -g -O0
CPPFLAGS = -D_POSIX_C_SOURCE=200809L
LDFLAGS = -pthread -lrt

all: switch swctl test

switch: switch.c shm_stats.c shm_stats.h
	$(CC) $(CPPFLAGS) $(CFLAGS) switch.c shm_stats.c -o switch $(LDFLAGS)

swctl: swctl.c shm_stats.c shm_stats.h
	$(CC) $(CPPFLAGS) $(CFLAGS) swctl.c shm_stats.c -o swctl $(LDFLAGS)

test: test.c frame.c frame.h mac_table.c mac_table.h
	$(CC) $(CPPFLAGS) $(CFLAGS) test.c frame.c mac_table.c -o test $(LDFLAGS)

clean:
	rm -f switch swctl test mac_table_dump.txt

.PHONY: all clean