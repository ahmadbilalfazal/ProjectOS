CC = gcc

CFLAGS = -std=c11 -Wall -Wextra -Werror -g -O0
CPPFLAGS = -D_POSIX_C_SOURCE=200809L
LDFLAGS = -pthread -lrt

all: switch swctl test test_net_io host

switch: switch.c shm_stats.c shm_stats.h frame.c frame.h net_io.c net_io.h
	$(CC) $(CPPFLAGS) $(CFLAGS) switch.c shm_stats.c frame.c net_io.c -o switch $(LDFLAGS)

swctl: swctl.c shm_stats.c shm_stats.h
	$(CC) $(CPPFLAGS) $(CFLAGS) swctl.c shm_stats.c -o swctl $(LDFLAGS)

test: test.c frame.c frame.h mac_table.c mac_table.h
	$(CC) $(CPPFLAGS) $(CFLAGS) test.c frame.c mac_table.c -o test $(LDFLAGS)

host: host.c frame.c frame.h net_io.c net_io.h
	$(CC) $(CPPFLAGS) $(CFLAGS) host.c frame.c net_io.c -o host $(LDFLAGS)

test_net_io: test_net_io.c net_io.c net_io.h
	$(CC) $(CPPFLAGS) $(CFLAGS) test_net_io.c net_io.c -o test_net_io $(LDFLAGS)

clean:
	rm -f switch swctl test test_net_io host mac_table_dump.txt switch.log

.PHONY: all clean