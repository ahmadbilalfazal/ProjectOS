Virtual Switch - Operating Systems Fall 2026

This project is written in C for Linux. It uses TCP sockets for hosts, a UNIX domain socket for the admin program, pthreads, mutexes, a semaphore, signals and POSIX shared memory.

Build

Open a terminal in the VirtualSwitch folder and run:

make clean
make

The Makefile uses the required compiler options:

-std=c11 -Wall -Wextra -Werror -g -O0

Programs

Start the switch:

./switch <tcp_port> <num_ports>

Example:

./switch 9000 4

Start hosts in separate terminals:

./host 127.0.0.1 9000 02:00:00:00:00:01
./host 127.0.0.1 9000 02:00:00:00:00:02

Enter messages using this format:

destination-MAC message

Example:

02:00:00:00:00:02 Hello

Enter /quit to close a host.

Admin commands

./swctl show ports
./swctl show mac
./swctl clear mac
./swctl stats --watch

Press Ctrl+C in the switch terminal for shutdown. SIGUSR1 asks the switch to write mac_table_dump.txt. SIGHUP is currently recognized but does not reload a configuration file.

Tests

Run the frame and MAC table tests:

./test

Run the network I/O tests:

./test_net_io

Current implementation notes

The current implementation supports the fixed 17-byte frame format, network byte order, partial TCP reads and writes, host connections, basic MAC learning, known-destination forwarding, unknown-destination flooding, striped MAC-table locks, a free-port semaphore with a timeout, shared-memory port counters, the UNIX socket admin interface, SIGINT shutdown, and SIGUSR1 MAC-table dumping.

The current forwarding path sends directly from the host receive thread. It does not yet implement the required per-port bounded output queues or sender threads. The following assignment features are also not implemented yet: reference-counted broadcast frames, MAC aging and LRU eviction, port security, three-level QoS queues and weighted scheduling, token-bucket rate limiting, VLAN isolation and trunk configuration, port mirroring, live port up/down and VLAN reconfiguration, SIGHUP configuration reload, and asynchronous queued logging.

Tier 3 not done