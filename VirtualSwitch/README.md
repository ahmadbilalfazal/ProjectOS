# Virtual Switch (Tier 1 and Tier 2)

A multithreaded Ethernet-style virtual switch written in C for Linux. Each host connects to the switch over TCP and receives a switch port. The switch learns source MAC addresses and forwards known unicast frames to the learned port. Unknown unicast and broadcast traffic is flooded to eligible ports.

## Build

Requirements: GCC, GNU Make, pthreads, and POSIX shared memory support.

```bash
make clean
make
```

The build uses the required flags:

```text
-std=c11 -Wall -Wextra -Werror -g -O0
```

## Run

Start the switch with a TCP port and number of switch ports:

```bash
./switch 9000 4
```

Start hosts in separate terminals:

```bash
./host 127.0.0.1 9000 02:00:00:00:00:01
./host 127.0.0.1 9000 02:00:00:00:00:02
```

Enter a destination MAC followed by a message. Use `/quit` to disconnect a host.

## Administration

Run these commands from the project directory while the switch is running:

```bash
./swctl show mac
./swctl show ports
./swctl clear mac
./swctl shutdown 2
./swctl up 2
./swctl vlan 1 20
./swctl trunk 2
./swctl mirror 0 3
./swctl stats --watch
```

- `shutdown <port>` administratively disables a port, drains its queued frames, and disconnects its current host.
- `up <port>` enables the port for a future host connection.
- `vlan <port> <vlan>` configures an access VLAN (1–4094).
- `trunk <port>` configures a trunk port that accepts and forwards frames across VLANs.
- `mirror <source> <target>` sends a copy of source-port traffic to the target port.
- `stats --watch` reads the process-shared statistics segment directly and refreshes once per second.

The UNIX-domain administration socket is `/tmp/vswitch.sock`. Shared statistics use `/vswitch_stats`.

## Configuration reload

Create `vswitch.conf` in the working directory. Each line is one of:

```text
0 access 10
1 trunk
2 access 20
```

Send SIGHUP to reload it:

```bash
kill -HUP <switch-pid>
```

The signal thread handles SIGINT for clean shutdown and SIGUSR1 to write `mac_table_dump.txt`.

## Implementation notes

- The wire header is exactly 17 bytes. Multi-byte fields use network byte order.
- `net_recv_all` and `net_send_all` handle partial transfers, interrupted system calls, disconnects, and SIGPIPE-safe sending.
- Each switch port has three bounded output queues and a dedicated sender thread. The sender uses weighted round-robin service (5:3:1) for priorities 0, 1, and 2. Queue overflow drops the frame instead of blocking the receiving thread.
- A single reference-counted frame buffer is shared by all destination queues for a flood or mirror operation.
- A dedicated timer thread refills each per-port token bucket. Output is limited to approximately 1 MiB/s with a 64 KiB burst; senders wait on timed condition variables when tokens are unavailable.
- The MAC table uses striped bucket mutexes, ages entries after 60 seconds, caps the table at 256 entries with oldest-entry eviction, logs MAC moves, and enforces a limit of 16 learned MAC addresses per port. Exceeding the per-port limit disables that port.
- A counting semaphore controls available host ports. New connections wait up to five seconds and are rejected when the switch is full.
- Statistics are stored in POSIX shared memory protected by a process-shared mutex.
- Logging is asynchronous: worker threads enqueue messages for a dedicated logger thread, which writes timestamped entries to `switch.log`.

## Locking overview

- MAC updates and aging acquire `mac_manage_mutex` before one bucket mutex. Bucket lookups and dumps acquire only the relevant bucket mutex.
- Port configuration and socket state use the port mutex. Queue contents and administrative-up state use the queue mutex. Code releases one before acquiring the other; there is no nested port-mutex/queue-mutex acquisition.
- Queue condition waits always use the corresponding queue mutex. Token balances and refill timestamps use a separate per-port token mutex.
- Shared counters use the process-shared statistics mutex. The logger queue uses its own mutex and condition variable.

## Tests

```bash
./test
./test_net_io
```

`test` covers frame serialization/deserialization and concurrent MAC-table operations. `test_net_io` covers partial reads, incomplete frames, and reliable sending.

## Scope and verification status

This source includes the Tier 1/Tier 2 mechanisms described above, but it should still be validated on the target Linux machine under concurrent load and with ThreadSanitizer/Valgrind before submission. The included tests are unit tests; they do not prove every concurrency, VLAN, QoS, rate-limit, mirror, reconfiguration, or shutdown scenario. No Tier 3 bonus features are included.
