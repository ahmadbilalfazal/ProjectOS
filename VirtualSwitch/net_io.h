#ifndef NET_IO_H
#define NET_IO_H

#include <stddef.h>

int net_recv_all(int fd, void *buffer, size_t length);
int net_send_all(int fd, const void *buffer, size_t length);

#endif