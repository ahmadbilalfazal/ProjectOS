#include "net_io.h"

#include <sys/socket.h>
#include <errno.h>

int net_recv_all(int fd, void *buffer, size_t length)
{
    size_t received = 0;

    while (received < length) {
        ssize_t n = recv(fd, (char *)buffer + received,
                         length - received, 0);

        if (n > 0) {
            received += (size_t)n;
        }
        else if (n == 0) {
            if (received == 0) {
                return 0;
            }

            errno = ECONNRESET;
            return -1;
        }
        else {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }
    }

    return 1;
}

int net_send_all(int fd, const void *buffer, size_t length)
{
    size_t sent = 0;

    while (sent < length) {
        ssize_t n = send(fd, (const char *)buffer + sent,
                         length - sent, MSG_NOSIGNAL);

        if (n > 0) {
            sent += (size_t)n;
        }
        else if (n == 0) {
            errno = EPIPE;
            return -1;
        }
        else {
            if (errno == EINTR) {
                continue;
            }

            return -1;
        }
    }

    return 0;
}