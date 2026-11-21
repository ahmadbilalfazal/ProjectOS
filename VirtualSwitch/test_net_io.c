#define _POSIX_C_SOURCE 200809L
#include "net_io.h"

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>

int main(void)
{
    int sockets[2];
    char received[20] = {0};
    const char *message = "Virtual Switch Test";

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        perror("socketpair");
        return 1;
    }

    pid_t pid = fork();

    if (pid < 0) {
        perror("fork");
        return 1;
    }

    if (pid == 0) {
        close(sockets[0]);

        struct timespec delay = {0, 100000000};

        write(sockets[1], message, 5);
        nanosleep(&delay, NULL);

        write(sockets[1], message + 5, 7);
        nanosleep(&delay, NULL);

        write(sockets[1], message + 12, strlen(message) - 12);

        close(sockets[1]);
        return 0;
    }

    close(sockets[1]);

    int result = net_recv_all(sockets[0], received, strlen(message));

    if (result == 1 && strcmp(received, message) == 0) {
        printf("PASS: Partial reads handled correctly\n");
    }
    else {
        printf("FAIL: Partial read test\n");
        return 1;
    }

    wait(NULL);
    close(sockets[0]);

    if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        perror("socketpair");
        return 1;
    }

    pid = fork();

    if (pid < 0) {
        perror("fork");
        return 1;
    }

    if (pid == 0) {
        close(sockets[0]);
        write(sockets[1], "HELLO", 5);
        close(sockets[1]);
        return 0;
    }

    close(sockets[1]);

    memset(received, 0, sizeof(received));

    result = net_recv_all(sockets[0], received, 10);

    if (result == -1) {
        printf("PASS: Incomplete frame detected\n");
    }
    else {
        printf("FAIL: Incomplete frame test\n");
        return 1;
    }

    wait(NULL);
    close(sockets[0]);

        if (socketpair(AF_UNIX, SOCK_STREAM, 0, sockets) < 0) {
        perror("socketpair");
        return 1;
    }

    const char *outgoing = "Reliable sending test";
    char incoming[30] = {0};

    if (net_send_all(sockets[0], outgoing, strlen(outgoing)) == 0 &&
        net_recv_all(sockets[1], incoming, strlen(outgoing)) == 1 &&
        strcmp(outgoing, incoming) == 0) {
        printf("PASS: Reliable sending works\n");
    }
    else {
        printf("FAIL: Reliable sending test\n");
        return 1;
    }

    close(sockets[0]);
    close(sockets[1]);
    return 0;
}