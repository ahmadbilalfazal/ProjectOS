#define _POSIX_C_SOURCE 200809L

#include "frame.h"
#include "net_io.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>

typedef struct {
    int fd;
} receiver_args_t;

int parse_mac(const char *text, uint8_t mac[6])
{
    unsigned int values[6];
    int used = 0;

    if (sscanf(text, "%2x:%2x:%2x:%2x:%2x:%2x%n",
               &values[0], &values[1], &values[2],
               &values[3], &values[4], &values[5], &used) != 6 ||
        text[used] != '\0') {
        return -1;
    }

    for (int i = 0; i < 6; i++) {
        if (values[i] > 255) {
            return -1;
        }

        mac[i] = (uint8_t)values[i];
    }

    return 0;
}

void print_mac(const uint8_t mac[6])
{
    printf("%02X:%02X:%02X:%02X:%02X:%02X",
           mac[0], mac[1], mac[2],
           mac[3], mac[4], mac[5]);
}

void *receive_frames(void *arg)
{
    receiver_args_t *args = arg;
    int fd = args->fd;

    while (1) {
        uint8_t header_buffer[FRAME_HEADER_SIZE];
        frame_header_t header;

        int result = net_recv_all(fd, header_buffer, FRAME_HEADER_SIZE);

        if (result <= 0) {
            break;
        }

        if (frame_deserialize(header_buffer, &header) != FRAME_HEADER_SIZE) {
            printf("Invalid frame received\n");
            break;
        }

        if (header.length > MAX_PAYLOAD_SIZE) {
            printf("Invalid payload length\n");
            break;
        }

        uint8_t payload[MAX_PAYLOAD_SIZE + 1];

        if (header.length > 0) {
            result = net_recv_all(fd, payload, header.length);

            if (result <= 0) {
                break;
            }
        }

        payload[header.length] = '\0';

        printf("\nReceived from ");
        print_mac(header.src);
        printf(": %s\n", payload);
        printf("> ");
        fflush(stdout);
    }

    printf("\nSwitch connection closed\n");
    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 4) {
        printf("Usage: ./host <switch_ip> <tcp_port> <mac>\n");
        return 1;
    }

    uint8_t own_mac[6];

    if (parse_mac(argv[3], own_mac) != 0) {
        printf("Invalid MAC address\n");
        return 1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);

    if (fd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in address;

    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_port = htons((uint16_t)atoi(argv[2]));

    if (inet_pton(AF_INET, argv[1], &address.sin_addr) != 1) {
        printf("Invalid switch IP address\n");
        close(fd);
        return 1;
    }

    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("connect");
        close(fd);
        return 1;
    }

    printf("Connected to switch as ");
    print_mac(own_mac);
    printf("\n");
    printf("Enter: <destination MAC> <message>\n");
    printf("Enter /quit to exit\n");

    receiver_args_t args;
    args.fd = fd;

    pthread_t receiver;

    if (pthread_create(&receiver, NULL, receive_frames, &args) != 0) {
        perror("pthread_create");
        close(fd);
        return 1;
    }

    char input[2048];

    while (1) {
        printf("> ");
        fflush(stdout);

        if (fgets(input, sizeof(input), stdin) == NULL) {
            break;
        }

        input[strcspn(input, "\n")] = '\0';

        if (strcmp(input, "/quit") == 0) {
            break;
        }

        char *message = strchr(input, ' ');

        if (message == NULL) {
            printf("Format: <destination MAC> <message>\n");
            continue;
        }

        *message = '\0';
        message++;

        uint8_t destination[6];

        if (parse_mac(input, destination) != 0) {
            printf("Invalid destination MAC\n");
            continue;
        }

        size_t length = strlen(message);

        if (length == 0 || length > MAX_PAYLOAD_SIZE) {
            printf("Message must contain 1 to 1500 bytes\n");
            continue;
        }

        frame_header_t header;

        header.length = (uint16_t)length;
        memcpy(header.src, own_mac, 6);
        memcpy(header.dst, destination, 6);
        header.vlan = 1;
        header.priority = 1;

        uint8_t packet[FRAME_HEADER_SIZE + MAX_PAYLOAD_SIZE];

        if (frame_serialize(&header, packet) != FRAME_HEADER_SIZE) {
            printf("Could not serialize frame\n");
            continue;
        }

        memcpy(packet + FRAME_HEADER_SIZE, message, length);

        if (net_send_all(fd, packet, FRAME_HEADER_SIZE + length) != 0) {
            perror("send");
            break;
        }
    }

    shutdown(fd, SHUT_RDWR);
    pthread_join(receiver, NULL);
    close(fd);

    return 0;
}