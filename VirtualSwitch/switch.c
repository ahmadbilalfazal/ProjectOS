#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <signal.h>
#include <stdatomic.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/mman.h>
#include <poll.h>
#include <time.h>
#include <errno.h>
#include <stdbool.h>
#include <sys/time.h>

#include "shm_stats.h"
#include "frame.h"
#include "net_io.h"

#include <arpa/inet.h>
#include <semaphore.h>

#define SOCKET_PATH "/tmp/vswitch.sock"
#define MAC_BUCKETS 16
#define MAC_TABLE_SIZE 256
#define BUFFER_SIZE 512

#define MAX_FRAME_SIZE (FRAME_HEADER_SIZE + MAX_PAYLOAD_SIZE)

typedef struct {
    int fd;
    int port;
    pthread_t thread;
    bool active;
    pthread_mutex_t mutex;
} switch_port_t;

static switch_port_t switch_ports[MAX_PORTS];
static sem_t free_ports;
static int tcp_server_fd = -1;
static int tcp_port_number;
static pthread_t tcp_listener_thread;

typedef struct mac_entry {
    unsigned char mac[6];
    int port;
    time_t updated;
    struct mac_entry *next;
} mac_entry_t;

typedef struct {
    pthread_mutex_t mutex;
    mac_entry_t *head;
} mac_bucket_t;

static mac_bucket_t mac_table[MAC_BUCKETS];

static shm_stats_t *shared_stats;
static int shm_fd = -1;
static int server_fd = -1;

static atomic_bool shutting_down = false;

static pthread_t admin_thread;
static pthread_t signal_thread;

static int running_ports;

static pthread_mutex_t handler_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t handler_cond = PTHREAD_COND_INITIALIZER;
static int active_handlers = 0;

unsigned int mac_hash(const unsigned char *mac)
{
    unsigned int hash = 0;

    for (int i = 0; i < 6; i++) {
        hash = hash * 31 + mac[i];
    }

    return hash % MAC_BUCKETS;
}

int parse_mac(const char *str, unsigned char *mac)
{
    unsigned int values[6];

    if (sscanf(str, "%2x:%2x:%2x:%2x:%2x:%2x",
               &values[0], &values[1], &values[2],
               &values[3], &values[4], &values[5]) != 6) {
        return -1;
    }

    for (int i = 0; i < 6; i++) {
        if (values[i] > 255) {
            return -1;
        }

        mac[i] = (unsigned char)values[i];
    }

    return 0;
}

void print_mac(FILE *file, const unsigned char *mac)
{
    fprintf(file, "%02X:%02X:%02X:%02X:%02X:%02X",
            mac[0], mac[1], mac[2],
            mac[3], mac[4], mac[5]);
}

void mac_table_init(void)
{
    for (int i = 0; i < MAC_BUCKETS; i++) {
        pthread_mutex_init(&mac_table[i].mutex, NULL);
        mac_table[i].head = NULL;
    }
}

void mac_table_clear(void)
{
    for (int i = 0; i < MAC_BUCKETS; i++) {
        pthread_mutex_lock(&mac_table[i].mutex);
    }

    for (int i = 0; i < MAC_BUCKETS; i++) {
        mac_entry_t *entry = mac_table[i].head;

        while (entry != NULL) {
            mac_entry_t *next = entry->next;
            free(entry);
            entry = next;
        }

        mac_table[i].head = NULL;
    }

    for (int i = MAC_BUCKETS - 1; i >= 0; i--) {
        pthread_mutex_unlock(&mac_table[i].mutex);
    }
}

void mac_table_destroy(void)
{
    mac_table_clear();

    for (int i = 0; i < MAC_BUCKETS; i++) {
        pthread_mutex_destroy(&mac_table[i].mutex);
    }
}

void mac_table_dump(FILE *file)
{
    fprintf(file, "Virtual Switch MAC Table\n");

    for (int i = 0; i < MAC_BUCKETS; i++) {
        pthread_mutex_lock(&mac_table[i].mutex);

        mac_entry_t *entry = mac_table[i].head;

        while (entry != NULL) {
            print_mac(file, entry->mac);
            fprintf(file, " port %d\n", entry->port);
            entry = entry->next;
        }

        pthread_mutex_unlock(&mac_table[i].mutex);
    }
}

void mac_table_show(int client_fd)
{
    char buffer[BUFFER_SIZE];

    snprintf(buffer, sizeof(buffer), "MAC Address       Port\n");

    send(client_fd, buffer, strlen(buffer), MSG_NOSIGNAL);

    for (int i = 0; i < MAC_BUCKETS; i++) {
        pthread_mutex_lock(&mac_table[i].mutex);

        mac_entry_t *entry = mac_table[i].head;

        while (entry != NULL) {
            snprintf(buffer, sizeof(buffer),
                     "%02X:%02X:%02X:%02X:%02X:%02X %d\n",
                     entry->mac[0], entry->mac[1],
                     entry->mac[2], entry->mac[3],
                     entry->mac[4], entry->mac[5],
                     entry->port);

            send(client_fd, buffer, strlen(buffer), MSG_NOSIGNAL);

            entry = entry->next;
        }

        pthread_mutex_unlock(&mac_table[i].mutex);
    }
}

int mac_table_learn(const unsigned char *mac, int port)
{
    unsigned int bucket = mac_hash(mac);

    pthread_mutex_lock(&mac_table[bucket].mutex);

    mac_entry_t *entry = mac_table[bucket].head;

    while (entry != NULL) {
        if (memcmp(entry->mac, mac, 6) == 0) {
            entry->port = port;
            entry->updated = time(NULL);

            pthread_mutex_unlock(&mac_table[bucket].mutex);
            return 0;
        }

        entry = entry->next;
    }

    entry = malloc(sizeof(mac_entry_t));

    if (entry == NULL) {
        pthread_mutex_unlock(&mac_table[bucket].mutex);
        return -1;
    }

    memcpy(entry->mac, mac, 6);

    entry->port = port;
    entry->updated = time(NULL);

    entry->next = mac_table[bucket].head;
    mac_table[bucket].head = entry;

    pthread_mutex_unlock(&mac_table[bucket].mutex);

    return 0;
}

int mac_table_lookup(const unsigned char *mac, int *port)
{
    unsigned int bucket = mac_hash(mac);

    pthread_mutex_lock(&mac_table[bucket].mutex);

    mac_entry_t *entry = mac_table[bucket].head;

    while (entry != NULL) {
        if (memcmp(entry->mac, mac, 6) == 0) {
            *port = entry->port;
            pthread_mutex_unlock(&mac_table[bucket].mutex);
            return 1;
        }

        entry = entry->next;
    }

    pthread_mutex_unlock(&mac_table[bucket].mutex);
    return 0;
}

void forward_frame(int source_port, const unsigned char *frame, size_t length,
                   const unsigned char *destination)
{
    int target_port = -1;

    if (mac_table_lookup(destination, &target_port) == 1) {
        if (target_port == source_port || target_port < 0 ||
            target_port >= running_ports) {
            return;
        }

        switch_port_t *port = &switch_ports[target_port];

        pthread_mutex_lock(&port->mutex);

        if (port->active && port->fd >= 0) {
            if (net_send_all(port->fd, frame, length) == 0) {
                pthread_mutex_lock(&shared_stats->mutex);
                shared_stats->ports[target_port].frames_out++;
                pthread_mutex_unlock(&shared_stats->mutex);
            }
            else {
                pthread_mutex_lock(&shared_stats->mutex);
                shared_stats->ports[target_port].frames_dropped++;
                pthread_mutex_unlock(&shared_stats->mutex);
            }
        }

        pthread_mutex_unlock(&port->mutex);
        return;
    }

    for (int i = 0; i < running_ports; i++) {
        if (i == source_port) {
            continue;
        }

        switch_port_t *port = &switch_ports[i];

        pthread_mutex_lock(&port->mutex);

        if (port->active && port->fd >= 0) {
            if (net_send_all(port->fd, frame, length) == 0) {
                pthread_mutex_lock(&shared_stats->mutex);
                shared_stats->ports[i].frames_out++;
                pthread_mutex_unlock(&shared_stats->mutex);
            }
            else {
                pthread_mutex_lock(&shared_stats->mutex);
                shared_stats->ports[i].frames_dropped++;
                pthread_mutex_unlock(&shared_stats->mutex);
            }
        }

        pthread_mutex_unlock(&port->mutex);
    }
}

void *host_handler(void *arg)
{
    switch_port_t *port = arg;
    int fd = port->fd;

    while (!atomic_load(&shutting_down)) {
        unsigned char frame[MAX_FRAME_SIZE];
        frame_header_t header;

        int result = net_recv_all(fd, frame, FRAME_HEADER_SIZE);

        if (result <= 0) {
            break;
        }

        if (frame_deserialize(frame, &header) != FRAME_HEADER_SIZE ||
            header.length > MAX_PAYLOAD_SIZE ||
            header.vlan < 1 || header.vlan > 4094 ||
            header.priority > 2) {
            break;
        }

        if (header.length > 0) {
            result = net_recv_all(fd, frame + FRAME_HEADER_SIZE,
                                  header.length);

            if (result <= 0) {
                break;
            }
        }

        size_t frame_length = FRAME_HEADER_SIZE + header.length;

        mac_table_learn(header.src, port->port);

        pthread_mutex_lock(&shared_stats->mutex);
        shared_stats->ports[port->port].frames_in++;
        pthread_mutex_unlock(&shared_stats->mutex);

        forward_frame(port->port, frame, frame_length, header.dst);
    }

    pthread_mutex_lock(&port->mutex);

    if (port->fd == fd) {
        port->active = false;
        port->fd = -1;
    }

    pthread_mutex_unlock(&port->mutex);

    close(fd);
    sem_post(&free_ports);

    printf("Host disconnected from port %d\n", port->port);

    pthread_mutex_lock(&handler_mutex);
    active_handlers--;
    pthread_cond_broadcast(&handler_cond);
    pthread_mutex_unlock(&handler_mutex);

    return NULL;
}

void *tcp_listener(void *arg)
{
    (void)arg;

    struct pollfd pfd;

    pfd.fd = tcp_server_fd;
    pfd.events = POLLIN;

    while (!atomic_load(&shutting_down)) {
        int result = poll(&pfd, 1, 500);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            perror("TCP poll");
            break;
        }

        if (result == 0) {
            continue;
        }

        if (!(pfd.revents & POLLIN)) {
            continue;
        }

        int client_fd = accept(tcp_server_fd, NULL, NULL);

        if (client_fd < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (atomic_load(&shutting_down)) {
                break;
            }

            perror("TCP accept");
            continue;
        }

        if (atomic_load(&shutting_down)) {
            close(client_fd);
            break;
        }

        struct timespec timeout;

        clock_gettime(CLOCK_REALTIME, &timeout);
        timeout.tv_sec += 5;

        if (sem_timedwait(&free_ports, &timeout) == -1) {
            send(client_fd, "SWITCH FULL\n", 12, MSG_NOSIGNAL);
            close(client_fd);
            continue;
        }

        if (atomic_load(&shutting_down)) {
            sem_post(&free_ports);
            close(client_fd);
            break;
        }

        int assigned_port = -1;

        for (int i = 0; i < running_ports; i++) {
            pthread_mutex_lock(&switch_ports[i].mutex);

            if (!switch_ports[i].active) {
                switch_ports[i].fd = client_fd;
                switch_ports[i].active = true;
                assigned_port = i;

                pthread_mutex_unlock(&switch_ports[i].mutex);
                break;
            }

            pthread_mutex_unlock(&switch_ports[i].mutex);
        }

        if (assigned_port == -1) {
            close(client_fd);
            sem_post(&free_ports);
            continue;
        }

        switch_port_t *port = &switch_ports[assigned_port];

        pthread_mutex_lock(&handler_mutex);
        active_handlers++;
        pthread_mutex_unlock(&handler_mutex);

        if (pthread_create(&port->thread, NULL, host_handler, port) != 0) {
            printf("Failed to create host thread\n");

            pthread_mutex_lock(&port->mutex);
            port->active = false;
            port->fd = -1;
            pthread_mutex_unlock(&port->mutex);

            pthread_mutex_lock(&handler_mutex);
            active_handlers--;
            pthread_cond_broadcast(&handler_cond);
            pthread_mutex_unlock(&handler_mutex);

            close(client_fd);
            sem_post(&free_ports);
            continue;
        }

        pthread_detach(port->thread);

        printf("Host connected on port %d\n", assigned_port);
    }

    return NULL;
}

void handle_command(int client_fd, char *command)
{
    if (strcmp(command, "show mac") == 0) {
        mac_table_show(client_fd);
    }
    else if (strcmp(command, "show ports") == 0) {
        char buffer[BUFFER_SIZE];

        pthread_mutex_lock(&shared_stats->mutex);

        snprintf(buffer, sizeof(buffer),
                 "Port  Frames In  Frames Out  Dropped  Mirrored\n");

        send(client_fd, buffer, strlen(buffer), MSG_NOSIGNAL);

        for (int i = 0; i < running_ports; i++) {
            snprintf(buffer, sizeof(buffer),
                     "%4d  %10lu  %10lu  %7lu  %8lu\n",
                     i,
                     shared_stats->ports[i].frames_in,
                     shared_stats->ports[i].frames_out,
                     shared_stats->ports[i].frames_dropped,
                     shared_stats->ports[i].frames_mirrored);

            send(client_fd, buffer, strlen(buffer), MSG_NOSIGNAL);
        }

        pthread_mutex_unlock(&shared_stats->mutex);
    }
    else if (strcmp(command, "clear mac") == 0) {
        mac_table_clear();

        send(client_fd, "MAC table cleared\n", 18, MSG_NOSIGNAL);
    }
    else {
        send(client_fd, "Unknown command\n", 16, MSG_NOSIGNAL);
    }
}

void *admin_listener(void *arg)
{
    (void)arg;

    struct pollfd pfd;

    pfd.fd = server_fd;
    pfd.events = POLLIN;

    while (!atomic_load(&shutting_down)) {
        int result = poll(&pfd, 1, 500);

        if (result < 0) {
            if (errno == EINTR) {
                continue;
            }

            perror("poll");
            break;
        }

        if (result == 0) {
            continue;
        }

        if (pfd.revents & POLLIN) {
            int client_fd = accept(server_fd, NULL, NULL);

            if (client_fd < 0) {
                if (errno == EINTR || errno == EAGAIN) {
                    continue;
                }

                if (atomic_load(&shutting_down)) {
                    break;
                }

                perror("accept");
                continue;
            }

            struct timeval receive_timeout;

            receive_timeout.tv_sec = 0;
            receive_timeout.tv_usec = 500000;

            if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout, sizeof(receive_timeout)) == -1) {
                perror("setsockopt");
                close(client_fd);
                continue;
            }

            char command[BUFFER_SIZE];
            int position = 0;
            char ch;

            while (position < BUFFER_SIZE - 1 && !atomic_load(&shutting_down)) {
                ssize_t n = recv(client_fd, &ch, 1, 0);

                if (n <= 0) {
                    break;
                }

                if (ch == '\n') {
                    break;
                }

                command[position++] = ch;
            }

            command[position] = '\0';

            if (position > 0) {
                handle_command(client_fd, command);
            }

            close(client_fd);
        }
    }

    return NULL;
}

void *signal_handler_thread(void *arg)
{
    (void)arg;

    sigset_t signals;

    sigemptyset(&signals);

    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGHUP);
    sigaddset(&signals, SIGUSR1);

    while (1) {
        int signal_number;

        int result = sigwait(&signals, &signal_number);

        if (result != 0) {
            continue;
        }

        if (signal_number == SIGUSR1) {
            FILE *file = fopen("mac_table_dump.txt", "w");

            if (file != NULL) {
                mac_table_dump(file);
                fclose(file);

                printf("MAC table dumped to mac_table_dump.txt\n");
            }
            else {
                perror("fopen");
            }
        }
        else if (signal_number == SIGHUP) {
            printf("SIGHUP received; no configuration file configured\n");
        }
        else if (signal_number == SIGINT) {
            printf("SIGINT received; shutting down\n");

            atomic_store(&shutting_down, true);

            break;
        }
    }

    return NULL;
}

int main(int argc, char *argv[])
{
    if (argc != 3) {
        printf("Usage: ./switch <tcp_port> <num_ports>\n");
        return 1;
    }

    tcp_port_number = atoi(argv[1]);
    running_ports = atoi(argv[2]);

    if (tcp_port_number < 1 || tcp_port_number > 65535) {
        printf("Invalid TCP port\n");
        return 1;
    }

    if (running_ports < 1 || running_ports > MAX_PORTS) {
        printf("Invalid number of ports\n");
        return 1;
    }

    for (int i = 0; i < running_ports; i++) {
        switch_ports[i].fd = -1;
        switch_ports[i].port = i;
        switch_ports[i].active = false;
        pthread_mutex_init(&switch_ports[i].mutex, NULL);
    }

    if (sem_init(&free_ports, 0, running_ports) == -1) {
        perror("sem_init");
        return 1;
    }

    sigset_t signals;

    sigemptyset(&signals);

    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGHUP);
    sigaddset(&signals, SIGUSR1);

    if (pthread_sigmask(SIG_BLOCK, &signals, NULL) != 0) {
        printf("Failed to block signals\n");
        return 1;
    }

    signal(SIGPIPE, SIG_IGN);

    tcp_server_fd = socket(AF_INET, SOCK_STREAM, 0);

    if (tcp_server_fd == -1) {
        perror("TCP socket");
        sem_destroy(&free_ports);
        return 1;
    }

    int reuse = 1;

    if (setsockopt(tcp_server_fd, SOL_SOCKET, SO_REUSEADDR,
                &reuse, sizeof(reuse)) == -1) {
        perror("TCP setsockopt");
        close(tcp_server_fd);
        sem_destroy(&free_ports);
        return 1;
    }

    struct sockaddr_in tcp_address;

    memset(&tcp_address, 0, sizeof(tcp_address));

    tcp_address.sin_family = AF_INET;
    tcp_address.sin_addr.s_addr = htonl(INADDR_ANY);
    tcp_address.sin_port = htons((uint16_t)tcp_port_number);

    if (bind(tcp_server_fd, (struct sockaddr *)&tcp_address,
            sizeof(tcp_address)) == -1) {
        perror("TCP bind");
        close(tcp_server_fd);
        sem_destroy(&free_ports);
        return 1;
    }

    if (listen(tcp_server_fd, 64) == -1) {
        perror("TCP listen");
        close(tcp_server_fd);
        sem_destroy(&free_ports);
        return 1;
    }

    mac_table_init();

    if (shm_stats_create(&shared_stats, &shm_fd, running_ports) == -1) {
        mac_table_destroy();
        return 1;
    }

    server_fd = socket(AF_UNIX, SOCK_STREAM, 0);

    if (server_fd == -1) {
        perror("socket");
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }

    struct sockaddr_un address;

    memset(&address, 0, sizeof(address));

    address.sun_family = AF_UNIX;

    strncpy(address.sun_path, SOCKET_PATH,
            sizeof(address.sun_path) - 1);

    unlink(SOCKET_PATH);

    if (bind(server_fd, (struct sockaddr *)&address,
             sizeof(address)) == -1) {
        perror("bind");
        close(server_fd);
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }

    if (listen(server_fd, 10) == -1) {
        perror("listen");
        close(server_fd);
        unlink(SOCKET_PATH);
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }

    printf("Virtual Switch Control Plane Started\n");
    printf("Ports: %d\n", running_ports);
    printf("Socket: %s\n", SOCKET_PATH);
    printf("Shared Memory: %s\n", SHM_NAME);

    if (pthread_create(&signal_thread, NULL,
                       signal_handler_thread, NULL) != 0) {
        printf("Failed to create signal thread\n");
        close(server_fd);
        unlink(SOCKET_PATH);
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }

    if (pthread_create(&admin_thread, NULL,
                       admin_listener, NULL) != 0) {
        printf("Failed to create admin thread\n");

        pthread_kill(signal_thread, SIGINT);
        pthread_join(signal_thread, NULL);

        close(server_fd);
        unlink(SOCKET_PATH);

        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();

        return 1;
    }

    if (pthread_create(&tcp_listener_thread, NULL, tcp_listener, NULL) != 0) {
        printf("Failed to create TCP listener thread\n");

        atomic_store(&shutting_down, true);
        pthread_kill(signal_thread, SIGINT);
        pthread_join(signal_thread, NULL);

        pthread_join(admin_thread, NULL);

        close(tcp_server_fd);
        close(server_fd);
        unlink(SOCKET_PATH);

        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        sem_destroy(&free_ports);

        return 1;
    }

    pthread_join(signal_thread, NULL);

    atomic_store(&shutting_down, true);

    pthread_join(tcp_listener_thread, NULL);

    for (int i = 0; i < running_ports; i++) {
        pthread_mutex_lock(&switch_ports[i].mutex);

        if (switch_ports[i].active && switch_ports[i].fd >= 0) {
            shutdown(switch_ports[i].fd, SHUT_RDWR);
        }

        pthread_mutex_unlock(&switch_ports[i].mutex);
    }

    pthread_mutex_lock(&handler_mutex);

    while (active_handlers > 0) {
        pthread_cond_wait(&handler_cond, &handler_mutex);
    }

    pthread_mutex_unlock(&handler_mutex);

    pthread_join(admin_thread, NULL);

    close(server_fd);
    close(tcp_server_fd);

    unlink(SOCKET_PATH);

    for (int i = 0; i < running_ports; i++) {
        pthread_mutex_destroy(&switch_ports[i].mutex);
    }

    sem_destroy(&free_ports);

    mac_table_destroy();

    shm_stats_destroy(shared_stats, shm_fd);

    printf("Switch Control Plane Stopped Cleanly\n");

    return 0;
}