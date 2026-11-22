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
#include <stdarg.h>

#include "shm_stats.h"
#include "frame.h"
#include "net_io.h"

#include <arpa/inet.h>
#include <semaphore.h>

#define SOCKET_PATH "/tmp/vswitch.sock"
#define MAC_BUCKETS 16
#define MAC_TABLE_SIZE 256
#define BUFFER_SIZE 512
#define QUEUE_LIMIT 128
#define TOKEN_RATE 1048576.0
#define TOKEN_BURST 65536.0
#define MAC_AGE_SECONDS 60
#define MACS_PER_PORT 16

#define MAX_FRAME_SIZE (FRAME_HEADER_SIZE + MAX_PAYLOAD_SIZE)

typedef struct frame_blob {
    atomic_int references;
    size_t length;
    unsigned char data[MAX_FRAME_SIZE];
} frame_blob_t;

typedef struct queue_node {
    frame_blob_t *frame;
    struct timespec queued_at;
    struct queue_node *next;
} queue_node_t;

typedef struct {
    int fd;
    int port;
    pthread_t thread;
    pthread_t sender_thread;
    bool active;
    bool administratively_up;
    int vlan;
    bool trunk;
    int mirror_port;
    pthread_mutex_t mutex;
    pthread_mutex_t queue_mutex;
    pthread_mutex_t token_mutex;
    pthread_cond_t queue_cond;
    queue_node_t *head[3];
    queue_node_t *tail[3];
    int queue_count;
    int rr_index;
    int rr_used[3];
    double tokens;
    struct timespec token_time;
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
static pthread_mutex_t mac_manage_mutex = PTHREAD_MUTEX_INITIALIZER;
static int mac_entry_count = 0;
static pthread_t aging_thread;
static pthread_t timer_thread;
static pthread_t logger_thread;
static pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t log_cond = PTHREAD_COND_INITIALIZER;
static char log_queue[128][256];
static int log_head = 0, log_tail = 0, log_count = 0;
static bool logger_stopping = false;

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


static void log_message(const char *format, ...)
{
    char message[256];
    va_list args;
    va_start(args, format);
    vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    pthread_mutex_lock(&log_mutex);
    if (log_count < 128) {
        snprintf(log_queue[log_tail], sizeof(log_queue[log_tail]), "%s", message);
        log_tail = (log_tail + 1) % 128;
        log_count++;
        pthread_cond_signal(&log_cond);
    }
    pthread_mutex_unlock(&log_mutex);
}

static void *logger_worker(void *arg)
{
    (void)arg;
    FILE *file = fopen("switch.log", "a");
    if (file == NULL) return NULL;
    for (;;) {
        pthread_mutex_lock(&log_mutex);
        while (log_count == 0 && !logger_stopping)
            pthread_cond_wait(&log_cond, &log_mutex);
        if (log_count == 0 && logger_stopping) {
            pthread_mutex_unlock(&log_mutex);
            break;
        }
        char message[256];
        snprintf(message, sizeof(message), "%s", log_queue[log_head]);
        log_head = (log_head + 1) % 128;
        log_count--;
        pthread_mutex_unlock(&log_mutex);
        time_t now = time(NULL);
        struct tm tm_now;
        localtime_r(&now, &tm_now);
        char stamp[32];
        strftime(stamp, sizeof(stamp), "%Y-%m-%d %H:%M:%S", &tm_now);
        fprintf(file, "[%s] %s\n", stamp, message);
        fflush(file);
    }
    fclose(file);
    return NULL;
}

static void logger_stop(void)
{
    pthread_mutex_lock(&log_mutex);
    logger_stopping = true;
    pthread_cond_broadcast(&log_cond);
    pthread_mutex_unlock(&log_mutex);
    pthread_join(logger_thread, NULL);
    pthread_cond_destroy(&log_cond);
    pthread_mutex_destroy(&log_mutex);
}

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
    pthread_mutex_lock(&mac_manage_mutex);
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
    mac_entry_count = 0;
    pthread_mutex_unlock(&mac_manage_mutex);
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
    pthread_mutex_lock(&mac_manage_mutex);
    pthread_mutex_lock(&mac_table[bucket].mutex);
    mac_entry_t *entry = mac_table[bucket].head;
    while (entry != NULL) {
        if (memcmp(entry->mac, mac, 6) == 0) {
            if (entry->port != port) log_message("MAC moved: port %d -> port %d", entry->port, port);
            entry->port = port;
            entry->updated = time(NULL);
            pthread_mutex_unlock(&mac_table[bucket].mutex);
            pthread_mutex_unlock(&mac_manage_mutex);
            return 0;
        }
        entry = entry->next;
    }
    pthread_mutex_unlock(&mac_table[bucket].mutex);

    int on_port = 0;
    for (int i = 0; i < MAC_BUCKETS; i++) {
        pthread_mutex_lock(&mac_table[i].mutex);
        for (entry = mac_table[i].head; entry != NULL; entry = entry->next)
            if (entry->port == port) on_port++;
        pthread_mutex_unlock(&mac_table[i].mutex);
    }
    if (on_port >= MACS_PER_PORT) {
        pthread_mutex_unlock(&mac_manage_mutex);
        return -2;
    }

    if (mac_entry_count >= MAC_TABLE_SIZE) {
        time_t oldest_time = 0;
        unsigned int oldest_bucket = 0;
        unsigned char oldest_mac[6] = {0};
        for (int i = 0; i < MAC_BUCKETS; i++) {
            pthread_mutex_lock(&mac_table[i].mutex);
            for (entry = mac_table[i].head; entry != NULL; entry = entry->next) {
                if (oldest_time == 0 || entry->updated < oldest_time) {
                    oldest_time = entry->updated;
                    oldest_bucket = (unsigned int)i;
                    memcpy(oldest_mac, entry->mac, 6);
                }
            }
            pthread_mutex_unlock(&mac_table[i].mutex);
        }
        pthread_mutex_lock(&mac_table[oldest_bucket].mutex);
        mac_entry_t **link = &mac_table[oldest_bucket].head;
        while (*link != NULL) {
            if (memcmp((*link)->mac, oldest_mac, 6) == 0) {
                mac_entry_t *old = *link;
                *link = old->next;
                free(old);
                mac_entry_count--;
                break;
            }
            link = &(*link)->next;
        }
        pthread_mutex_unlock(&mac_table[oldest_bucket].mutex);
    }

    entry = malloc(sizeof(mac_entry_t));
    if (entry == NULL) {
        pthread_mutex_unlock(&mac_manage_mutex);
        return -1;
    }
    memcpy(entry->mac, mac, 6);
    entry->port = port;
    entry->updated = time(NULL);
    pthread_mutex_lock(&mac_table[bucket].mutex);
    entry->next = mac_table[bucket].head;
    mac_table[bucket].head = entry;
    pthread_mutex_unlock(&mac_table[bucket].mutex);
    mac_entry_count++;
    pthread_mutex_unlock(&mac_manage_mutex);
    return 0;
}

static void *mac_aging_thread(void *arg)
{
    (void)arg;
    while (!atomic_load(&shutting_down)) {
        sleep(5);
        if (atomic_load(&shutting_down)) break;
        time_t now = time(NULL);
        pthread_mutex_lock(&mac_manage_mutex);
        for (int i = 0; i < MAC_BUCKETS; i++) {
            pthread_mutex_lock(&mac_table[i].mutex);
            mac_entry_t **link = &mac_table[i].head;
            while (*link != NULL) {
                mac_entry_t *entry = *link;
                if (now - entry->updated > MAC_AGE_SECONDS) {
                    *link = entry->next;
                    free(entry);
                    mac_entry_count--;
                } else link = &entry->next;
            }
            pthread_mutex_unlock(&mac_table[i].mutex);
        }
        pthread_mutex_unlock(&mac_manage_mutex);
    }
    return NULL;
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

static void frame_release(frame_blob_t *frame)
{
    if (atomic_fetch_sub(&frame->references, 1) == 1) free(frame);
}

static int enqueue_frame(int target, frame_blob_t *frame, int priority)
{
    switch_port_t *port = &switch_ports[target];
    queue_node_t *node = malloc(sizeof(*node));
    if (node == NULL) return -1;
    node->frame = frame;
    clock_gettime(CLOCK_MONOTONIC, &node->queued_at);
    node->next = NULL;

    pthread_mutex_lock(&port->queue_mutex);
    if (port->queue_count >= QUEUE_LIMIT || !port->administratively_up) {
        pthread_mutex_unlock(&port->queue_mutex);
        free(node);
        return -1;
    }
    atomic_fetch_add(&frame->references, 1);
    if (port->tail[priority] != NULL) port->tail[priority]->next = node;
    else port->head[priority] = node;
    port->tail[priority] = node;
    port->queue_count++;
    pthread_cond_signal(&port->queue_cond);
    pthread_mutex_unlock(&port->queue_mutex);
    return 0;
}

static queue_node_t *dequeue_frame(switch_port_t *port)
{
    static const int weights[3] = {5, 3, 1};
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    for (int priority = 2; priority >= 1; priority--) {
        if (port->head[priority] != NULL) {
            double age = (double)(now.tv_sec - port->head[priority]->queued_at.tv_sec) +
                         (double)(now.tv_nsec - port->head[priority]->queued_at.tv_nsec) / 1000000000.0;
            if (age >= 2.0) {
                queue_node_t *aged = port->head[priority];
                port->head[priority] = aged->next;
                if (port->head[priority] == NULL) port->tail[priority] = NULL;
                port->queue_count--;
                return aged;
            }
        }
    }
    for (int tries = 0; tries < 9; tries++) {
        int priority = port->rr_index;
        if (port->head[priority] != NULL) {
            queue_node_t *node = port->head[priority];
            port->head[priority] = node->next;
            if (port->head[priority] == NULL) port->tail[priority] = NULL;
            port->queue_count--;
            port->rr_used[priority]++;
            if (port->rr_used[priority] >= weights[priority] || port->head[priority] == NULL) {
                port->rr_used[priority] = 0;
                port->rr_index = (priority + 1) % 3;
            }
            return node;
        }
        port->rr_index = (port->rr_index + 1) % 3;
    }
    return NULL;
}

static void *token_timer_thread(void *arg)
{
    (void)arg;
    const struct timespec interval = {0, 100000000L};
    while (!atomic_load(&shutting_down)) {
        nanosleep(&interval, NULL);
        struct timespec now;
        clock_gettime(CLOCK_MONOTONIC, &now);
        for (int i = 0; i < running_ports; i++) {
            switch_port_t *port = &switch_ports[i];
            pthread_mutex_lock(&port->token_mutex);
            double elapsed = (double)(now.tv_sec - port->token_time.tv_sec) +
                             (double)(now.tv_nsec - port->token_time.tv_nsec) / 1000000000.0;
            if (elapsed > 0) {
                port->tokens += elapsed * TOKEN_RATE;
                if (port->tokens > TOKEN_BURST) port->tokens = TOKEN_BURST;
                port->token_time = now;
            }
            pthread_mutex_unlock(&port->token_mutex);
            pthread_mutex_lock(&port->queue_mutex);
            pthread_cond_signal(&port->queue_cond);
            pthread_mutex_unlock(&port->queue_mutex);
        }
    }
    return NULL;
}

static void *port_sender(void *arg)
{
    switch_port_t *port = arg;
    while (!atomic_load(&shutting_down)) {
        pthread_mutex_lock(&port->queue_mutex);
        while (port->queue_count == 0 && !atomic_load(&shutting_down))
            pthread_cond_wait(&port->queue_cond, &port->queue_mutex);
        queue_node_t *node = dequeue_frame(port);
        pthread_mutex_unlock(&port->queue_mutex);
        if (node == NULL) continue;

        while (!atomic_load(&shutting_down)) {
            pthread_mutex_lock(&port->token_mutex);
            bool has_tokens = port->tokens >= (double)node->frame->length;
            if (has_tokens) port->tokens -= (double)node->frame->length;
            pthread_mutex_unlock(&port->token_mutex);
            if (has_tokens) break;
            struct timespec until;
            clock_gettime(CLOCK_REALTIME, &until);
            until.tv_nsec += 200000000L;
            if (until.tv_nsec >= 1000000000L) { until.tv_sec++; until.tv_nsec -= 1000000000L; }
            pthread_mutex_lock(&port->queue_mutex);
            pthread_cond_timedwait(&port->queue_cond, &port->queue_mutex, &until);
            pthread_mutex_unlock(&port->queue_mutex);
        }

        pthread_mutex_lock(&port->mutex);
        int fd = port->active ? port->fd : -1;
        int sent = fd >= 0 ? net_send_all(fd, node->frame->data, node->frame->length) : -1;
        pthread_mutex_unlock(&port->mutex);
        pthread_mutex_lock(&shared_stats->mutex);
        if (sent == 0) shared_stats->ports[port->port].frames_out++;
        else shared_stats->ports[port->port].frames_dropped++;
        pthread_mutex_unlock(&shared_stats->mutex);
        frame_release(node->frame);
        free(node);
    }
    return NULL;
}

void forward_frame(int source_port, const unsigned char *frame, size_t length,
                   const unsigned char *destination, int vlan, int priority)
{
    frame_blob_t *shared = malloc(sizeof(*shared));
    if (shared == NULL) return;
    atomic_init(&shared->references, 1);
    shared->length = length;
    memcpy(shared->data, frame, length);

    int target_port = -1;
    int known = mac_table_lookup(destination, &target_port);
    pthread_mutex_lock(&switch_ports[source_port].mutex);
    int mirror = switch_ports[source_port].mirror_port;
    pthread_mutex_unlock(&switch_ports[source_port].mutex);
    for (int i = 0; i < running_ports; i++) {
        if (i == source_port) continue;
        if (known && i != target_port) continue;
        pthread_mutex_lock(&switch_ports[i].mutex);
        bool vlan_allowed = switch_ports[i].trunk || switch_ports[i].vlan == vlan;
        pthread_mutex_unlock(&switch_ports[i].mutex);
        if (!vlan_allowed) continue;
        if (enqueue_frame(i, shared, priority) == -1) {
            pthread_mutex_lock(&shared_stats->mutex);
            shared_stats->ports[i].frames_dropped++;
            pthread_mutex_unlock(&shared_stats->mutex);
        }
    }
    if (mirror >= 0 && mirror < running_ports && mirror != source_port && mirror != target_port) {
        if (enqueue_frame(mirror, shared, priority) == 0) {
            pthread_mutex_lock(&shared_stats->mutex);
            shared_stats->ports[mirror].frames_mirrored++;
            pthread_mutex_unlock(&shared_stats->mutex);
        }
    }
    frame_release(shared);
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

        pthread_mutex_lock(&port->mutex);
        bool vlan_allowed = port->trunk || header.vlan == port->vlan;
        pthread_mutex_unlock(&port->mutex);
        if (!vlan_allowed) {
            pthread_mutex_lock(&shared_stats->mutex);
            shared_stats->ports[port->port].frames_dropped++;
            pthread_mutex_unlock(&shared_stats->mutex);
            continue;
        }
        int learn_result = mac_table_learn(header.src, port->port);
        if (learn_result == -2) {
            log_message("Port security limit exceeded on port %d; shutting port down", port->port);
            pthread_mutex_lock(&port->queue_mutex);
            port->administratively_up = false;
            pthread_mutex_unlock(&port->queue_mutex);
            break;
        }

        pthread_mutex_lock(&shared_stats->mutex);
        shared_stats->ports[port->port].frames_in++;
        pthread_mutex_unlock(&shared_stats->mutex);

        forward_frame(port->port, frame, frame_length, header.dst, header.vlan, header.priority);
    }

    pthread_mutex_lock(&port->mutex);

    if (port->fd == fd) {
        port->active = false;
        port->fd = -1;
    }

    pthread_mutex_unlock(&port->mutex);

    close(fd);
    sem_post(&free_ports);

    log_message("Host disconnected from port %d", port->port);

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

        log_message("Host connected on port %d", assigned_port);
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
    else if (strncmp(command, "shutdown ", 9) == 0 || strncmp(command, "up ", 3) == 0) {
        int port_number = atoi(command + (command[0] == 'u' ? 3 : 9));
        if (port_number < 0 || port_number >= running_ports) { send(client_fd, "Invalid port\n", 13, MSG_NOSIGNAL); return; }
        switch_port_t *port = &switch_ports[port_number];
        pthread_mutex_lock(&port->queue_mutex);
        port->administratively_up = command[0] == 'u';
        if (!port->administratively_up) {
            for (int q = 0; q < 3; q++) {
                queue_node_t *node = port->head[q];
                while (node != NULL) { queue_node_t *next = node->next; frame_release(node->frame); free(node); node = next; }
                port->head[q] = port->tail[q] = NULL;
            }
            port->queue_count = 0;
        }
        pthread_cond_broadcast(&port->queue_cond);
        pthread_mutex_unlock(&port->queue_mutex);
        if (!port->administratively_up) { pthread_mutex_lock(&port->mutex); if (port->fd >= 0) shutdown(port->fd, SHUT_RDWR); pthread_mutex_unlock(&port->mutex); }
        send(client_fd, "Port updated\n", 13, MSG_NOSIGNAL);
    }
    else if (strncmp(command, "vlan ", 5) == 0) {
        int port_number = -1, vlan = -1;
        if (sscanf(command + 5, "%d %d", &port_number, &vlan) != 2 || port_number < 0 || port_number >= running_ports || vlan < 1 || vlan > 4094) { send(client_fd, "Usage: vlan <port> <1-4094>\n", 29, MSG_NOSIGNAL); return; }
        pthread_mutex_lock(&switch_ports[port_number].mutex);
        switch_ports[port_number].vlan = vlan;
        switch_ports[port_number].trunk = false;
        pthread_mutex_unlock(&switch_ports[port_number].mutex);
        send(client_fd, "VLAN updated\n", 13, MSG_NOSIGNAL);
    }
    else if (strncmp(command, "trunk ", 6) == 0) {
        int port_number = atoi(command + 6);
        if (port_number < 0 || port_number >= running_ports) { send(client_fd, "Invalid port\n", 13, MSG_NOSIGNAL); return; }
        pthread_mutex_lock(&switch_ports[port_number].mutex);
        switch_ports[port_number].trunk = true;
        pthread_mutex_unlock(&switch_ports[port_number].mutex);
        send(client_fd, "Trunk enabled\n", strlen("Trunk enabled\n"), MSG_NOSIGNAL);
    }
    else if (strncmp(command, "mirror ", 7) == 0) {
        int source = -1, target = -1;
        if (sscanf(command + 7, "%d %d", &source, &target) != 2 || source < 0 || target < 0 || source >= running_ports || target >= running_ports || source == target) { send(client_fd, "Usage: mirror <source> <target>\n", 33, MSG_NOSIGNAL); return; }
        pthread_mutex_lock(&switch_ports[source].mutex);
        switch_ports[source].mirror_port = target;
        pthread_mutex_unlock(&switch_ports[source].mutex);
        send(client_fd, "Mirror configured\n", strlen("Mirror configured\n"), MSG_NOSIGNAL);
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
            FILE *config = fopen("vswitch.conf", "r");
            if (config == NULL) {
                printf("SIGHUP: vswitch.conf not found\n");
            } else {
                char line[128];
                while (fgets(line, sizeof(line), config) != NULL) {
                    int port_number = -1, vlan = 1;
                    char mode[32];
                    int fields = sscanf(line, "%d %31s %d", &port_number, mode, &vlan);
                    if (fields < 2 || port_number < 0 || port_number >= running_ports) continue;
                    pthread_mutex_lock(&switch_ports[port_number].mutex);
                    if (strcmp(mode, "trunk") == 0) switch_ports[port_number].trunk = true;
                    else if (strcmp(mode, "access") == 0 && fields == 3 && vlan >= 1 && vlan <= 4094) {
                        switch_ports[port_number].trunk = false;
                        switch_ports[port_number].vlan = vlan;
                    }
                    pthread_mutex_unlock(&switch_ports[port_number].mutex);
                }
                fclose(config);
                printf("SIGHUP: configuration reloaded\n");
            }
        }
        else if (signal_number == SIGINT) {
            log_message("SIGINT received; shutting down");
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
        switch_ports[i].administratively_up = true;
        switch_ports[i].vlan = 1;
        switch_ports[i].trunk = false;
        switch_ports[i].mirror_port = -1;
        switch_ports[i].tokens = TOKEN_BURST;
        clock_gettime(CLOCK_MONOTONIC, &switch_ports[i].token_time);
        pthread_mutex_init(&switch_ports[i].mutex, NULL);
        pthread_mutex_init(&switch_ports[i].queue_mutex, NULL);
        pthread_mutex_init(&switch_ports[i].token_mutex, NULL);
        pthread_cond_init(&switch_ports[i].queue_cond, NULL);
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

    if (pthread_create(&logger_thread, NULL, logger_worker, NULL) != 0) {
        fprintf(stderr, "Failed to start logger thread\n");
        close(server_fd);
        unlink(SOCKET_PATH);
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }
    if (pthread_create(&aging_thread, NULL, mac_aging_thread, NULL) != 0) {
        fprintf(stderr, "Failed to start MAC aging thread\n");
        logger_stop();
        close(server_fd);
        unlink(SOCKET_PATH);
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }
    if (pthread_create(&timer_thread, NULL, token_timer_thread, NULL) != 0) {
        fprintf(stderr, "Failed to start token timer thread\n");
        atomic_store(&shutting_down, true);
        pthread_join(aging_thread, NULL);
        log_message("Startup failed");
        logger_stop();
        close(server_fd);
        unlink(SOCKET_PATH);
        shm_stats_destroy(shared_stats, shm_fd);
        mac_table_destroy();
        return 1;
    }
    for (int i = 0; i < running_ports; i++) {
        if (pthread_create(&switch_ports[i].sender_thread, NULL, port_sender, &switch_ports[i]) != 0) {
            fprintf(stderr, "Failed to create sender thread for port %d\n", i);
            atomic_store(&shutting_down, true);
            for (int j = 0; j < i; j++) {
                pthread_mutex_lock(&switch_ports[j].queue_mutex);
                pthread_cond_broadcast(&switch_ports[j].queue_cond);
                pthread_mutex_unlock(&switch_ports[j].queue_mutex);
                pthread_join(switch_ports[j].sender_thread, NULL);
            }
            pthread_join(timer_thread, NULL);
            pthread_join(aging_thread, NULL);
            log_message("Startup failed");
            logger_stop();
            close(server_fd);
            unlink(SOCKET_PATH);
            shm_stats_destroy(shared_stats, shm_fd);
            mac_table_destroy();
            return 1;
        }
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
        pthread_mutex_lock(&switch_ports[i].queue_mutex);
        pthread_cond_broadcast(&switch_ports[i].queue_cond);
        pthread_mutex_unlock(&switch_ports[i].queue_mutex);
        pthread_join(switch_ports[i].sender_thread, NULL);
        for (int q = 0; q < 3; q++) {
            queue_node_t *node = switch_ports[i].head[q];
            while (node != NULL) { queue_node_t *next = node->next; frame_release(node->frame); free(node); node = next; }
        }
        pthread_cond_destroy(&switch_ports[i].queue_cond);
        pthread_mutex_destroy(&switch_ports[i].token_mutex);
        pthread_mutex_destroy(&switch_ports[i].queue_mutex);
        pthread_mutex_destroy(&switch_ports[i].mutex);
    }

    sem_destroy(&free_ports);

    pthread_join(timer_thread, NULL);
    pthread_join(aging_thread, NULL);
    log_message("Switch stopped cleanly");
    logger_stop();
    mac_table_destroy();

    shm_stats_destroy(shared_stats, shm_fd);

    printf("Switch Control Plane Stopped Cleanly\n");

    return 0;
}