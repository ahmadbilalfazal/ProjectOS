#ifndef SHM_STATS_H
#define SHM_STATS_H

#include <pthread.h>

#define SHM_NAME "/vswitch_stats"
#define MAX_PORTS 64

typedef struct {
    unsigned long frames_in;
    unsigned long frames_out;
    unsigned long frames_dropped;
    unsigned long frames_mirrored;
} port_stats_t;

typedef struct {
    pthread_mutex_t mutex;
    int num_ports;
    port_stats_t ports[MAX_PORTS];
} shm_stats_t;

int shm_stats_create(shm_stats_t **stats, int *fd, int num_ports);

int shm_stats_attach(shm_stats_t **stats, int *fd);

void shm_stats_detach(shm_stats_t *stats, int fd);

void shm_stats_destroy(shm_stats_t *stats, int fd);

#endif