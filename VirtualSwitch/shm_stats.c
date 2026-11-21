#include "shm_stats.h"

#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

int shm_stats_create(shm_stats_t **stats, int *fd, int num_ports)
{
    *fd = shm_open(SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0666);

    if (*fd == -1) {
        perror("shm_open");
        return -1;
    }

    if (ftruncate(*fd, sizeof(shm_stats_t)) == -1) {
        perror("ftruncate");
        close(*fd);
        shm_unlink(SHM_NAME);
        return -1;
    }

    *stats = mmap(NULL, sizeof(shm_stats_t),
                  PROT_READ | PROT_WRITE, MAP_SHARED, *fd, 0);

    if (*stats == MAP_FAILED) {
        perror("mmap");
        close(*fd);
        shm_unlink(SHM_NAME);
        return -1;
    }

    memset(*stats, 0, sizeof(shm_stats_t));

    pthread_mutexattr_t attr;

    if (pthread_mutexattr_init(&attr) != 0) {
        munmap(*stats, sizeof(shm_stats_t));
        close(*fd);
        shm_unlink(SHM_NAME);
        return -1;
    }

    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0) {
        pthread_mutexattr_destroy(&attr);
        munmap(*stats, sizeof(shm_stats_t));
        close(*fd);
        shm_unlink(SHM_NAME);
        return -1;
    }

    if (pthread_mutex_init(&(*stats)->mutex, &attr) != 0) {
        pthread_mutexattr_destroy(&attr);
        munmap(*stats, sizeof(shm_stats_t));
        close(*fd);
        shm_unlink(SHM_NAME);
        return -1;
    }

    pthread_mutexattr_destroy(&attr);

    (*stats)->num_ports = num_ports;

    return 0;
}

int shm_stats_attach(shm_stats_t **stats, int *fd)
{
    *fd = shm_open(SHM_NAME, O_RDWR, 0666);

    if (*fd == -1) {
        perror("shm_open");
        return -1;
    }

    struct stat st;

    if (fstat(*fd, &st) == -1 ||
        st.st_size < (off_t)sizeof(shm_stats_t)) {
        perror("fstat");
        close(*fd);
        return -1;
    }

    *stats = mmap(NULL, sizeof(shm_stats_t),
                  PROT_READ | PROT_WRITE, MAP_SHARED, *fd, 0);

    if (*stats == MAP_FAILED) {
        perror("mmap");
        close(*fd);
        return -1;
    }

    return 0;
}

void shm_stats_detach(shm_stats_t *stats, int fd)
{
    munmap(stats, sizeof(shm_stats_t));
    close(fd);
}

void shm_stats_destroy(shm_stats_t *stats, int fd)
{
    pthread_mutex_destroy(&stats->mutex);

    munmap(stats, sizeof(shm_stats_t));

    close(fd);

    shm_unlink(SHM_NAME);
}