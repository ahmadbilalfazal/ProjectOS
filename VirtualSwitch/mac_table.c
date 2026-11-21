#include "mac_table.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static unsigned int mac_hash(const uint8_t mac[6])
{
    unsigned int hash = 0;

    for (int i = 0; i < 6; i++) {
        hash = hash * 31 + mac[i];
    }

    return hash % MAC_BUCKETS;
}

int mac_table_init(mac_table_t *table)
{
    if (table == NULL) {
        return -1;
    }

    for (int i = 0; i < MAC_BUCKETS; i++) {
        table->buckets[i].head = NULL;

        if (pthread_mutex_init(&table->buckets[i].mutex, NULL) != 0) {
            for (int j = 0; j < i; j++) {
                pthread_mutex_destroy(&table->buckets[j].mutex);
            }

            return -1;
        }
    }

    return 0;
}

void mac_table_destroy(mac_table_t *table)
{
    if (table == NULL) {
        return;
    }

    for (int i = 0; i < MAC_BUCKETS; i++) {
        mac_entry_t *entry = table->buckets[i].head;

        while (entry != NULL) {
            mac_entry_t *next = entry->next;

            free(entry);

            entry = next;
        }

        table->buckets[i].head = NULL;

        pthread_mutex_destroy(&table->buckets[i].mutex);
    }
}

int mac_table_lookup(mac_table_t *table, const uint8_t mac[6], int *port)
{
    if (table == NULL || mac == NULL || port == NULL) {
        return -1;
    }

    unsigned int bucket = mac_hash(mac);

    pthread_mutex_lock(&table->buckets[bucket].mutex);

    mac_entry_t *entry = table->buckets[bucket].head;

    while (entry != NULL) {
        if (memcmp(entry->mac, mac, 6) == 0) {
            *port = entry->port;

            pthread_mutex_unlock(&table->buckets[bucket].mutex);

            return 1;
        }

        entry = entry->next;
    }

    pthread_mutex_unlock(&table->buckets[bucket].mutex);

    return 0;
}

int mac_table_insert(mac_table_t *table, const uint8_t mac[6], int port)
{
    if (table == NULL || mac == NULL || port < 0) {
        return -1;
    }

    unsigned int bucket = mac_hash(mac);

    pthread_mutex_lock(&table->buckets[bucket].mutex);

    mac_entry_t *entry = table->buckets[bucket].head;

    while (entry != NULL) {
        if (memcmp(entry->mac, mac, 6) == 0) {
            if (entry->port != port) {
                printf("MAC moved: port %d -> port %d\n",
                       entry->port, port);

                entry->port = port;
            }

            pthread_mutex_unlock(&table->buckets[bucket].mutex);

            return 0;
        }

        entry = entry->next;
    }

    entry = malloc(sizeof(mac_entry_t));

    if (entry == NULL) {
        pthread_mutex_unlock(&table->buckets[bucket].mutex);

        return -1;
    }

    memcpy(entry->mac, mac, 6);

    entry->port = port;

    entry->next = table->buckets[bucket].head;

    table->buckets[bucket].head = entry;

    pthread_mutex_unlock(&table->buckets[bucket].mutex);

    return 0;
}