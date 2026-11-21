#include <stdio.h>
#include <stdint.h>
#include <pthread.h>

#include "frame.h"
#include "mac_table.h"

void test_frame(void)
{
    frame_header_t header;

    header.length = 100;

    uint8_t src[6] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
    uint8_t dst[6] = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};

    for (int i = 0; i < 6; i++) {
        header.src[i] = src[i];
        header.dst[i] = dst[i];
    }

    header.vlan = 10;
    header.priority = 1;

    uint8_t buffer[FRAME_HEADER_SIZE];

    frame_serialize(&header, buffer);

    printf("Serialized Header:\n");

    for (int i = 0; i < FRAME_HEADER_SIZE; i++) {
        printf("%02X ", buffer[i]);
    }

    printf("\n");

    frame_header_t decoded;

    frame_deserialize(buffer, &decoded);

    printf("\nDeserialized Header:\n");
    printf("Length: %u\n", decoded.length);

    printf("Source: ");

    for (int i = 0; i < 6; i++) {
        printf("%02X", decoded.src[i]);
        if (i != 5) printf(":");
    }

    printf("\nDestination: ");

    for (int i = 0; i < 6; i++) {
        printf("%02X", decoded.dst[i]);
        if (i != 5) printf(":");
    }

    printf("\nVLAN: %u\n", decoded.vlan);
    printf("Priority: %u\n", decoded.priority);
}

void *insert_thread(void *arg)
{
    mac_table_t *table = (mac_table_t *)arg;

    uint8_t mac[6] = {0x10, 0x20, 0x30, 0x40, 0x50, 0x60};

    for (int i = 0; i < 100; i++) {
        mac_table_insert(table, mac, i % 4);
    }

    return NULL;
}

void test_mac_table(void)
{
    mac_table_t table;

    if (mac_table_init(&table) != 0) {
        printf("MAC table initialization failed\n");
        return;
    }

    uint8_t mac1[6] = {0x00, 0x11, 0x22, 0x33, 0x44, 0x55};
    uint8_t mac2[6] = {0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF};

    printf("\nMAC Table Testing:\n");

    mac_table_insert(&table, mac1, 1);
    mac_table_insert(&table, mac2, 2);

    mac_table_insert(&table, mac1, 3);

    int port;

    if (mac_table_lookup(&table, mac1, &port) == 1) {
        printf("MAC 1 found on port %d\n", port);
    }

    if (mac_table_lookup(&table, mac2, &port) == 1) {
        printf("MAC 2 found on port %d\n", port);
    }

    printf("\nConcurrent Testing:\n");

    pthread_t threads[4];

    for (int i = 0; i < 4; i++) {
        pthread_create(&threads[i], NULL, insert_thread, &table);
    }

    for (int i = 0; i < 4; i++) {
        pthread_join(threads[i], NULL);
    }

    uint8_t mac3[6] = {0x10, 0x20, 0x30, 0x40, 0x50, 0x60};

    if (mac_table_lookup(&table, mac3, &port) == 1) {
        printf("Concurrent MAC lookup successful. Port: %d\n", port);
    }

    mac_table_destroy(&table);
}

int main(void)
{
    printf("========== FRAME TEST ==========\n");

    test_frame();

    printf("\n========== MAC TABLE TEST ==========\n");

    test_mac_table();

    return 0;
}