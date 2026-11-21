#include "frame.h"

#include <string.h>
#include <arpa/inet.h>

int frame_serialize(const frame_header_t *header, uint8_t *buffer)
{
    if (header == NULL || buffer == NULL) {
        return -1;
    }

    if (header->length > MAX_PAYLOAD_SIZE) {
        return -1;
    }

    uint16_t length = htons(header->length);
    uint16_t vlan = htons(header->vlan);

    memcpy(buffer, &length, 2);

    memcpy(buffer + 2, header->src, 6);

    memcpy(buffer + 8, header->dst, 6);

    memcpy(buffer + 14, &vlan, 2);

    buffer[16] = header->priority;

    return FRAME_HEADER_SIZE;
}

int frame_deserialize(const uint8_t *buffer, frame_header_t *header)
{
    if (buffer == NULL || header == NULL) {
        return -1;
    }

    uint16_t length;
    uint16_t vlan;

    memcpy(&length, buffer, 2);

    memcpy(header->src, buffer + 2, 6);

    memcpy(header->dst, buffer + 8, 6);

    memcpy(&vlan, buffer + 14, 2);

    header->length = ntohs(length);

    header->vlan = ntohs(vlan);

    header->priority = buffer[16];

    if (header->length > MAX_PAYLOAD_SIZE) {
        return -1;
    }

    return FRAME_HEADER_SIZE;
}