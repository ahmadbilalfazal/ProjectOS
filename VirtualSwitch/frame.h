#ifndef FRAME_H
#define FRAME_H

#include <stdint.h>
#include <stddef.h>

#define FRAME_HEADER_SIZE 17
#define MAX_PAYLOAD_SIZE 1500

typedef struct {
    uint16_t length;
    uint8_t src[6];
    uint8_t dst[6];
    uint16_t vlan;
    uint8_t priority;
} frame_header_t;

typedef struct __attribute__((packed)) {
    uint16_t length;
    uint8_t src[6];
    uint8_t dst[6];
    uint16_t vlan;
    uint8_t priority;
} frame_wire_header_t;

_Static_assert(sizeof(frame_wire_header_t) == 17, "Frame header must be exactly 17 bytes");

int frame_serialize(const frame_header_t *header, uint8_t *buffer);

int frame_deserialize(const uint8_t *buffer, frame_header_t *header);

#endif