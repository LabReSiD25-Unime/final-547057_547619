#ifndef COMMON_H
#define COMMON_H

#define NUM_MAPPERS 4

/* Path per le FIFO */
#define MASTER_TO_MAPPER_FIFO "/tmp/master_to_mapper_%d"
#define MAPPER_TO_REDUCER_FIFO "/tmp/mapper_%d_to_reducer"

#define MAX_WORD_LEN 64
#define MAX_LINE_LEN 1024

/* Struttura per i dati del chunk */
typedef struct {
    long offset;
    long size;
} ChunkData;

#endif
