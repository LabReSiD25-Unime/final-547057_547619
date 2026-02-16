#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <string.h>

#include "common.h"

/* Pulizia FIFO */
void cleanup_fifos() {
    char fifo_path[256];

    for (int i = 0; i < NUM_MAPPERS; i++) {
        snprintf(fifo_path, sizeof(fifo_path), MASTER_TO_MAPPER_FIFO, i);
        unlink(fifo_path);

        snprintf(fifo_path, sizeof(fifo_path), MAPPER_TO_REDUCER_FIFO, i);
        unlink(fifo_path);
    }
}

int main() {
    struct stat st;
    long file_size, chunk_size;
    char fifo_path[256];

    cleanup_fifos();

    /* Dimensione file */
    if (stat("data/lotr.txt", &st) < 0) {
        perror("stat");
        exit(EXIT_FAILURE);
    }

    file_size = st.st_size;
    chunk_size = file_size / NUM_MAPPERS;

    printf("[Master] File size: %ld bytes, chunk size: %ld\n\n", file_size, chunk_size);

    /* Crea FIFO Master -> Mapper */
    for (int i = 0; i < NUM_MAPPERS; i++) {
        snprintf(fifo_path, sizeof(fifo_path), MASTER_TO_MAPPER_FIFO, i);
        if (mkfifo(fifo_path, 0666) < 0) {
            perror("mkfifo master_to_mapper");
            exit(EXIT_FAILURE);
        }
    }

    /* Crea FIFO Mapper -> Reducer */
    for (int i = 0; i < NUM_MAPPERS; i++) {
        snprintf(fifo_path, sizeof(fifo_path), MAPPER_TO_REDUCER_FIFO, i);
        if (mkfifo(fifo_path, 0666) < 0) {
            perror("mkfifo mapper_to_reducer");
            exit(EXIT_FAILURE);
        }
    }

    /* Avvio Reducer */
    pid_t pid = fork();
    if (pid == 0) {
        execl("./reducer", "reducer", NULL);
        perror("execl reducer");
        exit(EXIT_FAILURE);
    } else {
        printf("Avvio reducer...\n\n");
        fflush(stdout);
    }

    sleep(1);

    /* Avvio Mapper */
    for (int i = 0; i < NUM_MAPPERS; i++) {
        pid = fork();
        if (pid == 0) {
            char id[16];
            snprintf(id, sizeof(id), "%d", i);
            execl("./mapper", "mapper", id, NULL);
            perror("execl mapper");
            exit(EXIT_FAILURE);
        } else {
            printf("Avvio mapper %d...\n", i);
            fflush(stdout);
        }
    }

    printf("\n");
    sleep(1);

    /* Invio chunk ai mapper */
    for (int i = 0; i < NUM_MAPPERS; i++) {
        snprintf(fifo_path, sizeof(fifo_path), MASTER_TO_MAPPER_FIFO, i);

        int fifo_fd = open(fifo_path, O_WRONLY);
        if (fifo_fd < 0) {
            perror("open master_to_mapper FIFO");
            continue;
        }

        ChunkData chunk;
        chunk.offset = i * chunk_size;
        chunk.size = (i == NUM_MAPPERS - 1)
                        ? (file_size - chunk.offset)
                        : chunk_size;

        ssize_t bytes_written = write(fifo_fd, &chunk, sizeof(ChunkData));
        if (bytes_written != sizeof(ChunkData)) {
            perror("write chunk to FIFO");
        }

        close(fifo_fd);

        if (i == 0) printf("\n"); 
        printf("[Master] Inviato chunk %d: offset= %ld, size= %ld\n", i, chunk.offset, chunk.size);
    }

    printf("\n");

    /* Attesa fine processi */
    for (int i = 0; i < NUM_MAPPERS + 1; i++) {
        wait(NULL);
    }

    cleanup_fifos();

    printf("[Master] Esecuzione completata\n");
    return 0;
}
