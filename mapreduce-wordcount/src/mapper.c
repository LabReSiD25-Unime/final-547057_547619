#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>

#include "common.h"

/* Linked list locale */
typedef struct WordCount {
    char word[MAX_WORD_LEN];
    int count;
    struct WordCount *next;
} WordCount;

WordCount *head = NULL;

/* Cerca parola */
WordCount *find_word(const char *word) {
    WordCount *curr = head;
    while (curr) {
        if (strcmp(curr->word, word) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

/* Aggiunge/incrementa parola */
void add_word(const char *word) {
    WordCount *wc = find_word(word);
    if (wc) {
        wc->count++;
    } else {
        wc = malloc(sizeof(WordCount));
        strncpy(wc->word, word, MAX_WORD_LEN);
        wc->word[MAX_WORD_LEN - 1] = '\0';
        wc->count = 1;
        wc->next = head;
        head = wc;
    }
}

/* Normalizza buffer: minuscolo, non alfanumerico → spazio */
void normalize_buffer(char *buffer) {
    for (int i = 0; buffer[i]; i++) {
        if (!isalnum((unsigned char)buffer[i]))
            buffer[i] = ' ';
        else
            buffer[i] = tolower(buffer[i]);
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Uso: %s <mapper_id>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int mapper_id = atoi(argv[1]);
    char fifo_path[256];

    /* Ricezione chunk dal Master */
    snprintf(fifo_path, sizeof(fifo_path), MASTER_TO_MAPPER_FIFO, mapper_id);

    int fifo_fd = open(fifo_path, O_RDONLY);
    if (fifo_fd < 0) {
        perror("open master_to_mapper FIFO");
        exit(EXIT_FAILURE);
    }

    ChunkData chunk;
    if (read(fifo_fd, &chunk, sizeof(ChunkData)) != sizeof(ChunkData)) {
        perror("read chunk");
        close(fifo_fd);
        exit(EXIT_FAILURE);
    }
    close(fifo_fd);

    /* Apertura file */
    FILE *fp = fopen("data/lotr.txt", "r");
    if (!fp) {
        perror("fopen");
        exit(EXIT_FAILURE);
    }

    long chunk_start = chunk.offset;
    long chunk_end   = chunk.offset + chunk.size;

    fseek(fp, chunk_start, SEEK_SET);

    if (chunk_start != 0) {
        int c;
        while ((c = fgetc(fp)) != EOF) {
            if (!isalnum(c))
                break;
        }
    }

    char buffer[MAX_LINE_LEN];

    /* Lettura chunk */
    while (ftell(fp) < chunk_end && fgets(buffer, sizeof(buffer), fp)) {
        normalize_buffer(buffer);

        char *token = strtok(buffer, " ");
        while (token) {
            if (strlen(token) > 0)
                add_word(token);
            token = strtok(NULL, " ");
        }
    }

    /* Completa ultima parola spezzata */
    if (ftell(fp) >= chunk_end) {
        int c;
        char extra[MAX_WORD_LEN];
        int i = 0;

        while ((c = fgetc(fp)) != EOF && isalnum(c)) {
            if (i < MAX_WORD_LEN - 1)
                extra[i++] = tolower(c);
        }
        extra[i] = '\0';

        if (i > 0)
            add_word(extra);
    }

    fclose(fp);

    /* Invio al Reducer */
    snprintf(fifo_path, sizeof(fifo_path), MAPPER_TO_REDUCER_FIFO, mapper_id);
    fifo_fd = open(fifo_path, O_WRONLY);
    if (fifo_fd < 0) {
        perror("open mapper_to_reducer FIFO");
        exit(EXIT_FAILURE);
    }

    FILE *stream = fdopen(fifo_fd, "w");
    if (!stream) {
        perror("fdopen");
        close(fifo_fd);
        exit(EXIT_FAILURE);
    }

    WordCount *curr = head;
    while (curr) {
        fprintf(stream, "%s %d\n", curr->word, curr->count);
        curr = curr->next;
    }

    fclose(stream);

    /* Free memoria */
    curr = head;
    while (curr) {
        WordCount *next = curr->next;
        free(curr);
        curr = next;
    }

    return 0;
}
