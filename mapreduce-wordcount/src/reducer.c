#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#include "common.h"

/* Linked list globale */
typedef struct WordCount {
    char word[MAX_WORD_LEN];
    int count;
    struct WordCount *next;
} WordCount;

WordCount *global_head = NULL;
pthread_mutex_t list_mutex = PTHREAD_MUTEX_INITIALIZER;
int total_words = 0;

typedef struct {
    int mapper_id;
} ThreadArgs;

/* Cerca parola nella lista globale */
WordCount *find_word_unsafe(const char *word) {
    WordCount *curr = global_head;
    while (curr) {
        if (strcmp(curr->word, word) == 0)
            return curr;
        curr = curr->next;
    }
    return NULL;
}

/* Aggiunge o aggiorna parola globalmente */
void add_word_safe(const char *word, int count) {
    pthread_mutex_lock(&list_mutex);
    
    WordCount *wc = find_word_unsafe(word);
    if (wc) {
        wc->count += count;
    } else {
        wc = malloc(sizeof(WordCount));
        strncpy(wc->word, word, MAX_WORD_LEN);
        wc->word[MAX_WORD_LEN - 1] = '\0';
        wc->count = count;
        wc->next = global_head;
        global_head = wc;
    }

    total_words += count;
    pthread_mutex_unlock(&list_mutex);
}

/* Thread per processare i dati da un Mapper */
void *process_mapper(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    int mapper_id = args->mapper_id;
    char fifo_path[256];

    snprintf(fifo_path, sizeof(fifo_path), MAPPER_TO_REDUCER_FIFO, mapper_id);

    int fifo_fd = open(fifo_path, O_RDONLY);
    if (fifo_fd < 0) {
        perror("open mapper_to_reducer FIFO");
        free(args);
        pthread_exit(NULL);
    }

    FILE *stream = fdopen(fifo_fd, "r");
    if (!stream) {
        perror("fdopen");
        close(fifo_fd);
        free(args);
        pthread_exit(NULL);
    }

    char word[MAX_WORD_LEN];
    int count;
    while (fscanf(stream, "%s %d", word, &count) == 2) {
        add_word_safe(word, count);
    }

    fclose(stream);
    free(args);
    pthread_exit(NULL);
}

/* Stampa risultati e salva report */
void print_sorted_results() {
    int unique_words = 0;
    WordCount *curr = global_head;

    while (curr) {
        unique_words++;
        curr = curr->next;
    }

    printf("Numero parole contate in totale: %d parole\n", total_words);
    printf("Numero parole uniche contate: %d parole\n\n", unique_words);
    printf("Report counter in report.txt\n\n");

    /* Scrive report */
    FILE *fp = fopen("report.txt", "w");
    if (!fp) {
        perror("fopen report.txt");
        return;
    }

    curr = global_head;
    while (curr) {
        fprintf(fp, "%s = %d\n", curr->word, curr->count);
        curr = curr->next;
    }
    fclose(fp);
}

int main() {
    pthread_t threads[NUM_MAPPERS];

    /* Crea thread per ogni mapper */
    for (int i = 0; i < NUM_MAPPERS; i++) {
        ThreadArgs *args = malloc(sizeof(ThreadArgs));
        args->mapper_id = i;
        if (pthread_create(&threads[i], NULL, process_mapper, args) != 0) {
            perror("pthread_create");
            free(args);
            exit(EXIT_FAILURE);
        }
    }

    for (int i = 0; i < NUM_MAPPERS; i++) pthread_join(threads[i], NULL);

    print_sorted_results();

    /* free memoria */
    WordCount *curr = global_head;
    while (curr) {
        WordCount *next = curr->next;
        free(curr);
        curr = next;
    }

    pthread_mutex_destroy(&list_mutex);
    return 0;
}

