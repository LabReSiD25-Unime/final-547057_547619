CC=gcc
CFLAGS=-Wall -O2 -pthread
LDFLAGS=-pthread

SRC=src
BIN=.

all: master mapper reducer

master: $(SRC)/master.c
	$(CC) $(CFLAGS) $< -o $(BIN)/master

mapper: $(SRC)/mapper.c
	$(CC) $(CFLAGS) $< -o $(BIN)/mapper

reducer: $(SRC)/reducer.c
	$(CC) $(CFLAGS) $< -o $(BIN)/reducer $(LDFLAGS)

clean:
	rm -f $(BIN)/master $(BIN)/mapper $(BIN)/reducer
	rm -rf /tmp/mapreduce_fifos

.PHONY: all clean
