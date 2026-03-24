# Makefile
CC = gcc
CFLAGS = -Wall -O2 -g -I./src -DENABLE_API
LDFLAGS = -lcurl -ljansson -lmicrohttpd -lpthread -lm -lsodium -lzmq

SRCS = $(wildcard src/*.c)
OBJS = $(SRCS:.c=.o)
TARGET = solo_gateway

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

src/%.o: src/%.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f src/*.o $(TARGET)
