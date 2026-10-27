CC=gcc
CFLAGS=-Wall -Wextra -pthread

producer_consumer: producer_consumer.c
	$(CC) $(CFLAGS) producer_consumer.c -o producer_consumer

run: producer_consumer
	./producer_consumer

clean:
	rm -f producer_consumer
