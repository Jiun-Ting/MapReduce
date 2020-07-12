CC=gcc
CFLAGS=-lpthread

wordcount: mapreduce.c wordcount.c
		$(CC) -o wordcount $(CFLAGS) mapreduce.c wordcount.c

clean:
		$(RM) wordcount

