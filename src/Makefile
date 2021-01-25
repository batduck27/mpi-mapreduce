CC=mpicc

all: wordfreq

wordfreq: wordfreq.o mapreduce.o keyvalue.o hash.o
	$(CC) $^ -o $@

mapreduce.o: mapreduce.c
	$(CC) -c $< -o $@

keyvalue.o: keyvalue.c
	$(CC) -c $< -o $@

wordfreq.o: wordfreq.c
	$(CC) -c $< -o $@

hash.o: hash.c
	$(CC) -c $< -o $@

clean:
	rm -rf *.o wordfreq