#include "mapreduce.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <pthread.h>


// Implement of mapreduce with multi-thread
// Name: Jiun-Ting Chen
// Date: November 2019

#define HASHTABLE_SIZE 65536

typedef struct _value {
	char* value;
	struct _value* next;
} Value;

typedef struct _entry {
	char* key;
	struct _value* value_n;
	struct _entry* next;
} Entry;

typedef struct _hashtable {
	int size;
	struct _entry** table;
	pthread_mutex_t* locks;
} Hashtable;

// create hashtable
Hashtable* ht_create(int size) {
	Hashtable* hashtable = malloc(sizeof(Hashtable));
	hashtable->table = malloc(sizeof(Entry*)*size);
	hashtable->locks = malloc(sizeof(pthread_mutex_t)*size);

	for (int i = 0; i < size; i++) {
		hashtable->table[i] = NULL;
		pthread_mutex_init(&(hashtable->locks[i]), NULL);
	}

	hashtable->size = size;
	return hashtable;
}

// free memory
void* ht_free(Hashtable* hashtable) {
	for (int i = 0; i < hashtable->size; i++) {
		while (hashtable->table[i] != NULL) {
			if (hashtable->table[i]->key != NULL) {
				free(hashtable->table[i]->key);
			}
			while (hashtable->table[i]->value_n != NULL) {
				Value* v_temp = hashtable->table[i]->value_n->next;
				free(hashtable->table[i]->value_n->value);
				free(hashtable->table[i]->value_n);
				hashtable->table[i]->value_n = v_temp;
			}
			Entry* e_temp = hashtable->table[i]->next;
			free(hashtable->table[i]);
			hashtable->table[i] = e_temp;
		}
	}
	free(hashtable->locks);
	free(hashtable->table);
	free(hashtable);
	return NULL;
}

// hash function
int ht_hash(Hashtable* hashtable, char* key) {
	unsigned long hash = 5381;
	int c;
	while ((c = *key++) != '\0')
		hash = hash * 33 + c;
	return hash % hashtable->size;
}

// create a new entry
Entry* ht_newEntry(char* key, char* value) {
	Value* newValue = malloc(sizeof(Value));
	newValue->value = strdup(value);
	newValue->next = NULL;

	Entry* newEntry = malloc(sizeof(Entry));
	newEntry->key = strdup(key);
	newEntry->value_n = newValue;
	newEntry->next = NULL;

	return newEntry;
}

// Insert a key-value into a hash table
void ht_insert(Hashtable* hashtable, char* key, char* value) {
	int bin = 0;
	Entry* newEntry = NULL;
	Entry* next = NULL;
	Entry* last = NULL;

	bin = ht_hash(hashtable, key);
	pthread_mutex_t* lock = hashtable->locks + bin;
	pthread_mutex_lock(lock);
	next = hashtable->table[bin];

	while (next != NULL && next->key != NULL && strcmp(key, next->key) > 0) {
		last = next;
		next = next->next;
	}

	// Key already exist: append value node
	if (next != NULL && next->key != NULL && strcmp(key, next->key) == 0) {
		Value* newValue = malloc(sizeof(Value));
		newValue->value = strdup(value);
		newValue->next = next->value_n;
		next->value_n = newValue;
	// Key not exist: Add a new entry
	} else {
		newEntry = ht_newEntry(key, value);

		// at the start of the linked list in this bin.
		if (next == hashtable->table[bin]) {
			newEntry->next = next;
			hashtable->table[bin] = newEntry;
		// at the end of the linked list in this bin.
		} else if (next == NULL) {
			last->next = newEntry;
		// in the middle of the list. 
		} else {
			newEntry->next = next;
			last->next = newEntry;
		}
	}
	pthread_mutex_unlock(lock);
}

// compare the keys
int compare_key(const void* s1, const void* s2){
	return strcmp((*(Entry**)s1)->key, (*(Entry**)s2)->key);
}

Partitioner partitioner;
int partitions;
Hashtable* hashtable[1024];
Hashtable* hashtable_temp[1024];
Mapper mapThread;
Reducer reduceThread;

// map wrapper
void* Map_wrapper(void* fileName) {
	mapThread((char*)fileName);
	return NULL;
}

// reduce wrapper
void* Reduce_wrapper(void* partition) {
	int j = *(int*)partition;
	Entry* expand_table[HASHTABLE_SIZE*10];
	int index = 0;
	for (int i = 0; i < HASHTABLE_SIZE; i++) {
		while (hashtable_temp[j]->table[i] != NULL && hashtable_temp[j]->table[i]->key != NULL) {
			expand_table[index] = hashtable_temp[j]->table[i];
			Value* value = hashtable_temp[j]->table[i]->value_n;
			while (value != NULL) {
				Value* value_temp = value;
				value = value->next;
				free(value_temp->value);
				free(value_temp);
			}
			free(value);
			hashtable_temp[j]->table[i] = hashtable_temp[j]->table[i]->next;
			index++;
		}
	}
	qsort(expand_table, index, sizeof(expand_table[0]), compare_key);

	for (int i = 0; i < index; i++) {
		reduceThread(expand_table[i]->key, get_next, j);
		free(expand_table[i]->key);
		free(expand_table[i]);
	}
	return NULL;
}

void MR_Emit(char* key, char* value) {
	unsigned long numPart = partitioner(key, partitions);
	ht_insert(hashtable[numPart], key, value);
	ht_insert(hashtable_temp[numPart], key, value);
}

// get the next node
char* get_next(char* key, int partition_number){
	unsigned long numPart = partitioner(key, partitions);
	int bin = ht_hash(hashtable[numPart], key);

	while (hashtable[numPart]->table[bin] != NULL) {
		if (strcmp(hashtable[numPart]->table[bin]->key, key) == 0) {
			if (hashtable[numPart]->table[bin]->value_n == NULL) {
				free(hashtable[numPart]->table[bin]->value_n);
				return NULL;
			}
			char* value = strdup(hashtable[numPart]->table[bin]->value_n->value);
			Value* value_temp = hashtable[numPart]->table[bin]->value_n;
			hashtable[numPart]->table[bin]->value_n = hashtable[numPart]->table[bin]->value_n->next;
			free(value_temp->value);
			free(value_temp);
			return value;
		}
		Entry* entry_temp = hashtable[numPart]->table[bin];
		hashtable[numPart]->table[bin] = hashtable[numPart]->table[bin]->next;
		free(entry_temp->key);
		free(entry_temp);
	}
	return NULL;
}

// default hash partition
unsigned long MR_DefaultHashPartition(char* key, int num_partitions) {
	unsigned long hash = 5381;
	int c;
	while ((c = *key++) != '\0')
		hash = hash * 33 + c;
	return hash % num_partitions;
}

// sorted partition
unsigned long MR_SortedPartition(char* key, int num_partitions) {
	unsigned int key_value = strtoul(key, NULL, 10);
	int num_bits = 0;

	while (num_partitions) {
		num_partitions /= 2;
		num_bits++;
	}

	int bit = 0;
	int part = 0;
	int p = 1;
	for (int k = 32 - (num_bits - 1); k < 32; k++) {
		bit = (key_value & (1 << k)) >> k;
		part += (bit * p);
		p *= 2;
	}
	return part;
}

// main implementation to create, and join
void MR_Run(int argc, char* argv[],
	Mapper map, int num_mappers,
	Reducer reduce, int num_reducers,
	Partitioner partition, int num_partitions) {
	partitioner = partition;
	partitions = num_partitions;
	mapThread = map;
	reduceThread = reduce;

	for (int j = 0; j < num_partitions; j++) {
		hashtable[j] = ht_create(HASHTABLE_SIZE);
		hashtable_temp[j] = ht_create(HASHTABLE_SIZE);
	}

	int numfiles = argc - 1;
	pthread_t mapThread[num_mappers];
	for (int i = 0; i < numfiles; i++) {
		pthread_create(&mapThread[i % num_mappers], NULL, Map_wrapper, argv[i + 1]);
		if ((i % num_mappers) == (num_mappers - 1)) {
			for (int j = 0; j < num_mappers; j++) {
				pthread_join(mapThread[j], NULL);
			}
		}
	}

	for (int i = 0; i < numfiles % num_mappers; i++) {
		pthread_join(mapThread[i], NULL);
	}

	pthread_t reduceThread[num_reducers];
	int* arg = NULL;
	for (int i = 0; i < num_partitions; i++) {
		arg = malloc(sizeof(*arg));
		*arg = i;
		pthread_create(&reduceThread[i % num_reducers], NULL, Reduce_wrapper, (void*)arg);
		if ((i % num_reducers) == (num_reducers - 1)) {
			for (int j = 0; j < num_reducers; j++) {
				pthread_join(reduceThread[j], NULL);
			}
		}
	}

	for (int i = 0; i < num_partitions % num_reducers; i++) {
		pthread_join(reduceThread[i], NULL);
	}

	for (int i = 0; i < num_partitions; ++i) {
		ht_free(hashtable[i]);
		ht_free(hashtable_temp[i]);
	}
	free(arg);
	arg = NULL;
}
