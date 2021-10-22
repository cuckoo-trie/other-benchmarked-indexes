#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#include <stdint.h>

typedef struct {
	double value;
	char key[];
} redis_string_kv;

EXTERNC void* hot_create();
EXTERNC int hot_upsert(void* trie, redis_string_kv* kv);
EXTERNC const redis_string_kv* hot_lookup(void* trie, char* key);
EXTERNC void* hot_iter_create(void* trie, char* key);
EXTERNC const redis_string_kv* hot_iter_next(void* iter);
EXTERNC void hot_iter_free(void* iter);
EXTERNC void hot_free(void* trie);