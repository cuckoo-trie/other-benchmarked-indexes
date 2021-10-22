#ifdef __cplusplus
#define EXTERNC extern "C"
#else
#define EXTERNC
#endif

#include <stdint.h>

typedef struct {
	double value;
	uint32_t key_size;
	char key[];
} redis_string_kv_with_len;

EXTERNC void* artolc_create();
EXTERNC void artolc_insert(void* tree, redis_string_kv_with_len* kv);
EXTERNC redis_string_kv_with_len* artolc_lookup(void* tree, uint8_t* key, uint32_t key_len);
EXTERNC uint64_t artolc_range_from(void* tree, uint8_t* start_key, uint32_t start_key_len,
								   uint64_t max_results, redis_string_kv_with_len** results);
EXTERNC void artolc_free(void* tree);