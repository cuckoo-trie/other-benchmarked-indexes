#include "Tree.h"
#include "artolc_for_redis.h"

#define RANGE_END_KEY_SIZE 128

void load_key(TID tid, Key &key) {
	redis_string_kv_with_len* kv = (redis_string_kv_with_len*) tid;
	key.data = (uint8_t*) (kv->key);
	key.len = kv->key_size;
}


class ArtolcAndThreadInfo {
	public:
	ART_OLC::Tree tree;
	ThreadInfo thread_info;
	Key max_key;
	ArtolcAndThreadInfo() : tree(load_key), thread_info(tree.getThreadInfo()) {
		uint8_t* max_key_buf = new uint8_t[RANGE_END_KEY_SIZE];

		// Initialize max_key to a string of 0xFF's with a NULL terminator for use in
		// range queries
		memset(max_key_buf, 0xFF, RANGE_END_KEY_SIZE - 1);
		max_key_buf[RANGE_END_KEY_SIZE - 1] = 0;
		max_key.set((const char*) max_key_buf, RANGE_END_KEY_SIZE);
		delete[] max_key_buf;
	}
};

void* artolc_create() {
	return new ArtolcAndThreadInfo();
}

void artolc_insert(void* tree, redis_string_kv_with_len* kv) {
	Key key;
	ArtolcAndThreadInfo* artolc = (ArtolcAndThreadInfo*) tree;

	load_key((TID)kv, key);
	artolc->tree.insert(key, (TID)kv, artolc->thread_info);
}

redis_string_kv_with_len* artolc_lookup(void* tree, uint8_t* key, uint32_t key_len) {
	Key artolc_key;
	ArtolcAndThreadInfo* artolc = (ArtolcAndThreadInfo*) tree;

	artolc_key.data = key;
	artolc_key.len = key_len;
	TID result = artolc->tree.lookup(artolc_key, artolc->thread_info);
	if (result == 0)
		return NULL;
	return (redis_string_kv_with_len*) result;
}

uint64_t artolc_range_from(void* tree, uint8_t* start_key, uint32_t start_key_len,
						   uint64_t max_results, redis_string_kv_with_len** results) {
	Key artolc_start_key;
	Key dummy;
	std::size_t num_results;
	ArtolcAndThreadInfo* artolc = (ArtolcAndThreadInfo*) tree;

	artolc_start_key.data = start_key;
	artolc_start_key.len = start_key_len;
	artolc->tree.lookupRange(artolc_start_key, artolc->max_key, dummy, (TID*) results,
							 max_results, num_results, artolc->thread_info);

	// If <len> is large, the destructor for Key will assume that it owns the buffer used
	// for the key data and will attempt to free it. We don't own <start_key>, so make sure
	// it doesn't get freed.
	artolc_start_key.len = 0;

	return num_results;
}

void artolc_free(void* tree) {
	ArtolcAndThreadInfo* artolc = (ArtolcAndThreadInfo*) tree;
	delete artolc;
}