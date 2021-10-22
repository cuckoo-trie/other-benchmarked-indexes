#include "hot_for_redis.h"
#include <hot/singlethreaded/HOTSingleThreaded.hpp>

template<typename KVType>
struct KVKeyExtractor {
	inline const char* operator()(const KVType kv) {
		return (const char*) &(kv->key);
	}
};

typedef hot::singlethreaded::HOTSingleThreaded<const redis_string_kv*, KVKeyExtractor> redis_hot_t;

void* hot_create() {
	redis_hot_t* result = new redis_hot_t;
	return result;
}

// Returns 1 if a new key was inserted
int hot_upsert(void* trie, redis_string_kv* kv) {
	redis_hot_t* hot_trie = (redis_hot_t*) trie;

	auto prev = hot_trie->upsert(kv);

	// If <prev> is invalid this means that a new key was inserted
	if (!prev.mIsValid)
		return 1;

	// Otherwise, an existing key had its value changed
	return 0;
}

const redis_string_kv* hot_lookup(void* trie, char* key) {
	redis_hot_t* hot_trie = (redis_hot_t*) trie;

	auto value = hot_trie->lookup(key);

	if (!value.mIsValid)
		return NULL;       // Not found

	return value.mValue;
}

void* hot_iter_create(void* trie, char* start_key) {
	redis_hot_t* hot_trie = (redis_hot_t*) trie;

	redis_hot_t::const_iterator iter = hot_trie->lower_bound(start_key);
	redis_hot_t::const_iterator* result = new redis_hot_t::const_iterator(iter);

	return result;
}

const redis_string_kv* hot_iter_next(void* iter) {
	redis_hot_t::const_iterator* hot_iter = (redis_hot_t::const_iterator*) iter;

	const redis_string_kv* result = **hot_iter;
	if (result != NULL)
		++(*hot_iter);

	return result;
}

void hot_iter_free(void* iter) {
	redis_hot_t::const_iterator* hot_iter = (redis_hot_t::const_iterator*) iter;
	delete hot_iter;
}

void hot_free(void* trie) {
	redis_hot_t* hot_trie = (redis_hot_t*) trie;
	delete hot_trie;
}