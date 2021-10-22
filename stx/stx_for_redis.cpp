#include <stddef.h>
#include <tlx/container/btree.hpp>
#include "stx_for_redis.h"

// A function to extract the key from a key+value object. Required by the STX BTree.
// We use stx_kv_t* for both keys and values, so this is the identity function.
struct key_of_value {
	static uint8_t* const& get(uint8_t* const& value) {
		return value;
	}
};

struct kv_compare {
	bool operator()(const uint8_t* const& lhs, const uint8_t* const& rhs) const {
		if (strcmp((const char*) lhs, (const char*) rhs) < 0)
			return true;

		return false;
	}
};

// Hack: the key and value types are the same - a pointer to the beginning of the <key>
// field in a stx_kv_t. I couldn't find another alternative that would allow 8-byte key
// and value types (using larger types makes the btree larger than neccessary) without
// requiring memory allocation & copying to convert the Redis keys to the key_type.
typedef tlx::BTree<uint8_t*, uint8_t*, struct key_of_value, struct kv_compare> kv_btree;

void* stx_create() {
	kv_btree* result = new kv_btree;
	return result;
}

int stx_insert(void* tree, stx_kv_t* new_kv) {
	kv_btree* btree = (kv_btree*) tree;

	auto result = btree->insert((uint8_t*) &(new_kv->key));
	if (result.second)
		return 1;

	return 0;
}

stx_kv_t* stx_lookup(void* tree, uint8_t* key) {
	kv_btree* btree = (kv_btree*) tree;

	kv_btree::iterator result = btree->find(key);
	if (result == btree->end())
		return NULL;

	uint8_t* key_ptr = *result;
	stx_kv_t* kv = (stx_kv_t*) (key_ptr - offsetof(stx_kv_t, key));
	return kv;
}

void* stx_iter_create() {
	kv_btree::iterator* result = new kv_btree::iterator;
	return result;
}

void stx_iter_goto(void* tree, void* iter, uint8_t* key) {
	kv_btree* btree = (kv_btree*) tree;
	kv_btree::iterator* iterator = (kv_btree::iterator*) iter;
	*iterator = btree->lower_bound(key);
}

stx_kv_t* stx_iter_next(void* tree, void* iter) {
	kv_btree* btree = (kv_btree*) tree;
	kv_btree::iterator* iterator = (kv_btree::iterator*) iter;
	if (*iterator == btree->end())
		return NULL;

	uint8_t* key_ptr = *(*iterator);
	stx_kv_t* kv = (stx_kv_t*) (key_ptr - offsetof(stx_kv_t, key));
	(*iterator)++;  // Move the iterator to the next element

	return kv;
}

void stx_iter_free(void* iter) {
	kv_btree::iterator* iterator = (kv_btree::iterator*) iter;
	delete iterator;
}

void stx_free(void* tree) {
	kv_btree* btree = (kv_btree*) tree;
	delete btree;
}