#include <stdio.h>
#include <time.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

#include "../util.h"
#include "wormhole/lib.h"
#include "wormhole/wh.h"

#define MILLION 1000000
#define DEFAULT_VALUE_SIZE 8
#define DEFAULT_NUM_THREADS 4

#define PID_NO_PROFILER 0

// A wormhole memory manager that uses the given kv structs instead of copying them.
// This makes the API of wormhole more similar to the Cuckoo Trie and HOT, faster,
// and less dependent upon malloc performance
const struct kvmap_mm mm_dont_copy_kv = {
  .in = kvmap_mm_in_noop,
  .out = kvmap_mm_out_noop,
  .free = kvmap_mm_free_noop,
  .priv = NULL,
};

pid_t profiler_pid = PID_NO_PROFILER;

// Notify the profiler that the critical section starts, so it should start collecting statistics
void notify_critical_section_start() {
	if (profiler_pid != PID_NO_PROFILER)
		kill(profiler_pid, SIGUSR1);
}

void notify_critical_section_end() {
	if (profiler_pid != PID_NO_PROFILER)
		kill(profiler_pid, SIGUSR1);
}

// Create a contiguous buffer of <struct kv>-s representing the keys in the
// dataset together with dummy values. Return an array of pointers to these
// structs.
struct kv** read_wormhole_kvs(dataset_t* dataset, int value_size) {
	uint64_t i;
	ct_key key;
	dynamic_buffer_t kvs_buf;
	uint8_t* key_buf = malloc(MAX_KEY_SIZE);
	uintptr_t* kv_ptrs = malloc(sizeof(struct kv*) * dataset->num_keys);
	key.bytes = key_buf;

	dynamic_buffer_init(&kvs_buf);
	for (i = 0;i < dataset->num_keys;i++) {
		dataset->read_key(dataset, &key);

		uint64_t pos = dynamic_buffer_extend(&kvs_buf, sizeof(struct kv) + key.size + value_size);
		struct kv* kv = (struct kv*) (kvs_buf.ptr + pos);

		// Don't use kv_refill_* to set the kv fields as that also computes the hash.
		// Hash calculation should be done when inserting, s.t. it is counted as insertion
		// time.
		kv->klen = key.size;
		kv->vlen = value_size;
		memcpy(kv->kv, key.bytes, key.size);
		memset(kv->kv + kv->klen, 0xAB, kv->vlen);
		kv_ptrs[i] = pos;
	}

	for (i = 0;i < dataset->num_keys;i++)
		kv_ptrs[i] += (uintptr_t)(kvs_buf.ptr);

	return (struct kv**) kv_ptrs;
}

void insert_kvs(struct wormhole* const wormhole, struct kv** kvs, int num_kvs) {
	uint64_t i;

	for (i = 0;i < num_kvs;i++) {
		kv_update_hash(kvs[i]);
		whunsafe_set(wormhole, kvs[i]);
	}
}

void insert_kvs_mt(struct wormref* const ref, struct kv** kvs, int num_kvs) {
	uint64_t i;

	for (i = 0;i < num_kvs;i++) {
		kv_update_hash(kvs[i]);
		wormhole_set(ref, kvs[i]);
	}
}

void load_dataset(char* dataset_name) {
	struct timespec start_time;
	struct timespec end_time;
	uint64_t i;
	int result;
	dataset_t dataset;
	struct kv** kv_ptrs;
	struct wormhole * const wormhole = whunsafe_create(&mm_dont_copy_kv);

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	kv_ptrs = read_wormhole_kvs(&dataset, 0);

	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0;i < dataset.num_keys;i++) {
		struct kv* kv = kv_ptrs[i];
		kv_update_hash(kv);
		whunsafe_set(wormhole, kv);
		speculation_barrier();
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	float time_took = time_diff(&end_time, &start_time);
	printf("Took %.2fs (%.0fns/key)\n", time_took, time_took / dataset.num_keys * 1.0e9);
	printf("RESULT: ops=%lu ms=%d\n", dataset.num_keys, (int)(time_took * 1000));
}

void mem_usage(char* dataset_name, uint64_t num_keys) {
	uint64_t i;
	uint64_t total_key_size = 0;
	uint64_t total_kv_size = 0;
	uint64_t start_mem_bytes;
	uint64_t end_mem_bytes;
	struct kv** kv_ptrs;
	dataset_t dataset;
	struct wormhole * const wormhole = whunsafe_create(&mm_dont_copy_kv);

	if (num_keys == 0)
		num_keys = DATASET_ALL_KEYS;

	seed_and_print();
	init_dataset(&dataset, dataset_name, num_keys);
	kv_ptrs = read_wormhole_kvs(&dataset, 0);

	for (i = 0;i < dataset.num_keys;i++) {
		struct kv* kv = kv_ptrs[i];
		total_key_size += kv->klen + sizeof(kv->kvlen);
		total_kv_size += kv_size(kv);
	}

	start_mem_bytes = virt_mem_usage();
	insert_kvs(wormhole, kv_ptrs, dataset.num_keys);
	end_mem_bytes = virt_mem_usage();

	uint64_t total_mem = end_mem_bytes - start_mem_bytes + total_kv_size;
	uint64_t index_overhead = total_mem - total_key_size;
	printf("Loaded %lu keys (%.0fMB)\n", dataset.num_keys, ((float)total_key_size) / 1000000.0);
	printf("Total mem:      %.0fMB (%.1fb/key)\n",
		   ((float)total_mem) / 1000000.0, ((float)total_mem)/dataset.num_keys);
	printf("Index overhead: %.0fMB (%.1fb/key)\n",
		   ((float)index_overhead) / 1000000.0, ((float)index_overhead)/dataset.num_keys);
	printf("RESULT: keys=%lu bytes=%lu\n", dataset.num_keys, index_overhead);
}

void pos_lookup(dataset_t* dataset) {
	const uint64_t num_lookups = 10 * MILLION;
	struct timespec start_time;
	struct timespec end_time;
	uint64_t i;
	int result;
	ct_key* keys;
	struct kv** kv_ptrs;
	dynamic_buffer_t workload_data;
	struct wormhole * const wormhole = whunsafe_create(&mm_dont_copy_kv);
	uint64_t* workload_offsets = (uint64_t*) malloc(sizeof(uint64_t) * num_lookups);

	kv_ptrs = read_wormhole_kvs(dataset, 0);

	printf("Loading...\n");
	insert_kvs(wormhole, kv_ptrs, dataset->num_keys);

	printf("Creating workload...\n");
	dynamic_buffer_init(&workload_data);
	for (i = 0;i < num_lookups;i++) {
		struct kv* kv = kv_ptrs[rand_uint64() % dataset->num_keys];
		uint64_t pos = dynamic_buffer_extend(&workload_data, sizeof(struct kref) + kv->klen);
		struct kref* key = (struct kref*) (workload_data.ptr + pos);
		key->len = kv->klen;
		memcpy(&(key[1]), kv->kv, kv->klen);
		workload_offsets[i] = pos;
	}

	for (i = 0;i < num_lookups;i++) {
		struct kref* key = (struct kref*) (workload_data.ptr + workload_offsets[i]);
		key->ptr = (uint8_t*) &(key[1]);
	}

	printf("Performing lookups...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0;i < num_lookups;i++) {
		struct kref* key = (struct kref*) (workload_data.ptr + workload_offsets[i]);

		kref_update_hash32(key);
		result = whunsafe_probe(wormhole, key);
		if (result == 0) {
			printf("Error: Inserted key wasn't found\n");
			return;
		}
		speculation_barrier();
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	notify_critical_section_end();

	float time_took = time_diff(&end_time, &start_time);
	printf("Took %.2fs (%.0fns/key)\n", time_took, time_took / num_lookups * 1.0e9);
	printf("RESULT: ops=%lu ms=%d\n", num_lookups, (int)(time_took * 1000));
}

typedef struct {
	struct wormhole* trie;
	uint8_t* kvs;
	uint64_t num_kvs;
} mt_insert_ctx;

void* mt_insert_thread(void* arg) {
	uint64_t i;
	int result;
	uint8_t* buf_pos;
	mt_insert_ctx* ctx = (mt_insert_ctx*) arg;
	struct wormref* const ref = wormhole_ref(ctx->trie);

	buf_pos = ctx->kvs;
	for (i = 0;i < ctx->num_kvs;i++) {
		struct kv* kv = (struct kv*) buf_pos;
		kv_update_hash(kv);
		result = wormhole_set(ref, kv);
		if (result == 0) {
			printf("Error: wormhole_set error\n");
			return NULL;
		}

		buf_pos += kv_size(kv);
		speculation_barrier();
	}

	wormhole_unref(ref);
	return NULL;
}

void mt_insert(char* dataset_name, int num_threads) {
	int i;
	int result;
	dataset_t dataset;
	struct kv** kv_ptrs;
	struct timespec start_time;
	struct timespec end_time;
	mt_insert_ctx thread_contexts[num_threads];
	struct wormhole* wormhole = wormhole_create(&mm_dont_copy_kv);

	seed_and_print();

	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	kv_ptrs = read_wormhole_kvs(&dataset, DEFAULT_VALUE_SIZE);

	uint64_t workload_start = 0;
	uint64_t workload_end;
	for (i = 0;i < num_threads;i++) {
		mt_insert_ctx* ctx = &(thread_contexts[i]);
		workload_end = dataset.num_keys * (i+1) / num_threads;
		ctx->kvs = (uint8_t*)(kv_ptrs[workload_start]);
		ctx->num_kvs = workload_end - workload_start;
		ctx->trie = wormhole;

		workload_start = workload_end;
	}

	printf("Inserting...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	run_multiple_threads(mt_insert_thread, num_threads, thread_contexts, sizeof(mt_insert_ctx));
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	float time_took = time_diff(&end_time, &start_time);
	report_mt(time_took, dataset.num_keys, num_threads);
}

typedef struct {
	struct wormhole * trie;
	struct kref* krefs;
	uint64_t num_keys;
} mt_lookup_ctx;

void* mt_lookup_thread(void* arg) {
	uint64_t i;
	int result;
	mt_lookup_ctx* ctx = (mt_lookup_ctx*) arg;
	struct wormref* const ref = wormhole_ref(ctx->trie);

	for (i = 0; i < ctx->num_keys; i++) {
		struct kref* kref = &(ctx->krefs[i]);

		kref_update_hash32(kref);
		result = wormhole_probe(ref, kref);
		if (result == 0) {
			printf("Error: Inserted key wasn't found\n");
			return NULL;
		}
		speculation_barrier();
	}
	wormhole_unref(ref);
	return NULL;
}

void mt_pos_lookup(char* dataset_name, int num_threads) {
	const uint64_t lookups_per_thread = 10 * MILLION;
	uint64_t thread, i;
	int result;
	struct timespec start_time;
	struct timespec end_time;
	struct kv** kv_ptrs;
	uint64_t total_lookups;
	struct kref* workload_krefs;
	dynamic_buffer_t workload_keys;
	mt_lookup_ctx thread_contexts[num_threads];

	struct wormhole * trie = wormhole_create(&mm_dont_copy_kv);
	struct wormref * ref = wormhole_ref(trie);

	dataset_t dataset;

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	kv_ptrs = read_wormhole_kvs(&dataset, 0);

	printf("Loading...\n");
	insert_kvs_mt(ref, kv_ptrs, dataset.num_keys);
	wormhole_unref(ref);

	printf("Creating workloads...\n");
	total_lookups = lookups_per_thread * num_threads;
	dynamic_buffer_init(&workload_keys);
	workload_krefs = malloc(sizeof(struct kref) * total_lookups);
	for (i = 0; i < total_lookups; i++) {
		struct kv* kv = kv_ptrs[rand_uint64() % dataset.num_keys];
		struct kref* kref = &(workload_krefs[i]);

		// Put the key in the keys buffer
		uint64_t offset = dynamic_buffer_extend(&workload_keys, kv->klen);
		memcpy(workload_keys.ptr + offset, kv->kv, kv->klen);

		// Create the kref
		kref->ptr = (uint8_t*) offset;
		kref->len = kv->klen;
	}

	for (i = 0; i < total_lookups; i++)
		workload_krefs[i].ptr += (uintptr_t) (workload_keys.ptr);

	for (i = 0; i < num_threads; i++) {
		thread_contexts[i].trie = trie;
		thread_contexts[i].krefs = &(workload_krefs[lookups_per_thread * i]);
		thread_contexts[i].num_keys = lookups_per_thread;
	}

	printf("Performing lookups...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	run_multiple_threads(mt_lookup_thread, num_threads, thread_contexts, sizeof(mt_lookup_ctx));
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	float time_took = time_diff(&end_time, &start_time);
	report_mt(time_took, total_lookups, num_threads);
}

/*
// Load the dataset, then move an iterator over short ranges while reading each key.
void read_ranges(char* dataset_name) {
	const uint64_t num_ranges = MILLION;
	const uint64_t max_range_size = 100;
	struct timespec start_time;
	struct timespec end_time;
	uint64_t i, j;
	int result;
	dataset_t dataset;
	ct_key* keys;
	struct kv* wh_key = malloc(sizeof(struct kv) + MAX_KEY_SIZE);
	struct kv* iteration_result = malloc(sizeof(struct kv) + MAX_KEY_SIZE);
	struct wormhole * const wormhole = whunsafe_create(NULL);
	struct wormhole_iter * iter;

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	keys = read_dataset(&dataset);

	printf("Loading...\n");
	insert_keys(wormhole, keys, dataset.num_keys);

	printf("Iterating...\n");
	iter = wormhole_iter_create_unsafe(wormhole);
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0;i < num_ranges;i++) {
		uint64_t range_size = rand_dword() % max_range_size;
		uint64_t start_key = rand_dword() % dataset.num_keys;

		wh_key->klen = keys[start_key].size;
		wh_key->vlen = 0;
		memcpy(wh_key->kv, keys[start_key].bytes, keys[start_key].size);
		kv_update_hash(wh_key);
		wormhole_iter_seek_unsafe(iter, wh_key);
		for (j = 0;j < range_size;j++)
			wormhole_iter_next_unsafe(iter, iteration_result);
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	printf("Iteration took %.2fs\n", time_diff(&end_time, &start_time));
}
*/

const ycsb_workload_spec YCSB_A_SPEC = {{0.5,  0,    0.5,  0,    0   , 0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_B_SPEC = {{0.95, 0,    0.05, 0,    0   , 0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_C_SPEC = {{1.0,  0,    0,    0,    0   , 0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_D_SPEC = {{0,    0.95, 0,    0.05, 0   , 0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_E_SPEC = {{0,    0,    0,    0.05, 0.95, 0  }, 50 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_F_SPEC = {{0.5,  0,    0,    0,    0,    0.5}, 10 * MILLION, DIST_ZIPF};

int generate_ycsb_workload(dataset_t* dataset, struct kv** kv_ptrs, ycsb_workload* workload,
						   const ycsb_workload_spec* spec, int thread_id, int num_threads) {
	uint64_t i;
	int data_size;
	struct kv* kv;
	struct kref* key;
	uint64_t insert_offset;
	uint64_t num_inserts = 0;
	uint64_t inserts_per_thread;
	uint64_t read_latest_block_size;
	dynamic_buffer_t workload_buf;
	rand_distribution dist;
	rand_distribution backward_dist;

	workload->ops = (ycsb_op*) malloc(sizeof(ycsb_op) * spec->num_ops);
	workload->num_ops = spec->num_ops;

	inserts_per_thread = spec->op_type_probs[YCSB_INSERT] * spec->num_ops;
	workload->initial_num_keys = dataset->num_keys - inserts_per_thread * num_threads;
	insert_offset = workload->initial_num_keys + inserts_per_thread * thread_id;
	read_latest_block_size = spec_read_latest_block_size(spec, num_threads);

	if (spec->distribution == DIST_UNIFORM) {
		rand_uniform_init(&dist, workload->initial_num_keys);
	} else if (spec->distribution == DIST_ZIPF) {
		rand_zipf_init(&dist, workload->initial_num_keys, YCSB_SKEW);
	} else {
		printf("Error: Unknown YCSB distribution\n");
		return 0;
	}

	if (spec->op_type_probs[YCSB_READ_LATEST] > 0.0) {
		// spec->distribution is meaningless for read-latest. Read offsets for read-latest are
		// always Zipf-distributed.
		assert(spec->distribution == DIST_ZIPF);
		rand_zipf_rank_init(&backward_dist, workload->initial_num_keys, YCSB_SKEW);
	}

	if (spec->op_type_probs[YCSB_SCAN] > 0 && spec->num_ops * num_threads < dataset->num_keys / 4) {
		// In Wormhole leaves are unsorted when the insertion is finished, and are only
		// sorted when a scan operation touches them. Therefore, until all leaves are touched,
		// scan operations will be slower as they have to sort leaves.
		printf("Warning: Workload is small, scan operations will appear to be slower\n");
	}

	dynamic_buffer_init(&workload_buf);
	for (i = 0; i < spec->num_ops; i++) {
		ycsb_op* op = &(workload->ops[i]);
		op->type = choose_ycsb_op_type(spec->op_type_probs);

		if (num_inserts == inserts_per_thread && op->type == YCSB_INSERT) {
			// Cannot perform read/update before any keys were inserted
			i--;
			continue;
		}

		switch (op->type) {
			case YCSB_READ:
			case YCSB_SCAN:{
				kv = kv_ptrs[rand_dist(&dist)];
				data_size = sizeof(struct kref) + kv->klen;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				key = (struct kref*) (workload_buf.ptr + op->data_pos);
				key->len = kv->klen;
				// key->ptr is set later
				memcpy(&(key[1]), kv->kv, kv->klen);
			}
			break;

			case YCSB_READ_LATEST:
				// Data for read-latest ops is generated separately
				break;

			case YCSB_RMW:
			case YCSB_UPDATE:{
				kv = kv_ptrs[rand_dist(&dist)];
				data_size = kv_size(kv);
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				struct kv* new_kv = (struct kv*) (workload_buf.ptr + op->data_pos);
				memcpy(new_kv, kv, data_size);
				memset(new_kv->kv + new_kv->klen, 7, new_kv->vlen);
			}
			break;

			case YCSB_INSERT:{
				kv = kv_ptrs[insert_offset + num_inserts];
				num_inserts++;
				data_size = kv_size(kv);
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);
				memcpy(workload_buf.ptr + op->data_pos, kv, data_size);
			}
			break;

			default:
				printf("Error: Unknown YCSB op type %d\n", op->type);
				return 0;
		}
	}

	// Create the read-latest key blocks
	uint64_t block;
	uint64_t thread;
	for (thread = 0; thread < num_threads; thread++) {
		uint8_t** block_offsets = malloc(sizeof(uint64_t) * (num_inserts + 1));
		workload->read_latest_blocks_for_thread[thread] = block_offsets;

		// We have one block for each amount of inserts between 0 and num_inserts, /inclusive/
		for (block = 0; block < num_inserts + 1; block++) {
			for (i = 0; i < read_latest_block_size; i++) {
				uint64_t backwards = rand_dist(&backward_dist);
				if (backwards < block * num_threads) {
					// This read-latest op refers to a key that was inserted during the workload
					backwards /= num_threads;
					kv = kv_ptrs[insert_offset + block - backwards - 1];
				} else {
					// This read-latest op refers to a key that was loaded before the workload started
					backwards -= block * num_threads;
					kv = kv_ptrs[workload->initial_num_keys - backwards - 1];
				}

				data_size = sizeof(struct kref) + kv->klen;
				uint64_t data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				key = (struct kref*) (workload_buf.ptr + data_pos);
				key->len = kv->klen;
				// key->ptr is set later
				memcpy(&(key[1]), kv->kv, kv->klen);

				if (i == 0)
					block_offsets[block] = (uint8_t*) data_pos;
			}

			uint64_t sentinel_pos = dynamic_buffer_extend(&workload_buf, sizeof(struct kref));
			struct kref* sentinel = (struct kref*) (workload_buf.ptr + sentinel_pos);
			sentinel->len = 0xFFFFFFFF;
		}
	}

	workload->data_buf = workload_buf.ptr;

	// Fill the pointers in the read-latest kref structs
	for (thread = 0; thread < num_threads; thread++) {
		for (block = 0; block < num_inserts + 1; block++) {
			workload->read_latest_blocks_for_thread[thread][block] += (uintptr_t) (workload->data_buf);

			uint8_t* block_ptr = workload->read_latest_blocks_for_thread[thread][block];
			for (i = 0; i < read_latest_block_size; i++) {
				key = (struct kref*) block_ptr;
				key->ptr = (uint8_t*) &(key[1]);
				block_ptr += sizeof(struct kref) + key->len;
			}
		}
	}

	// And in read/scan kref structs
	for (i = 0;i < spec->num_ops;i++) {
		if (workload->ops[i].type == YCSB_READ || workload->ops[i].type == YCSB_SCAN) {
			struct kref* key = (struct kref*) (workload_buf.ptr + workload->ops[i].data_pos);
			key->ptr = (uint8_t*) &(key[1]);
		}
	}

	return 1;
}

void ycsb(char* dataset_name, const ycsb_workload_spec* spec) {
	struct timespec start_time;
	struct timespec end_time;
	uint64_t i, j;
	int result;
	dataset_t dataset;
	struct kv** kv_ptrs;
	ycsb_workload workload;
	uint64_t inserts_done = 0;
	int advance_read_latest_block = 0;
	uint8_t* next_read_latest_key;
	struct wormhole_iter * iter;
	struct wormhole * const wormhole = whunsafe_create(&mm_dont_copy_kv);

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	kv_ptrs = read_wormhole_kvs(&dataset, DEFAULT_VALUE_SIZE);

	generate_ycsb_workload(&dataset, kv_ptrs, &workload, spec, 0, 1);

	printf("Loading trie with %lu keys (%.1f%% of dataset)...\n", workload.initial_num_keys,
		   ((float)(workload.initial_num_keys)) / dataset.num_keys * 100.0);
	insert_kvs(wormhole, kv_ptrs, workload.initial_num_keys);
	iter = whunsafe_iter_create(wormhole);

	next_read_latest_key = workload.read_latest_blocks_for_thread[0][0];

	printf("Running workload...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	ycsb_op* ops = workload.ops;
	for (i = 0;i < spec->num_ops;i++) {
		ycsb_op* op = &(ops[i]);
		switch (op->type) {
			case YCSB_READ:{
				struct kref* key = (struct kref*) (workload.data_buf + op->data_pos);
				kref_update_hash32(key);
				result = whunsafe_probe(wormhole, key);
				if (result == 0) {
					printf("Error: Inserted key wasn't found\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_READ_LATEST:{
				struct kref* key = (struct kref*) next_read_latest_key;

				if (advance_read_latest_block) {
					// We have performed an insert. Set next_read_latest_key s.t. the next
					// read_latest op uses the new distribution
					next_read_latest_key = workload.read_latest_blocks_for_thread[0][inserts_done];
					advance_read_latest_block = 0;
				} else if (key->len != 0xFFFFFFFF) {
					// No inserts done & didn't reach sentinel - advance pointer in current block
					next_read_latest_key += sizeof(struct kref) + key->len;
				}

				if (key->len == 0xFFFFFFFF) {
					// Reached sentinel key
					break;
				}
				__builtin_prefetch(next_read_latest_key);

				kref_update_hash32(key);
				result = whunsafe_probe(wormhole, key);
				if (result == 0) {
					printf("Error: Inserted key wasn't found\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_UPDATE:
			case YCSB_INSERT:{
				struct kv* new_kv = (struct kv*) (workload.data_buf + op->data_pos);
				kv_update_hash(new_kv);
				result = whunsafe_set(wormhole, new_kv);
				if (result == 0) {
					printf("Error: whunsafe_set error\n");
					return;
				}
				inserts_done++;
				advance_read_latest_block = 1;
				speculation_barrier();
			}
			break;

			case YCSB_RMW:{
				struct kv* new_kv = (struct kv*) (workload.data_buf + op->data_pos);
				struct kref key;

				// Read key
				key.len = new_kv->klen;
				key.ptr = new_kv->kv;
				kref_update_hash32(&key);
				result = whunsafe_probe(wormhole, &key);
				if (result == 0) {
					printf("Error: Inserted key wasn't found\n");
					return;
				}

				// Put new value
				kv_update_hash(new_kv);
				result = whunsafe_set(wormhole, new_kv);
				if (result == 0) {
					printf("Error: whunsafe_set error\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_SCAN:{
				struct kref* key = (struct kref*) (workload.data_buf + op->data_pos);
				uint64_t checksum = 0;
				uint64_t range_size = (rand_dword() % 100) + 1;
				kref_update_hash32(key);
				whunsafe_iter_seek(iter, key);
				for (j = 0;j < range_size;j++) {
					struct kv* iteration_result = whunsafe_iter_next(iter, NULL);
					if (!iteration_result)
						break; // Reached the last key

					checksum += (uintptr_t) iteration_result;
				}

				// Make sure <checksum> isn't optimized away
				if (checksum == 0xFFFFFFFFFFFF)
					printf("Impossible!\n");
				speculation_barrier();
			}
			break;

			default:
				printf("Error: Unknown YCSB op type\n");
				return;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	float time_took = time_diff(&end_time, &start_time);
	report(time_took, spec->num_ops);
}

typedef struct  ycsb_thread_ctx_struct {
	struct wormhole* trie;
	uint64_t thread_id;
	uint64_t num_threads;
	uint64_t inserts_done;
	struct ycsb_thread_ctx_struct* thread_contexts;
	ycsb_workload workload;
} ycsb_thread_ctx;

void* mt_ycsb_thread(void* arg) {
	uint64_t i, j;
	int result;
	uint64_t total_read_latest = 0;
	uint64_t failed_read_latest = 0;
	uint64_t read_latest_from_thread = 0;
	ycsb_thread_ctx* inserter;
	ycsb_thread_ctx* ctx = (ycsb_thread_ctx*) arg;
	struct wormref* const ref = wormhole_ref(ctx->trie);
	struct wormhole_iter* iter;

	uint64_t last_inserts_done[ctx->num_threads];
	uint8_t* next_read_latest_key[ctx->num_threads];
	uint8_t** thread_read_latest_blocks[ctx->num_threads];

	for (i = 0;i < ctx->num_threads;i++) {
		last_inserts_done[i] = 0;
		thread_read_latest_blocks[i] = ctx->thread_contexts[i].workload.read_latest_blocks_for_thread[ctx->thread_id];
		next_read_latest_key[i] = thread_read_latest_blocks[i][0];
	}

	// When a Wormhole iterator is created it holds a lock on the minimal leaf
	// until it is moved somewhere else with iter_seek. Therefore, creating a new
	// iterator for each range would block other threads doing the same. Instead,
	// create a single iterator when starting and use it for all range scans.
	iter = wormhole_iter_create(ref);
	for (i = 0;i < ctx->workload.num_ops;i++) {
		ycsb_op* op = &(ctx->workload.ops[i]);
		switch (op->type) {
			case YCSB_READ:{
				struct kref* key = (struct kref*) (ctx->workload.data_buf + op->data_pos);
				kref_update_hash32(key);
				result = wormhole_probe(ref, key);
				if (result == 0) {
					printf("Error: Inserted key wasn't found\n");
					return NULL;
				}
				speculation_barrier();
			}
			break;

			case YCSB_READ_LATEST:{
				total_read_latest++;

				struct kref* key = (struct kref*) next_read_latest_key[read_latest_from_thread];

				// Advancing next_read_latest_key must be done before checking whether to
				// move to another block (by comparing inserts_done). Otherwise, in the
				// single-threaded case, we'll advance next_read_latest_key[0] after it was
				// set to the block start, and by an incorrect amount.
				if (key->len != 0xFFFFFFFFU)
					next_read_latest_key[read_latest_from_thread] += sizeof(struct kref) + key->len;

				read_latest_from_thread++;
				if (read_latest_from_thread == ctx->num_threads)
					read_latest_from_thread = 0;

				inserter = &(ctx->thread_contexts[read_latest_from_thread]);
				uint64_t inserts_done = __atomic_load_n(&(inserter->inserts_done), __ATOMIC_RELAXED);
				if (inserts_done != last_inserts_done[read_latest_from_thread]) {
					last_inserts_done[read_latest_from_thread] = inserts_done;

					uint8_t* block_start = thread_read_latest_blocks[read_latest_from_thread][inserts_done];
					next_read_latest_key[read_latest_from_thread] = block_start;
					__builtin_prefetch(&(thread_read_latest_blocks[read_latest_from_thread][inserts_done+8]));
				}
				__builtin_prefetch(next_read_latest_key[read_latest_from_thread]);

				if (key->len == 0xFFFFFFFF) {
					// Reached end-of-block sentinel
					failed_read_latest++;
					break;
				}

				kref_update_hash32(key);
				result = wormhole_probe(ref, key);
				if (result == 0) {
					printf("Error: Inserted key wasn't found\n");
					return NULL;
				}
				speculation_barrier();
			}
			break;

			case YCSB_UPDATE:
			case YCSB_INSERT:{
				struct kv* new_kv = (struct kv*) (ctx->workload.data_buf + op->data_pos);
				kv_update_hash(new_kv);
				result = wormhole_set(ref, new_kv);
				if (result == 0) {
					printf("Error: wormhole_set error\n");
					return NULL;
				}

				// Use atomic_store to make sure that the write isn't reordered with ct_insert,
				// and eventually becomes visible to other threads.
				__atomic_store_n(&(ctx->inserts_done), ctx->inserts_done + 1, __ATOMIC_RELEASE);
				speculation_barrier();
			}
			break;

			case YCSB_RMW:{
				struct kv* new_kv = (struct kv*) (ctx->workload.data_buf + op->data_pos);
				struct kref key;

				// Read key
				key.len = new_kv->klen;
				key.ptr = new_kv->kv;
				kref_update_hash32(&key);
				result = wormhole_probe(ref, &key);
				if (result == 0) {
					printf("Error: Inserted key wasn't found\n");
					return NULL;
				}

				// Put new value
				kv_update_hash(new_kv);
				result = wormhole_set(ref, new_kv);
				if (result == 0) {
					printf("Error: whunsafe_set error\n");
					return NULL;
				}
				speculation_barrier();
			}
			break;

			case YCSB_SCAN:{
				struct kref* key = (struct kref*) (ctx->workload.data_buf + op->data_pos);
				uint64_t checksum = 0;
				uint64_t range_size = (rand_dword() % 100) + 1;
				kref_update_hash32(key);

				wormhole_iter_seek(iter, key);
				for (j = 0;j < range_size;j++) {
					struct kv* iteration_result = wormhole_iter_next(iter, NULL);
					if (!iteration_result)
						break; // Reached the last key

					checksum += (uintptr_t) iteration_result;
				}
				// Unless we park the iterator, it holds a lock on the leaf its in,
				// preventing any insertions to that leaf.
				wormhole_iter_park(iter);

				// Make sure <checksum> isn't optimized away
				if (checksum == 0xFFFFFFFFFFFF)
					printf("Impossible!\n");
				speculation_barrier();
			}
			break;
		}
	}

	wormhole_unref(ref);

	if (failed_read_latest > 0) {
		printf("Note: %lu / %lu (%.1f%%) of read-latest operations were skipped\n",
			failed_read_latest, total_read_latest,
			((float)failed_read_latest) / total_read_latest * 100.0);
	}

	return NULL;
}

void mt_ycsb(char* dataset_name, const ycsb_workload_spec* spec, int num_threads) {
	int i;
	int result;
	dataset_t dataset;
	struct kv** kv_ptrs;
	struct timespec start_time;
	struct timespec end_time;
	ycsb_thread_ctx thread_contexts[num_threads];
	struct wormhole* wormhole = wormhole_create(&mm_dont_copy_kv);

	seed_and_print();

	printf("Reading dataset...\n");
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	kv_ptrs = read_wormhole_kvs(&dataset, DEFAULT_VALUE_SIZE);

	printf("Creating workloads");
	for (i = 0;i < num_threads; i++) {
		thread_contexts[i].trie = wormhole;
		thread_contexts[i].thread_id = i;
		thread_contexts[i].num_threads = num_threads;
		thread_contexts[i].inserts_done = 0;
		thread_contexts[i].thread_contexts = thread_contexts;
		generate_ycsb_workload(&dataset, kv_ptrs, &(thread_contexts[i].workload), spec,
							   i, num_threads);
		printf(".");
		fflush(stdout);
	}
	printf("\n");

	printf("Loading trie...\n");
	insert_kvs(wormhole, kv_ptrs, thread_contexts[0].workload.initial_num_keys);

	printf("Running workloads...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	run_multiple_threads(mt_ycsb_thread, num_threads, thread_contexts, sizeof(ycsb_thread_ctx));
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	float time_took = time_diff(&end_time, &start_time);
	report_mt(time_took, spec->num_ops * num_threads, num_threads);
}

const flag_spec_t FLAGS[] = {
	{ "--profiler-pid", 1},
	{ "--threads", 1},
	{ "--num-keys", 1},
	{ "--ycsb-uniform-dist", 0},
	{ "--dataset-size", 1},
	{ NULL, 0}
};

int main(int argc, char** argv) {
	char* dataset_name;
	dataset_t dataset;
	uint64_t dataset_size;
	int result;
	int num_threads;
	int is_singlethread_ycsb = 0;
	int is_multithread_ycsb = 0;
	ycsb_workload_spec ycsb_workload;

	args_t* args = parse_args((flag_spec_t*) FLAGS, argc, argv);
	if (args == NULL) {
		printf("Commandline error\n");
		return 1;
	}
	if (args->num_args < 2) {
		printf("Usage: %s <test name> <dataset name> \n", argv[0]);
		return 1;
	}

	profiler_pid = get_int_flag(args, "--profiler-pid", PID_NO_PROFILER);
	num_threads = get_int_flag(args, "--threads", DEFAULT_NUM_THREADS);
	dataset_name = args->args[1];

	if (!strcmp(args->args[0], "insert"))
		load_dataset(dataset_name);

	if (!strcmp(args->args[0], "mem-usage"))
		mem_usage(dataset_name, get_int_flag(args, "--num-keys", 0));

	if (!strcmp(args->args[0], "pos-lookup")) {
		seed_and_print();
		dataset_size = get_uint64_flag(args, "--dataset-size", DATASET_ALL_KEYS);
		result = init_dataset(&dataset, dataset_name, dataset_size);
		if (!result) {
			printf("Error creating dataset.\n");
			return 1;
		}
		pos_lookup(&dataset);
	}

	if (!strcmp(args->args[0], "mt-pos-lookup"))
		mt_pos_lookup(dataset_name, num_threads);

	if (!strcmp(args->args[0], "ycsb-a")) {
		ycsb_workload = YCSB_A_SPEC;
		is_singlethread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "ycsb-b")) {
		ycsb_workload = YCSB_B_SPEC;
		is_singlethread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "ycsb-c")) {
		ycsb_workload = YCSB_C_SPEC;
		is_singlethread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "ycsb-d")) {
		ycsb_workload = YCSB_D_SPEC;
		is_singlethread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "ycsb-e")) {
		ycsb_workload = YCSB_E_SPEC;
		is_singlethread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "ycsb-f")) {
		ycsb_workload = YCSB_F_SPEC;
		is_singlethread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "mt-ycsb-a")) {
		ycsb_workload = YCSB_A_SPEC;
		is_multithread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "mt-ycsb-b")) {
		ycsb_workload = YCSB_B_SPEC;
		is_multithread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "mt-ycsb-c")) {
		ycsb_workload = YCSB_C_SPEC;
		is_multithread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "mt-ycsb-d")) {
		ycsb_workload = YCSB_D_SPEC;
		is_multithread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "mt-ycsb-e")) {
		ycsb_workload = YCSB_E_SPEC;
		is_multithread_ycsb = 1;
	}

	if (!strcmp(args->args[0], "mt-ycsb-f")) {
		ycsb_workload = YCSB_F_SPEC;
		is_multithread_ycsb = 1;
	}

	if (is_singlethread_ycsb || is_multithread_ycsb) {
		if (has_flag(args, "--ycsb-uniform-dist"))
			ycsb_workload.distribution = DIST_UNIFORM;
	}

	if (is_singlethread_ycsb)
		ycsb(dataset_name, &ycsb_workload);

	if (is_multithread_ycsb)
		mt_ycsb(dataset_name, &ycsb_workload, num_threads);

	if (!strcmp(args->args[0], "mt-insert"))
		mt_insert(dataset_name, num_threads);
/*
	if (!strcmp(argv[1], "read-range"))
		read_ranges(argv[2]);
*/
	return 0;
}
