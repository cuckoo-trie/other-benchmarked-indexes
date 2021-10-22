#include <stdio.h>
#include <time.h>
#include <signal.h>
#include "../util.h"
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <hot/singlethreaded/HOTSingleThreaded.hpp>
#include <hot/rowex/HOTRowex.hpp>

#define MILLION 1000000
#define DEFAULT_NUM_KEYS (10 * MILLION)
#define DEFAULT_NUM_THREADS 4

#define PID_NO_PROFILER 0

template<typename KVType>
struct KVKeyExtractor {
	inline const char* operator()(const KVType kv) {
		return (const char*) &(kv->key);
	}
};

typedef hot::singlethreaded::HOTSingleThreaded<const char*, idx::contenthelpers::IdentityKeyExtractor> string_hot_t;
typedef hot::singlethreaded::HOTSingleThreaded<const string_kv*, KVKeyExtractor> kv_hot_t;
typedef hot::rowex::HOTRowex<const char*, idx::contenthelpers::IdentityKeyExtractor> mt_string_hot_t;
typedef hot::rowex::HOTRowex<const string_kv*, KVKeyExtractor> mt_kv_hot_t;

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

// Benchmark the case where each key-value pair is at most 8 bytes, and therefore can fit
// inside the nodes themselves
void load_uint64(uint64_t num_keys) {
	struct timespec start_time;
	struct timespec end_time;
	uint64_t i = 0;
	uint64_t start_mem, end_mem;
	hot::singlethreaded::HOTSingleThreaded<uint64_t, idx::contenthelpers::IdentityKeyExtractor> trie;

	seed_and_print();
	start_mem = virt_mem_usage();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0;i < num_keys;i++) {
		uint64_t key = ((uint64_t)rand_dword() << 32) + rand_dword();

		// Inserting keys with the high bit set causes assertion failures for some reason. Therefore, we benchmark 63-bit keys.
		key /= 2;
		trie.insert(key);
		speculation_barrier();
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	end_mem = virt_mem_usage();
	printf("Memory used: %luKB\n", (end_mem - start_mem) / 1024);

	float time_took = time_diff(&end_time, &start_time);
	printf("Took %.2fs (%.0fns/key)\n", time_took, time_took / num_keys * 1.0e9);
}

void mem_usage(char* dataset_name) {
	dataset_t dataset;
	int result;
	uint64_t i;
	uint64_t start_mem, end_mem;
	uint64_t index_overhead;
	uint64_t keys_size = 0;
	const char* all_keys;
	const char* pos;
	string_hot_t trie;

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}

	all_keys = (const char*)serialize_dataset(&dataset);
	if (!all_keys) {
		printf("Cannot interpret keys as strings. Do they contain NULL bytes?\n");
		return;
	}
	pos = all_keys;
	start_mem = virt_mem_usage();
	for (i = 0;i < dataset.num_keys;i++) {
		trie.insert(pos);
		pos += strlen(pos) + 1;
		keys_size += strlen(pos) + 1;
	}
	end_mem = virt_mem_usage();
	index_overhead = end_mem - start_mem;
	printf("Keys size: %luKB (%.1fb/key)\n", keys_size / 1024, ((float)keys_size) / dataset.num_keys);
	printf("Index size: %luKB (%.1fb/key)\n", index_overhead / 1024, ((float)index_overhead) / dataset.num_keys);
	printf("RESULT: keys=%lu bytes=%lu\n", dataset.num_keys, index_overhead);
}

// Benchmark the case where key-value pairs are longer than 8 bytes, so they are stored
// separately and their addresses are stored in the nodes
void load_dataset(char* dataset_name) {
	struct timespec start_time;
	struct timespec end_time;
	uint64_t i;
	int result;
	dataset_t dataset;
	ct_key* keys;
	string_hot_t trie;

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}

	printf("Reading dataset...\n");
	keys = read_string_dataset(&dataset);

	printf("Validating...\n");
	for (i = 0;i < dataset.num_keys;i++) {
		if (memchr(keys[i].bytes, 0, keys[i].size)) {
			printf("Invalid dataset. Key #%lu contains NULL byte.\n", i);
			return;
		}
	}

	printf("Loading...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0;i < dataset.num_keys;i++) {
		trie.insert((const char*)keys[i].bytes);
		speculation_barrier();
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	notify_critical_section_end();

	float time_took = time_diff(&end_time, &start_time);
	printf("Took %.2fs (%.0fns/key)\n", time_took, time_took / dataset.num_keys * 1.0e9);
	printf("RESULT: ops=%lu ms=%d\n", dataset.num_keys, (int)(time_took * 1000));
}

void pos_lookup(dataset_t* dataset) {
	const uint64_t num_lookups = 10 * MILLION;
	uint64_t i;
	struct timespec start_time;
	struct timespec end_time;
	ct_key* keys;
	string_hot_t trie;
	dynamic_buffer_t workload_data;
	uint64_t* workload_offsets = (uint64_t*) malloc(sizeof(uint64_t) * num_lookups);

	keys = read_string_dataset(dataset);

	printf("Loading...\n");
	for (i = 0;i < dataset->num_keys;i++)
		trie.insert((const char*)keys[i].bytes);

	printf("Creating workload...\n");
	dynamic_buffer_init(&workload_data);
	for (i = 0;i < num_lookups;i++) {
		ct_key* key = &(keys[rand_uint64() % dataset->num_keys]);
		uint64_t pos = dynamic_buffer_extend(&workload_data, key->size + 1);
		memcpy(workload_data.ptr + pos, key->bytes, key->size + 1);
		workload_offsets[i] = pos;
	}

	printf("Performing lookups...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0;i < num_lookups;i++) {
		auto value = trie.lookup((const char*)(workload_data.ptr + workload_offsets[i]));
		if (!value.mIsValid) {
			printf("ERROR! Key not found.\n");
			break;
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
	uint64_t num_keys;
	const char** keys;
	mt_string_hot_t* trie;
} mt_lookup_ctx;

void* mt_lookup_thread(void* arg) {
	mt_lookup_ctx* ctx = (mt_lookup_ctx*) arg;
	uint64_t i;

	for (i = 0; i < ctx->num_keys; i++) {
		auto value = ctx->trie->lookup(ctx->keys[i]);
		if (!value.mIsValid) {
			printf("ERROR! Key not found.\n");
			break;
		}
		speculation_barrier();
	}

	return NULL;
}

void mt_pos_lookup(char* dataset_name, unsigned int num_threads) {
	const uint64_t lookups_per_thread = 10 * MILLION;
	uint64_t i;
	int result;
	ct_key* keys;
	dataset_t dataset;
	mt_string_hot_t trie;
	struct timespec start_time;
	struct timespec end_time;
	dynamic_buffer_t workload_data;
	mt_lookup_ctx thread_contexts[num_threads];
	uint64_t total_lookups = num_threads * lookups_per_thread;
	const char** workload_keys = (const char**) malloc(sizeof(void*) * lookups_per_thread * num_threads);

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	keys = read_string_dataset(&dataset);

	printf("Loading...\n");
	for (i = 0;i < dataset.num_keys;i++)
		trie.insert((const char*)keys[i].bytes);

	printf("Creating workload...\n");
	dynamic_buffer_init(&workload_data);
	for (i = 0; i < total_lookups; i++) {
		ct_key* key = &(keys[rand_uint64() % dataset.num_keys]);
		uint64_t pos = dynamic_buffer_extend(&workload_data, key->size + 1);
		memcpy(workload_data.ptr + pos, key->bytes, key->size + 1);
		workload_keys[i] = (const char*) pos;
	}

	for (i = 0;i < total_lookups; i++)
		workload_keys[i] += (uintptr_t) workload_data.ptr;

	for (i = 0;i < num_threads; i++) {
		thread_contexts[i].trie = &trie;
		thread_contexts[i].num_keys = lookups_per_thread;
		thread_contexts[i].keys = &(workload_keys[i * lookups_per_thread]);
	}

	printf("Performing lookups...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	run_multiple_threads(mt_lookup_thread, num_threads, thread_contexts, sizeof(mt_lookup_ctx));
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	notify_critical_section_end();

	float time_took = time_diff(&end_time, &start_time);
	report_mt(time_took, lookups_per_thread * num_threads, num_threads);
}

typedef struct {
	uint64_t num_keys;
	ct_key* keys;
	mt_string_hot_t* trie;
} mt_insert_ctx;

void* mt_insert_thread(void* arg) {
	mt_insert_ctx* ctx = (mt_insert_ctx*) arg;
	uint64_t i;

	for (i = 0; i < ctx->num_keys; i++) {
		ctx->trie->insert((const char*) ctx->keys[i].bytes);
		speculation_barrier();
	}

	return NULL;
}

void mt_insert(char* dataset_name, unsigned int num_threads) {
	uint64_t i;
	int result;
	ct_key* keys;
	dataset_t dataset;
	mt_string_hot_t trie;
	struct timespec start_time;
	struct timespec end_time;
	pthread_t thread_ids[num_threads];
	mt_insert_ctx thread_contexts[num_threads];

	printf("Reading dataset...\n");
	init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	keys = read_string_dataset(&dataset);

	for (i = 0; i < num_threads; i++) {
		uint64_t start_key = (dataset.num_keys * i) / num_threads;
		uint64_t end_key = (dataset.num_keys * (i+1)) / num_threads;
		thread_contexts[i].num_keys = end_key - start_key;
		thread_contexts[i].keys = &(keys[start_key]);
		thread_contexts[i].trie = &trie;
	}

	printf("Inserting...\n");
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	for (i = 0; i < num_threads; i++) {
		result = pthread_create(&(thread_ids[i]), NULL, mt_insert_thread, &(thread_contexts[i]));
		if (result != 0) {
			printf("Failed to cerate thread\n");
			return;
		}
	}

	for (i = 0; i < num_threads; i++) {
		result = pthread_join(thread_ids[i], NULL);
		if (result != 0) {
			printf("Failed to join thread\n");
			return;
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	float time_took = time_diff(&end_time, &start_time);
	report_mt(time_took, dataset.num_keys, num_threads);
}

void read_ranges(string_hot_t* trie, ct_key* keys, uint64_t num_keys, uint64_t num_ranges, uint64_t max_range_size) {
	uint64_t i,j;
	uint64_t first_byte_sum = 0;

	for (i = 0;i < num_ranges;i++) {
		uint64_t range_size = rand_dword() % max_range_size;
		uint64_t start_key = rand_dword() % num_keys;
		auto it = trie->lower_bound((const char*)keys[start_key].bytes);
		for (j = 0;j < range_size;j++) {
			const char* value = *it;
			first_byte_sum += (unsigned char)(*value);  // Perform some computation to force reading the value
			++it;                      // Advance to the next value
			if (it == trie->end())
				break;   // Reading a value from an exhausted iterator is not supported
		}
	}
	printf("Done. Checksum: %lu\n", first_byte_sum);  // Print first_byte_sum to make sure it is not optimized out
}

void prefetch_ranges(string_hot_t* trie, ct_key* keys, uint64_t num_keys, uint64_t num_ranges, uint64_t max_range_size) {
	uint64_t i,j;
	const char* range_keys[max_range_size];
	uint64_t first_byte_sum = 0;

	for (i = 0;i < num_ranges;i++) {
		uint64_t range_size = rand_dword() % max_range_size;
		uint64_t start_key = rand_dword() % num_keys;
		auto it = trie->lower_bound((const char*)keys[start_key].bytes);
		for (j = 0;j < range_size;j++) {
			const char* value = *it;
			range_keys[j] = value;
			__builtin_prefetch(value);
			++it;                      // Advance to the next value
			if (it == trie->end())
				break;   // Reading a value from an exhausted iterator is not supported
		}

		// Change range_size to the actual size of the range, in case we hit the dataset end
		range_size = j;
		for (j = 0; j < range_size;j++)
			first_byte_sum += (unsigned char)(*range_keys[j]);  // Perform some computation to force reading the value
	}
	printf("Done. Checksum: %lu\n", first_byte_sum);  // Print first_byte_sum to make sure it is not optimized out
}

void skip_ranges(string_hot_t* trie, ct_key* keys, uint64_t num_keys, uint64_t num_ranges, uint64_t max_range_size) {
	uint64_t i,j;
	uint64_t ranges_overflown = 0;
	for (i = 0;i < num_ranges;i++) {
		uint64_t range_size = rand_dword() % max_range_size;
		uint64_t start_key = rand_dword() % num_keys;
		auto it = trie->lower_bound((const char*)keys[start_key].bytes);
		for (j = 0;j < range_size;j++) {
			++it;                      // Advance to the next value
			if (it == trie->end()) {
				ranges_overflown++;
				break;   // Reading a value from an exhausted iterator is not supported
			}
		}
	}
	printf("Done. %lu/%lu ranges hit dataset end\n", ranges_overflown, num_ranges);
}

typedef void (*range_func_t)(string_hot_t*, ct_key*, uint64_t, uint64_t, uint64_t);

// Load the dataset, then move an iterator over short ranges while reading each key.
void process_ranges(char* dataset_name, range_func_t range_func) {
	struct timespec start_time;
	struct timespec end_time;
	const uint64_t num_ranges = MILLION;
	const uint64_t max_range_size = 100;
	uint64_t i;
	int result;
	dataset_t dataset;
	ct_key* keys;
	string_hot_t trie;

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}
	keys = read_string_dataset(&dataset);

	printf("Loading...\n");
	for (i = 0;i < dataset.num_keys;i++)
		trie.insert((const char*)keys[i].bytes);

	printf("Iterating...\n");
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	range_func(&trie, keys, dataset.num_keys, num_ranges, max_range_size);
	clock_gettime(CLOCK_MONOTONIC, &end_time);

	printf("Iteration took %.2fs\n", time_diff(&end_time, &start_time));
}

const ycsb_workload_spec YCSB_A_SPEC = {{0.5,  0,    0.5,  0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_B_SPEC = {{0.95, 0,    0.05, 0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_C_SPEC = {{1.0,  0,    0,    0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_D_SPEC = {{0,    0.95, 0,    0.05, 0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_E_SPEC = {{0,    0,    0,    0.05, 0.95, 0  }, 2  * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_F_SPEC = {{0.5,  0,    0,    0,    0,    0.5}, 10 * MILLION, DIST_ZIPF};

typedef struct ycsb_thread_ctx_struct {
	void* trie;  // Either mt_kv_hot_t* or kv_hot_t*
	uint64_t thread_id;
	uint64_t num_threads;
	uint64_t inserts_done;
	struct ycsb_thread_ctx_struct* thread_contexts;
	ycsb_workload workload;
} ycsb_thread_ctx;

template<typename HOTType>
void execute_ycsb_workload(ycsb_thread_ctx* ctx) {
	uint64_t i, j;
	uint64_t inserter_idx;
	uint64_t total_read_latest = 0;
	uint64_t failed_read_latest = 0;
	uint64_t read_latest_from_thread = 0;
	ycsb_thread_ctx* inserter;
	HOTType trie = (HOTType) (ctx->trie);

	uint64_t last_inserts_done[ctx->num_threads];
	uint8_t* next_read_latest_key[ctx->num_threads];
	uint8_t** thread_read_latest_blocks[ctx->num_threads];

	for (i = 0;i < ctx->num_threads;i++) {
		last_inserts_done[i] = 0;
		thread_read_latest_blocks[i] = ctx->thread_contexts[i].workload.read_latest_blocks_for_thread[ctx->thread_id];
		next_read_latest_key[i] = thread_read_latest_blocks[i][0];
	}

	for (i = 0;i < ctx->workload.num_ops; i++) {
		ycsb_op* op = &(ctx->workload.ops[i]);
		switch (op->type) {
			case YCSB_READ:{
				char* key = (char*) (ctx->workload.data_buf + op->data_pos);
				auto result = trie->lookup(key);
				if (!result.mIsValid) {
					printf("Error: key not found\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_READ_LATEST:{
				total_read_latest++;
				inserter_idx = read_latest_from_thread;

				blob_t* key = (blob_t*) next_read_latest_key[inserter_idx];

				// Advancing next_read_latest_key must be done before checking whether to
				// move to another block (by comparing inserts_done). Otherwise, in the
				// single-threaded case, we'll advance next_read_latest_key[0] after it was
				// set to the block start, and by an incorrect amount.
				if (key->size != 0xFFFFFFFFU)
					next_read_latest_key[inserter_idx] += sizeof(blob_t) + key->size;

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

				if (key->size == 0xFFFFFFFFU) {
					// Reached end-of-block sentinel
					failed_read_latest++;
					break;
				}

				auto result = trie->lookup((const char*) (key->bytes));
				if (!result.mIsValid) {
					printf("Error: key not found\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_UPDATE:{
				string_kv* updated_kv = (string_kv*) (ctx->workload.data_buf + op->data_pos);
				auto upsert_result = trie->upsert(updated_kv);
				if (!upsert_result.mIsValid) {
					printf("Error: upsert inserted a new key instead of updating an existing one\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_INSERT:{
				string_kv* kv = (string_kv*) (ctx->workload.data_buf + op->data_pos);
				bool result = trie->insert(kv);
				if (!result) {
					printf("Error: key wasn't inserted\n");
					return;
				}

				// Use atomic_store to make sure that the write isn't reordered with ct_insert,
				// and eventually becomes visible to other threads.
				__atomic_store_n(&(ctx->inserts_done), ctx->inserts_done + 1, __ATOMIC_RELEASE);
				speculation_barrier();
			}
			break;

			case YCSB_RMW:{
				string_kv* updated_kv = (string_kv*) (ctx->workload.data_buf + op->data_pos);

				// Read key
				auto result = trie->lookup(updated_kv->key);
				if (!result.mIsValid) {
					printf("Error: key not found\n");
					return;
				}

				// Update key
				auto upsert_result = trie->upsert(updated_kv);
				if (!upsert_result.mIsValid) {
					printf("Error: upsert inserted a new key instead of updating an existing one\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_SCAN:{
				char* key = (char*) (ctx->workload.data_buf + op->data_pos);
				uint64_t range_size = (rand_dword() % 100) + 1;

				uint64_t checksum = 0;
				auto it = trie->lower_bound((const char*)key);
				auto end = trie->end();
				for (j = 0;j < range_size;j++) {
					++it;
					if (it == end)
						break;
					checksum += (uintptr_t) *it;

				}
				// Make sure <checksum> isn't optimized away
				if (checksum == 0xFFFFFFFFFFFF)
					printf("Impossible!\n");
				speculation_barrier();
			}
			break;

			default:
				abort();
		}
	}

	if (failed_read_latest > 0) {
		printf("Note: %lu / %lu (%.1f%%) of read-latest operations were skipped\n",
			failed_read_latest, total_read_latest,
			((float)failed_read_latest) / total_read_latest * 100.0);
	}
}

void generate_ycsb_workload(dataset_t* dataset, string_kv** kvs, ycsb_workload* workload,
						   const ycsb_workload_spec* spec, int thread_id,
						   int num_threads) {
	uint64_t i;
	int data_size;
	string_kv* kv;
	uint64_t num_inserts = 0;
	uint64_t insert_offset;
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
		return;
	}

	if (spec->op_type_probs[YCSB_READ_LATEST] > 0.0) {
		// spec->distribution is meaningless for read-latest. Read offsets for read-latest are
		// always Zipf-distributed.
		assert(spec->distribution == DIST_ZIPF);
		rand_zipf_rank_init(&backward_dist, workload->initial_num_keys, YCSB_SKEW);
	}

	dynamic_buffer_init(&workload_buf);
	for (i = 0; i < spec->num_ops; i++) {
		ycsb_op* op = &(workload->ops[i]);
		op->type = choose_ycsb_op_type(spec->op_type_probs);

		if (num_inserts == inserts_per_thread && op->type == YCSB_INSERT) {
			// Used all keys intended for insertion. Do another op type.
			i--;
			continue;
		}

		switch (op->type) {
			case YCSB_SCAN:
			case YCSB_READ:{
				kv = kvs[rand_dist(&dist)];
				data_size = strlen(kv->key) + 1;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);
				memcpy(workload_buf.ptr + op->data_pos, kv->key, data_size);
			}
			break;

			case YCSB_READ_LATEST:
				// Data for read-latest ops is generated separately
				break;

			case YCSB_RMW:
			case YCSB_UPDATE:{
				kv = kvs[rand_dist(&dist)];
				data_size = strlen(kv->key) + 1 + sizeof(string_kv);
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				string_kv* updated_kv = (string_kv*) (workload_buf.ptr + op->data_pos);
				memcpy(updated_kv, kv, data_size);
				updated_kv->value = 7;
			}
			break;

			case YCSB_INSERT:{
				kv = kvs[insert_offset + num_inserts];
				num_inserts++;
				data_size = strlen(kv->key) + 1 + sizeof(string_kv);
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				memcpy(workload_buf.ptr + op->data_pos, kv, data_size);
			}
			break;

			default:
				printf("Error: Unknown YCSB op type\n");
				return;
		}
	}

	// Create the read-latest key blocks
	uint64_t block;
	uint64_t thread;
	for (thread = 0; thread < num_threads; thread++) {
		uint8_t** block_offsets = (uint8_t**) malloc(sizeof(uint64_t) * (num_inserts + 1));
		workload->read_latest_blocks_for_thread[thread] = block_offsets;

		// We have one block for each amount of inserts between 0 and num_inserts, /inclusive/
		for (block = 0; block < num_inserts + 1; block++) {
			for (i = 0; i < read_latest_block_size; i++) {
				uint64_t backwards = rand_dist(&backward_dist);
				if (backwards < block * num_threads) {
					// This read-latest op refers to a key that was inserted during the workload
					backwards /= num_threads;
					kv = kvs[insert_offset + block - backwards - 1];
				} else {
					// This read-latest op refers to a key that was loaded before the workload started
					backwards -= block * num_threads;
					kv = kvs[workload->initial_num_keys - backwards - 1];
				}

				data_size = sizeof(blob_t) + strlen(kv->key) + 1;
				uint64_t data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				blob_t* key = (blob_t*) (workload_buf.ptr + data_pos);
				key->size = strlen(kv->key) + 1;
				memcpy(key->bytes, kv->key, key->size);

				if (i == 0)
					block_offsets[block] = (uint8_t*) data_pos;
			}

			uint64_t sentinel_pos = dynamic_buffer_extend(&workload_buf, sizeof(blob_t));
			blob_t* sentinel = (blob_t*) (workload_buf.ptr + sentinel_pos);
			sentinel->size = 0xFFFFFFFFU;
		}
	}

	workload->data_buf = workload_buf.ptr;

	// Now that the final buffer address is known, convert the read-latest offsets to pointers
	for (thread = 0; thread < num_threads; thread++) {
		for (block = 0; block < num_inserts + 1; block++)
			workload->read_latest_blocks_for_thread[thread][block] += (uintptr_t) (workload->data_buf);
	}
}

void ycsb(char* dataset_name, const ycsb_workload_spec* spec) {
	struct timespec start_time;
	struct timespec end_time;
	ycsb_thread_ctx ctx;
	dataset_t dataset;
	string_kv** kvs;
	kv_hot_t trie;
	int result;
	uint64_t i;

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}

	kvs = create_string_kvs(&dataset);

	// Create workload
	generate_ycsb_workload(&dataset, kvs, &(ctx.workload), spec, 0, 1);

	// Initialize context
	ctx.trie = &trie;
	ctx.thread_id = 0;
	ctx.num_threads = 1;
	ctx.inserts_done = 0;
	ctx.thread_contexts = &ctx;

	// Fill the tree
	for (i = 0; i < ctx.workload.initial_num_keys; i++) {
		trie.insert(kvs[i]);
	}

	// Perform YCSB ops
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	execute_ycsb_workload<kv_hot_t*>(&ctx);
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	notify_critical_section_end();
	float time_took = time_diff(&end_time, &start_time);
	report(time_took, spec->num_ops);
}

void* ycsb_thread(void* arg) {
	ycsb_thread_ctx* ctx = (ycsb_thread_ctx*) arg;
	execute_ycsb_workload<mt_kv_hot_t*>(ctx);
	return NULL;
}

void mt_ycsb(char* dataset_name, const ycsb_workload_spec* spec, unsigned int num_threads) {
	uint64_t i;
	int result;
	dataset_t dataset;
	string_kv** kvs;
	mt_kv_hot_t trie;
	struct timespec start_time;
	struct timespec end_time;
	ycsb_thread_ctx thread_contexts[num_threads];

	seed_and_print();
	result = init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	if (!result) {
		printf("Error creating dataset.\n");
		return;
	}

	kvs = create_string_kvs(&dataset);

	for (i = 0;i < num_threads;i++) {
		ycsb_thread_ctx* ctx = &(thread_contexts[i]);
		generate_ycsb_workload(&dataset, kvs, &(ctx->workload), spec, i, num_threads);
		ctx->trie = &trie;
		ctx->thread_id = i;
		ctx->inserts_done = 0;
		ctx->num_threads = num_threads;
		ctx->thread_contexts = thread_contexts;
	}

	// Fill the tree
	for (i = 0; i < thread_contexts[0].workload.initial_num_keys; i++) {
		trie.insert(kvs[i]);
	}

	// Perform YCSB ops
	notify_critical_section_start();
	clock_gettime(CLOCK_MONOTONIC, &start_time);
	run_multiple_threads(ycsb_thread, num_threads, thread_contexts, sizeof(ycsb_thread_ctx));
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	notify_critical_section_end();
	float time_took = time_diff(&end_time, &start_time);
	report_mt(time_took, spec->num_ops * num_threads, num_threads);
}

const flag_spec_t FLAGS[] = {
	{ "--profiler-pid", 1},
	{ "--threads", 1},
	{ "--dataset-size", 1},
	{ "--ycsb-uniform-dist", 0},
	{ NULL, 0}
};

int main(int argc, char** argv) {
	int result;
	char* test_name;
	char* dataset_name = NULL;
	int num_threads;
	dataset_t dataset;
	uint64_t dataset_size;
	ycsb_workload_spec ycsb_workload;
	int is_ycsb = 0;
	int is_mt_ycsb = 0;
	args_t* args = parse_args((flag_spec_t*) FLAGS, argc, argv);

	if (args == NULL) {
		printf("Commandline error\n");
		return 1;
	}
	if (args->num_args < 1) {
		printf("Missing test name\n");
		return 1;
	}
	profiler_pid = get_int_flag(args, "--profiler-pid", PID_NO_PROFILER);
	test_name = args->args[0];

	if (!strcmp(test_name, "load-uint64")) {
		int num_keys = DEFAULT_NUM_KEYS;
		if (argc >= 3)
			num_keys = atoi(argv[2]);
		load_uint64(num_keys);
		return 0;
	}

	if (args->num_args < 2) {
		printf("Missing dataset name\n");
		return 1;
	}
	dataset_name = args->args[1];
	num_threads = get_int_flag(args, "--threads", DEFAULT_NUM_THREADS);

	if (!strcmp(test_name, "pos-lookup")) {
		seed_and_print();
		dataset_size = get_uint64_flag(args, "--dataset-size", DATASET_ALL_KEYS);
		result = init_dataset(&dataset, dataset_name, dataset_size);
		if (!result) {
			printf("Error creating dataset.\n");
			return 1;
		}
		pos_lookup(&dataset);
		return 0;
	}
	if (!strcmp(test_name, "mt-pos-lookup")) {
		mt_pos_lookup(dataset_name, num_threads);
		return 0;
	}
	if (!strcmp(test_name, "range-read")) {
		process_ranges(dataset_name, read_ranges);
		return 0;
	}
	if (!strcmp(test_name, "range-skip")) {
		process_ranges(dataset_name, skip_ranges);
		return 0;
	}
	if (!strcmp(test_name, "range-prefetch")) {
		process_ranges(dataset_name, prefetch_ranges);
		return 0;
	}
	if (!strcmp(test_name, "insert")) {
		load_dataset(dataset_name);
		return 0;
	}
	if (!strcmp(test_name, "ycsb-a")) {
		ycsb_workload = YCSB_A_SPEC;
		is_ycsb = 1;
	}
	if (!strcmp(test_name, "ycsb-b")) {
		ycsb_workload = YCSB_B_SPEC;
		is_ycsb = 1;
	}
	if (!strcmp(test_name, "ycsb-c")) {
		ycsb_workload = YCSB_C_SPEC;
		is_ycsb = 1;
	}
	if (!strcmp(test_name, "ycsb-d")) {
		ycsb_workload = YCSB_D_SPEC;
		is_ycsb = 1;
	}
	if (!strcmp(test_name, "ycsb-e")) {
		ycsb_workload = YCSB_E_SPEC;
		is_ycsb = 1;
	}
	if (!strcmp(test_name, "ycsb-f")) {
		ycsb_workload = YCSB_F_SPEC;
		is_ycsb = 1;
	}
	if (!strcmp(test_name, "mt-ycsb-a")) {
		ycsb_workload = YCSB_A_SPEC;
		is_mt_ycsb = 1;
	}
	if (!strcmp(test_name, "mt-ycsb-b")) {
		ycsb_workload = YCSB_B_SPEC;
		is_mt_ycsb = 1;
	}
	if (!strcmp(test_name, "mt-ycsb-c")) {
		ycsb_workload = YCSB_C_SPEC;
		is_mt_ycsb = 1;
	}
	if (!strcmp(test_name, "mt-ycsb-d")) {
		ycsb_workload = YCSB_D_SPEC;
		is_mt_ycsb = 1;
	}
	if (!strcmp(test_name, "mt-ycsb-e")) {
		ycsb_workload = YCSB_E_SPEC;
		is_mt_ycsb = 1;
	}
	if (!strcmp(test_name, "mt-ycsb-f")) {
		ycsb_workload = YCSB_F_SPEC;
		is_mt_ycsb = 1;
	}
	if ((is_ycsb || is_mt_ycsb) && has_flag(args, "--ycsb-uniform-dist"))
		ycsb_workload.distribution = DIST_UNIFORM;

	if (is_ycsb) {
		ycsb(dataset_name, &ycsb_workload);
		return 0;
	}

	if (is_mt_ycsb) {
		mt_ycsb(dataset_name, &ycsb_workload, num_threads);
		return 0;
	}

	if (!strcmp(test_name, "mt-insert")) {
		mt_insert(dataset_name, num_threads);
		return 0;
	}
	if (!strcmp(test_name, "mem-usage")) {
		mem_usage(dataset_name);
		return 0;
	}

	printf("Unknown test name '%s'\n", test_name);
	return 1;
}
