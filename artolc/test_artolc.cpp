#include <signal.h>

#include "Tree.h"
#include "../util.h"

#define PID_NO_PROFILER 0
#define DEFAULT_VALUE_SIZE 8

// ARTOLC requires range queries to specify an end key. To support unbounded
// ranges for YCSB, we set the end key to a string of 0xFF bytes. This is the
// length of that string (including the terminating NULL)
#define RANGE_END_SIZE 129

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

typedef struct {
	uint32_t key_size;
	uint32_t value_size;
	uint8_t kv[];
} kv_t;

// <tid> describes a key-value pair. Set <key> to the key of that pair.
void load_key(TID tid, Key &key) {
	kv_t* kv = (kv_t*) tid;

	// Hack: ART_OLC expects the loadKey function to /copy/ the key data into <key>.
	// To save a memcpy call we instead /change the .data pointer/ of <key> to point
	// to the key bytes inside the kv.
	// For keys longer than stackLen, ART_OLC assumes that the data pointer points
	// to a buffer owned by the Key object and will be freed in the Key::~Key destructor. To
	// prevent ART_OLC from trying to free the key bytes inside the KV, we simply disallow
	// keys longer than stackLen.
	if (kv->key_size > key.stackLen) {
		printf("Error: key too long\n");
		abort();
	}

	key.len = kv->key_size;
	key.data = kv->kv;
}

kv_t** read_kvs(dataset_t* dataset, uint64_t value_size) {
	uint64_t i;
	ct_key key;
	dynamic_buffer_t kvs_buf;
	uint8_t* key_buf = (uint8_t*) malloc(MAX_KEY_SIZE);
	uintptr_t* kv_ptrs = (uintptr_t*) malloc(sizeof(struct kv*) * dataset->num_keys);
	key.bytes = key_buf;

	dynamic_buffer_init(&kvs_buf);
	for (i = 0;i < dataset->num_keys;i++) {
		dataset->read_key(dataset, &key);

		uint64_t pos = dynamic_buffer_extend(&kvs_buf, sizeof(kv_t) + key.size + 1 + value_size);
		kv_t* kv = (kv_t*) (kvs_buf.ptr + pos);

		kv->key_size = key.size + 1;
		kv->value_size = value_size;
		memcpy(kv->kv, key.bytes, key.size);

		// Null-terminate the key. ARTOLC doesn't handle correctly the case where one key is the
		// prefix of another. Using keys without null bytes and null-terminating them ensures that
		// no key is the prefix of another.
		kv->kv[key.size] = 0;
		memset(kv->kv + kv->key_size, 0xAB, kv->value_size);
		kv_ptrs[i] = pos;
	}

	for (i = 0;i < dataset->num_keys;i++)
		kv_ptrs[i] += (uintptr_t)(kvs_buf.ptr);

	return (kv_t**) kv_ptrs;
}

typedef struct {
	ART_OLC::Tree* tree;
	uint64_t num_keys;
	uint8_t* workload_buf;
} mt_insert_thread_ctx;

void* mt_insert_thread(void* arg) {
	uint64_t i;
	Key kv_key;
	uint8_t* buf_pos;
	mt_insert_thread_ctx* ctx = (mt_insert_thread_ctx*) arg;

	auto thread_info = ctx->tree->getThreadInfo();
	buf_pos = ctx->workload_buf;
	for (i = 0; i < ctx->num_keys; i++) {
		kv_t* kv = (kv_t*) buf_pos;

		load_key((TID) kv, kv_key);   // Set kv_key to point to the key of <kv>
		ctx->tree->insert(kv_key, (TID) kv, thread_info);
		buf_pos += sizeof(kv_t) + kv->key_size + kv->value_size;
		speculation_barrier();
	}

	return NULL;
}

void test_mt_insert(dataset_t* dataset, unsigned int num_threads) {
	uint64_t i;
	kv_t** kv_ptrs;
	stopwatch_t timer;
	ART_OLC::Tree tree(load_key);
	mt_insert_thread_ctx thread_contexts[num_threads];

	printf("Reading dataset...\n");
	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	for (i = 0;i < num_threads;i++) {
		uint64_t first_kv = dataset->num_keys * i / num_threads;
		uint64_t last_kv = dataset->num_keys * (i+1) / num_threads;

		thread_contexts[i].workload_buf = (uint8_t*) kv_ptrs[first_kv];
		thread_contexts[i].num_keys = last_kv - first_kv;
		thread_contexts[i].tree = &tree;
	}

	printf("Inserting...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	run_multiple_threads(mt_insert_thread, num_threads, thread_contexts, sizeof(mt_insert_thread_ctx));
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report_mt(time_took, dataset->num_keys, num_threads);
}

typedef struct {
	ART_OLC::Tree* tree;
	uint8_t* keys_buf;
	uint64_t num_keys;
} mt_pos_lookup_ctx;

void* mt_pos_lookup_thread(void* arg) {
	uint64_t i;
	Key key;
	uint8_t* buf_pos;
	mt_pos_lookup_ctx* ctx = (mt_pos_lookup_ctx*) arg;

	auto thread_info = ctx->tree->getThreadInfo();

	buf_pos = ctx->keys_buf;
	for (i = 0; i < ctx->num_keys; i++) {
		blob_t* target_key = (blob_t*) buf_pos;
		key.data = target_key->bytes;
		key.len = target_key->size;
		TID result = ctx->tree->lookup(key, thread_info);
		if (result == 0) {
			printf("Error: a key was not found!\n");
			return NULL;
		}

		buf_pos += sizeof(blob_t) + target_key->size;
		speculation_barrier();
	}

	return NULL;
}

void test_mt_pos_lookup(dataset_t* dataset, unsigned int num_threads) {
	const uint64_t lookups_per_thread = 10 * MILLION;

	uint64_t i, j;
	Key key;
	kv_t** kv_ptrs;
	stopwatch_t timer;
	dynamic_buffer_t workloads_buf;
	ART_OLC::Tree tree(load_key);
	mt_pos_lookup_ctx thread_contexts[num_threads];
	auto thread_info = tree.getThreadInfo();

	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	printf("Inserting...\n");
	for (i = 0; i < dataset->num_keys; i++) {
		load_key((TID) kv_ptrs[i], key);
		tree.insert(key, (TID) kv_ptrs[i], thread_info);
	}

	printf("Creating workloads...\n");
	dynamic_buffer_init(&workloads_buf);
	for (i = 0; i < num_threads; i++) {
		thread_contexts[i].tree = &tree;
		thread_contexts[i].num_keys = lookups_per_thread;
		thread_contexts[i].keys_buf = (uint8_t*) workloads_buf.pos;

		for (j = 0; j < lookups_per_thread; j++) {
			kv_t* kv = kv_ptrs[rand_uint64() % dataset->num_keys];
			uint64_t data_size = sizeof(blob_t) + kv->key_size;
			uint64_t offset = dynamic_buffer_extend(&workloads_buf, data_size);
			blob_t* data = (blob_t*) (workloads_buf.ptr + offset);
			data->size = kv->key_size;
			memcpy(data->bytes, kv->kv, kv->key_size);
		}
	}

	for (i = 0; i < num_threads; i++)
		thread_contexts[i].keys_buf += (uintptr_t) workloads_buf.ptr;

	printf("Performing lookups...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	run_multiple_threads(mt_pos_lookup_thread, num_threads, thread_contexts, sizeof(mt_pos_lookup_ctx));
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report_mt(time_took, lookups_per_thread * num_threads, num_threads);
}

void test_mem_usage(dataset_t* dataset) {
	uint64_t i;

	Key key;
	kv_t** kv_ptrs;
	uint64_t start_mem, end_mem;
	uint64_t index_overhead;
	ART_OLC::Tree tree(load_key);
	auto thread_info = tree.getThreadInfo();

	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	start_mem = virt_mem_usage();
	for (i = 0; i < dataset->num_keys; i++) {
		load_key((TID) kv_ptrs[i], key);
		tree.insert(key, (TID) kv_ptrs[i], thread_info);
	}
	end_mem = virt_mem_usage();

	index_overhead = end_mem - start_mem;
	printf("Used %.1fMB (%.1fb/key)\n", index_overhead / 1.0e6,
		index_overhead / ((float)dataset->num_keys));
	printf("RESULT: keys=%lu bytes=%lu\n", dataset->num_keys, index_overhead);
}

const ycsb_workload_spec YCSB_A_SPEC = {{0.5,  0,    0.5,  0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_B_SPEC = {{0.95, 0,    0.05, 0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_C_SPEC = {{1.0,  0,    0,    0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_D_SPEC = {{0,    0.95, 0,    0.05, 0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_E_SPEC = {{0,    0,    0,    0.05, 0.95, 0  }, 2  * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_F_SPEC = {{0.5,  0,    0,    0,    0,    0.5}, 10 * MILLION, DIST_ZIPF};

void generate_ycsb_workload(dataset_t* dataset, kv_t** kv_ptrs, ycsb_workload* workload,
							const ycsb_workload_spec* spec, int thread_id, int num_threads) {
	kv_t* kv;
	uint64_t i;
	uint64_t data_size;
	uint64_t insert_offset;
	uint64_t inserts_per_thread;
	uint64_t read_latest_block_size;
	uint64_t num_inserts = 0;
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
			case YCSB_READ: {
				kv = kv_ptrs[rand_dist(&dist)];
				data_size = sizeof(blob_t) + kv->key_size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				blob_t* key = (blob_t*) (workload_buf.ptr + op->data_pos);
				key->size = kv->key_size;
				memcpy(key->bytes, kv->kv, kv->key_size);
			}
			break;

			case YCSB_READ_LATEST:
				// Data for read-latest ops is generated separately
				break;

			case YCSB_RMW:
			case YCSB_UPDATE: {
				kv = kv_ptrs[rand_dist(&dist)];
				data_size = sizeof(kv_t) + kv->key_size + kv->value_size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				kv_t* new_kv = (kv_t*) (workload_buf.ptr + op->data_pos);
				new_kv->key_size = kv->key_size;
				new_kv->value_size = kv->value_size;
				memcpy(new_kv->kv, kv->kv, kv->key_size);
				memset(new_kv->kv + new_kv->key_size, 7, new_kv->value_size);  // Update to a dummy value
			}
			break;

			case YCSB_INSERT: {
				kv = kv_ptrs[insert_offset + num_inserts];
				num_inserts++;
				data_size = sizeof(kv_t) + kv->key_size + kv->value_size;
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
					kv = kv_ptrs[insert_offset + block - backwards - 1];
				} else {
					// This read-latest op refers to a key that was loaded before the workload started
					backwards -= block * num_threads;
					kv = kv_ptrs[workload->initial_num_keys - backwards - 1];
				}

				data_size = sizeof(blob_t) + kv->key_size;
				uint64_t data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				blob_t* key = (blob_t*) (workload_buf.ptr + data_pos);
				key->size = kv->key_size;
				memcpy(key->bytes, kv->kv, kv->key_size);

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

typedef struct ycsb_thread_ctx_struct {
	ART_OLC::Tree* tree;
	uint64_t num_threads;
	uint64_t thread_id;
	uint64_t inserts_done;
	struct ycsb_thread_ctx_struct* thread_contexts;
	ycsb_workload workload;
} ycsb_thread_ctx;

void* ycsb_thread(void* arg) {
	uint64_t i, j;
	Key key;
	Key dummy;
	Key max_key;

	TID range_results[100];
	std::size_t num_range_results;
	uint8_t max_key_bytes[RANGE_END_SIZE];

	uint64_t total_read_latest = 0;
	uint64_t failed_read_latest = 0;
	uint64_t read_latest_from_thread = 0;
	ycsb_thread_ctx* inserter;
	ycsb_thread_ctx* ctx = (ycsb_thread_ctx*) arg;
	uint64_t rand_state = ctx->thread_id;

	auto thread_info = ctx->tree->getThreadInfo();

	memset(max_key_bytes, 0xFF, RANGE_END_SIZE - 1);
	max_key_bytes[RANGE_END_SIZE - 1] = 0;
	max_key.set((const char*) max_key_bytes, RANGE_END_SIZE);

	uint64_t last_inserts_done[ctx->num_threads];
	uint8_t* next_read_latest_key[ctx->num_threads];
	uint8_t** thread_read_latest_blocks[ctx->num_threads];

	for (i = 0; i < ctx->num_threads; i++) {
		last_inserts_done[i] = 0;
		thread_read_latest_blocks[i] = ctx->thread_contexts[i].workload.read_latest_blocks_for_thread[ctx->thread_id];
		next_read_latest_key[i] = thread_read_latest_blocks[i][0];
	}

	for (i = 0; i < ctx->workload.num_ops; i++) {
		ycsb_op* op = &(ctx->workload.ops[i]);
		switch (op->type) {
			case YCSB_READ: {
				blob_t* target_key = (blob_t*) (ctx->workload.data_buf + op->data_pos);
				key.len = target_key->size;
				key.data = target_key->bytes;
				TID result = ctx->tree->lookup(key, thread_info);
				if (result == 0) {
					printf("Error: a key was not found\n");
					return NULL;
				}
				speculation_barrier();
			}
			break;

			case YCSB_READ_LATEST: {
				total_read_latest++;
				blob_t* target_key = (blob_t*) next_read_latest_key[read_latest_from_thread];

				// Advancing next_read_latest_key must be done before checking whether to
				// move to another block (by comparing inserts_done). Otherwise, in the
				// single-threaded case, we'll advance next_read_latest_key[0] after it was
				// set to the block start, and by an incorrect amount.
				if (target_key->size != 0xFFFFFFFFU)
					next_read_latest_key[read_latest_from_thread] += sizeof(blob_t) + target_key->size;

				read_latest_from_thread++;
				if (read_latest_from_thread == ctx->num_threads)
					read_latest_from_thread = 0;

				// From here onwards, read_latest_from_thread is the thread from which we'll read
				// in the /next/ READ_LATEST op, not the current one.
				inserter = &(ctx->thread_contexts[read_latest_from_thread]);
				uint64_t inserts_done = __atomic_load_n(&(inserter->inserts_done), __ATOMIC_RELAXED);
				if (inserts_done != last_inserts_done[read_latest_from_thread]) {
					last_inserts_done[read_latest_from_thread] = inserts_done;

					uint8_t* block_start = thread_read_latest_blocks[read_latest_from_thread][inserts_done];
					next_read_latest_key[read_latest_from_thread] = block_start;
					__builtin_prefetch(&(thread_read_latest_blocks[read_latest_from_thread][inserts_done+8]));
				}
				__builtin_prefetch(next_read_latest_key[read_latest_from_thread]);

				if (target_key->size == 0xFFFFFFFFU) {
					// Reached end-of-block sentinel
					failed_read_latest++;
					break;
				}

				key.len = target_key->size;
				key.data = target_key->bytes;
				TID result = ctx->tree->lookup(key, thread_info);
				if (result == 0) {
					printf("Error: a key was not found\n");
					return NULL;
				}
				speculation_barrier();
			}
			break;

			case YCSB_INSERT: {
				kv_t* kv = (kv_t*) (ctx->workload.data_buf + op->data_pos);
				load_key((TID) kv, key);
				ctx->tree->insert(key, (TID) kv, thread_info);

				// Use atomic_store to make sure that the write isn't reordered with ct_insert,
				// and eventually becomes visible to other threads.
				__atomic_store_n(&(ctx->inserts_done), ctx->inserts_done + 1, __ATOMIC_RELEASE);
				speculation_barrier();
			}
			break;

			case YCSB_RMW: {
				kv_t* kv = (kv_t*) (ctx->workload.data_buf + op->data_pos);
				key.len = kv->key_size;
				key.data = kv->kv;

				// Find existing value
				TID result = ctx->tree->lookup(key, thread_info);
				if (result == 0) {
					printf("Error: a key was not found\n");
					return NULL;
				}

				// Insert the new value
				ctx->tree->insert(key, (TID) kv, thread_info);
				speculation_barrier();
			}
			break;

			case YCSB_UPDATE: {
				kv_t* kv = (kv_t*) (ctx->workload.data_buf + op->data_pos);
				load_key((TID) kv, key);
				ctx->tree->insert(key, (TID) kv, thread_info);
				speculation_barrier();
			}
			break;

			case YCSB_SCAN: {
				uint64_t range_size = (rand_dword_r(&rand_state) % 100) + 1;
				blob_t* start_key = (blob_t*) (ctx->workload.data_buf + op->data_pos);

				key.len = start_key->size;
				key.data = start_key->bytes;
				ctx->tree->lookupRange(key, max_key, dummy, range_results, range_size,
									   num_range_results, thread_info);

				// Use the results of the range lookup s.t. it won't be optimized out
				uintptr_t checksum = 0;
				for (j = 0; j < num_range_results; j++)
					checksum += range_results[j];

				if (checksum == ((uint64_t)-1ULL))
					printf("Impossible!\n");

				speculation_barrier();
			}
			break;

			default:
				printf("Error: unknown YCSB op type\n");
				return NULL;
		}
	}
	return NULL;
}

void test_ycsb(dataset_t* dataset, const ycsb_workload_spec* spec, unsigned int num_threads) {
	uint64_t i;
	Key key;
	kv_t** kv_ptrs;
	stopwatch_t timer;
	ART_OLC::Tree tree(load_key);
	ycsb_thread_ctx thread_contexts[num_threads];
	auto thread_info = tree.getThreadInfo();

	printf("Reading dataset...\n");
	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	printf("Generating workloads...\n");
	for (i = 0; i < num_threads; i++) {
		thread_contexts[i].tree = &tree;
		thread_contexts[i].num_threads = num_threads;
		thread_contexts[i].thread_id = i;
		thread_contexts[i].inserts_done = 0;
		thread_contexts[i].thread_contexts = thread_contexts;
		generate_ycsb_workload(dataset, kv_ptrs, &(thread_contexts[i].workload), spec, i, num_threads);
	}

	printf("Inserting...\n");
	for (i = 0; i < dataset->num_keys; i++) {
		load_key((TID) kv_ptrs[i], key);
		tree.insert(key, (TID) kv_ptrs[i], thread_info);
	}

	printf("Running workloads...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	run_multiple_threads(ycsb_thread, num_threads, thread_contexts, sizeof(ycsb_thread_ctx));
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report_mt(time_took, spec->num_ops * num_threads, num_threads);
}

const flag_spec_t FLAGS[] = {
	{ "--threads", 1},
	{ "--dataset-size", 1},
	{ "--profiler-pid", 1},
	{ "--ycsb-uniform-dist", 0},
	{ NULL, 0}
};

int main(int argc, char** argv) {
	int result;
	int num_threads;
	int is_ycsb = 0;
	char* test_name;
	uint64_t dataset_size;
	dataset_t dataset;
	ycsb_workload_spec ycsb_spec;
	args_t* args = parse_args(FLAGS, argc, argv);

	if (args == NULL || args->num_args < 2) {
		printf("Usage: %s [options] test_name dataset\n", argv[0]);
		return 1;
	}

	profiler_pid = get_int_flag(args, "--profiler-pid", PID_NO_PROFILER);
	dataset_size = get_uint64_flag(args, "--dataset-size", DATASET_ALL_KEYS);
	num_threads = get_int_flag(args, "--threads", 4);

	seed_and_print();
	result = init_dataset(&dataset, args->args[1], dataset_size);

	if (!result) {
		printf("Error creating dataset.\n");
		return 1;
	}

	test_name = args->args[0];

	if (!strcmp(test_name, "insert") || !strcmp(test_name, "mt-insert")) {
		test_mt_insert(&dataset, num_threads);
		return 0;
	}

	if (!strcmp(test_name, "pos-lookup") || !strcmp(test_name, "mt-pos-lookup")) {
		test_mt_pos_lookup(&dataset, num_threads);
		return 0;
	}

	if (!strcmp(test_name, "mem-usage")) {
		test_mem_usage(&dataset);
		return 0;
	}

	if (!strcmp(test_name, "ycsb-a") || !strcmp(test_name, "mt-ycsb-a")) {
		ycsb_spec = YCSB_A_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-b") || !strcmp(test_name, "mt-ycsb-b")) {
		ycsb_spec = YCSB_B_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-c") || !strcmp(test_name, "mt-ycsb-c")) {
		ycsb_spec = YCSB_C_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-d") || !strcmp(test_name, "mt-ycsb-d")) {
		ycsb_spec = YCSB_D_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-e") || !strcmp(test_name, "mt-ycsb-e")) {
		ycsb_spec = YCSB_E_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-f") || !strcmp(test_name, "mt-ycsb-f")) {
		ycsb_spec = YCSB_F_SPEC;
		is_ycsb = 1;
	}

	if (is_ycsb) {
		if (has_flag(args, "--ycsb-uniform-dist"))
			ycsb_spec.distribution = DIST_UNIFORM;

		test_ycsb(&dataset, &ycsb_spec, num_threads);
		return 0;
	}

	printf("Unknown test name '%s'\n", test_name);
	return 1;
}