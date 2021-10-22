#include <stdio.h>
#include <string.h>
#include <signal.h>

#include <tlx/container/btree.hpp>
#include "util.h"

#define DEFAULT_VALUE_SIZE 8

#define PID_NO_PROFILER 0

typedef struct {
	uint32_t key_size;
	uint32_t value_size;
	uint8_t kv[];
} kv_t;

// A function to extract the key from a key+value object. Required by the STX BTree.
// We use kv_t* for both keys and values, so this is the identity function.
struct key_of_value {
	static kv_t* const& get(kv_t* const& value) {
		return value;
	}
};

struct kv_compare {
	bool operator()(const kv_t* const& lhs, const kv_t* const& rhs) const {
		int cmp;
		uint32_t min_size;
		if (lhs->key_size < rhs->key_size)
			min_size = lhs->key_size;
		else
			min_size = rhs->key_size;

		cmp = memcmp(lhs->kv, rhs->kv, min_size);

		if (cmp < 0)
			return true;

		if (cmp == 0 && lhs->key_size < rhs->key_size)
			return true;  // lhs is a strict prefix of rhs

		return false;
	}
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

typedef tlx::BTree<kv_t*, kv_t*, struct key_of_value, struct kv_compare> kv_btree;

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

		kv->key_size = key.size;
		kv->value_size = value_size;
		memcpy(kv->kv, key.bytes, key.size);

		memset(kv->kv + kv->key_size, 0xAB, kv->value_size);
		kv_ptrs[i] = pos;
	}

	for (i = 0;i < dataset->num_keys;i++)
		kv_ptrs[i] += (uintptr_t)(kvs_buf.ptr);

	return (kv_t**) kv_ptrs;
}

void test_insert(dataset_t* dataset) {
	uint64_t i;
	kv_t** kv_ptrs;
	stopwatch_t timer;
	kv_btree tree;

	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	notify_critical_section_start();
	stopwatch_start(&timer);
	for (i = 0; i < dataset->num_keys; i++)
		tree.insert(kv_ptrs[i]);
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report(time_took, dataset->num_keys);
}

void test_pos_lookup(dataset_t* dataset) {
	const uint64_t num_lookups = 10 * MILLION;
	uint64_t i;
	uint8_t* buf_pos;
	uint64_t data_size, data_offset;
	stopwatch_t timer;
	kv_t** kv_ptrs;
	kv_btree tree;
	dynamic_buffer_t workload_buf;

	printf("Reading dataset...\n");
	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	printf("Inserting...\n");
	for (i = 0; i < dataset->num_keys; i++)
		tree.insert(kv_ptrs[i]);

	dynamic_buffer_init(&workload_buf);
	for (i = 0; i < num_lookups; i++) {
		kv_t* kv = kv_ptrs[rand_uint64() % dataset->num_keys];
		data_size = sizeof(kv_t) + kv->key_size;
		data_offset = dynamic_buffer_extend(&workload_buf, data_size);

		kv_t* workload_kv = (kv_t*) (workload_buf.ptr + data_offset);
		workload_kv->key_size = kv->key_size;
		memcpy(workload_kv->kv, kv->kv, kv->key_size);
	}

	printf("Performing lookups...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	buf_pos = workload_buf.ptr;
	for (i = 0; i < num_lookups; i++) {
		kv_t* key = (kv_t*) buf_pos;
		kv_btree::iterator result = tree.find(key);
		if (result == tree.end()) {
			printf("Error: a key that was inserted was not found!\n");
			return;
		}
		buf_pos += sizeof(kv_t) + key->key_size;
	}
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report(time_took, num_lookups);
}

void test_mem_usage(dataset_t* dataset) {
	uint64_t i;
	uint64_t start_mem, end_mem;
	kv_t** kv_ptrs;
	kv_btree tree;

	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	start_mem = virt_mem_usage();
	for (i = 0; i < dataset->num_keys; i++)
		tree.insert(kv_ptrs[i]);
	end_mem = virt_mem_usage();

	uint64_t index_overhead = end_mem - start_mem;
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
							const ycsb_workload_spec* spec) {
	kv_t* kv;
	uint64_t i;
	uint64_t data_size;
	uint64_t total_inserts;
	uint64_t insert_offset;
	dynamic_buffer_t workload_buf;
	rand_distribution dist;
	rand_distribution backward_dist;

	workload->ops = (ycsb_op*) malloc(sizeof(ycsb_op) * spec->num_ops);
	workload->num_ops = spec->num_ops;

	total_inserts = spec->op_type_probs[YCSB_INSERT] * spec->num_ops;
	workload->initial_num_keys = dataset->num_keys - total_inserts;
	insert_offset = workload->initial_num_keys;

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

		if (insert_offset == dataset->num_keys && op->type == YCSB_INSERT) {
			// Used all keys intended for insertion. Do another op type.
			i--;
			continue;
		}

		switch (op->type) {
			case YCSB_SCAN:
			case YCSB_READ:{
				kv = kv_ptrs[rand_dist(&dist)];
				data_size = sizeof(kv_t) + kv->key_size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);
				kv_t* target_key = (kv_t*)(workload_buf.ptr + op->data_pos);
				target_key->key_size = kv->key_size;
				memcpy(target_key->kv, kv->kv, kv->key_size);
			}
			break;

			case YCSB_READ_LATEST:{
				op->type = YCSB_READ;  // READ and READ_LATEST are executed the same way
				uint64_t backwards = rand_dist(&backward_dist);
				kv = kv_ptrs[insert_offset - 1 - backwards];
				data_size = sizeof(kv_t) + kv->key_size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);
				kv_t* target_key = (kv_t*)(workload_buf.ptr + op->data_pos);
				target_key->key_size = kv->key_size;
				memcpy(target_key->kv, kv->kv, kv->key_size);
			}
			break;

			case YCSB_RMW:
			case YCSB_UPDATE:{
				kv = kv_ptrs[rand_dist(&dist)];
				data_size = sizeof(kv_t) + kv->key_size + DEFAULT_VALUE_SIZE;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				kv_t* target_key = (kv_t*)(workload_buf.ptr + op->data_pos);
				target_key->key_size = kv->key_size;
				target_key->value_size = DEFAULT_VALUE_SIZE;
				memcpy(target_key->kv, kv->kv, kv->key_size);
				memset(target_key->kv + target_key->key_size, 7, DEFAULT_VALUE_SIZE);
			}
			break;

			case YCSB_INSERT:{
				kv = kv_ptrs[insert_offset];
				insert_offset++;
				data_size = sizeof(kv_t) + kv->key_size + DEFAULT_VALUE_SIZE;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				kv_t* target_key = (kv_t*)(workload_buf.ptr + op->data_pos);
				target_key->key_size = kv->key_size;
				target_key->value_size = DEFAULT_VALUE_SIZE;
				memcpy(target_key->kv, kv->kv, kv->key_size);
				memset(target_key->kv + target_key->key_size, 7, DEFAULT_VALUE_SIZE);
			}
			break;

			default:
				printf("Error: Unknown YCSB op type\n");
				return;

		}
	}

	workload->data_buf = workload_buf.ptr;
}

void execute_ycsb_workload(kv_btree* tree, ycsb_workload* workload) {
	uint64_t i, j;

	for (i = 0; i < workload->num_ops; i++) {
		ycsb_op* op = &(workload->ops[i]);
		switch (op->type) {
			case YCSB_READ:{
				kv_t* key = (kv_t*) (workload->data_buf + op->data_pos);
				kv_btree::iterator result = tree->find(key);
				if (result == tree->end()) {
					printf("Error: a key that was inserted was not found!\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_INSERT:{
				kv_t* key = (kv_t*) (workload->data_buf + op->data_pos);
				tree->insert(key);
				speculation_barrier();
			}
			break;

			case YCSB_UPDATE:{
				kv_t* key = (kv_t*) (workload->data_buf + op->data_pos);
				kv_btree::iterator result = tree->find(key);
				if (result == tree->end()) {
					printf("Error: a key was not found!\n");
					return;
				}
				kv_t* kv = *result;

				// We assume that the update doesn't change the value size, so it can be performed in-place
				assert(key->value_size == kv->value_size);

				memcpy(kv->kv + kv->key_size, key->kv + key->key_size, key->value_size);
				speculation_barrier();
			}
			break;

			case YCSB_RMW:{
				kv_t* kv = (kv_t*) (workload->data_buf + op->data_pos);

				// Read key
				kv_btree::iterator result = tree->find(kv);
				if (result == tree->end()) {
					printf("Error: a key that was inserted was not found!\n");
					return;
				}

				// Update key
				auto insert_result = tree->insert(kv);
				if (insert_result.second) {
					printf("Error: a new key was inserted instead of an existing key being updated!\n");
					return;
				}
				speculation_barrier();
			}
			break;

			case YCSB_SCAN:{
				kv_t* key = (kv_t*) (workload->data_buf + op->data_pos);
				uint64_t range_size = (rand_dword() % 100) + 1;

				auto it = tree->upper_bound(key);
				auto end = tree->end();
				uint64_t checksum;
				for (j = 0; j < range_size; j++) {
					++it;

					if (it == end)
						break;

					kv_t* key_ptr = *it;
					checksum += (uint64_t) key_ptr;
				}
				if (checksum == 0xFFFFFFFFFFFF)
					printf("Impossible!\n");
				speculation_barrier();
			}
			break;

			default:
				printf("Error: Unknown YCSB op\n");
				return;
		}
	}
}

void test_ycsb(dataset_t* dataset, ycsb_workload_spec* spec) {
	uint64_t i;
	kv_btree tree;
	kv_t** kv_ptrs;
	stopwatch_t timer;
	ycsb_workload workload;

	printf("Reading dataset...\n");
	kv_ptrs = read_kvs(dataset, DEFAULT_VALUE_SIZE);

	printf("Creating workload...\n");
	generate_ycsb_workload(dataset, kv_ptrs, &workload, spec);

	for (i = 0; i < workload.initial_num_keys; i++)
		tree.insert(kv_ptrs[i]);

	printf("Running...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	execute_ycsb_workload(&tree, &workload);
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report(time_took, spec->num_ops);
}

const flag_spec_t FLAGS[] = {
	{ "--dataset-size", 1},
	{ "--profiler-pid", 1},
	{ "--ycsb-uniform-dist", 0},
	{ NULL, 0}
};

int main(int argc, char** argv) {
	int result;
	dataset_t dataset;
	uint64_t dataset_size;
	ycsb_workload_spec ycsb_spec;
	int is_ycsb = 0;

	args_t* args = parse_args(FLAGS, argc, argv);

	if (args == NULL || args->num_args != 2) {
		printf("Usage: %s [options] test-name dataset\n", argv[0]);
		return 1;
	}

	profiler_pid = get_int_flag(args, "--profiler-pid", PID_NO_PROFILER);
	dataset_size = get_uint64_flag(args, "--dataset-size", DATASET_ALL_KEYS);
	result = init_dataset(&dataset, args->args[1], dataset_size);

	if (!strcmp(args->args[0], "insert")) {
		test_insert(&dataset);
		return 0;
	}

	if (!strcmp(args->args[0], "pos-lookup")) {
		test_pos_lookup(&dataset);
		return 0;
	}

	if (!strcmp(args->args[0], "mem-usage")) {
		test_mem_usage(&dataset);
		return 0;
	}

	if (!strcmp(args->args[0], "ycsb-a")) {
		is_ycsb = 1;
		ycsb_spec = YCSB_A_SPEC;
	}

	if (!strcmp(args->args[0], "ycsb-b")) {
		is_ycsb = 1;
		ycsb_spec = YCSB_B_SPEC;
	}

	if (!strcmp(args->args[0], "ycsb-c")) {
		is_ycsb = 1;
		ycsb_spec = YCSB_C_SPEC;
	}

	if (!strcmp(args->args[0], "ycsb-d")) {
		is_ycsb = 1;
		ycsb_spec = YCSB_D_SPEC;
	}

	if (!strcmp(args->args[0], "ycsb-e")) {
		is_ycsb = 1;
		ycsb_spec = YCSB_E_SPEC;
	}

	if (!strcmp(args->args[0], "ycsb-f")) {
		is_ycsb = 1;
		ycsb_spec = YCSB_F_SPEC;
	}

	if (is_ycsb) {
		if (has_flag(args, "--ycsb-uniform-dist"))
			ycsb_spec.distribution = DIST_UNIFORM;
		test_ycsb(&dataset, &ycsb_spec);
		return 0;
	}

	printf("Unknown test name '%s'\n", args->args[0]);
	return 1;
}
