#define _GNU_SOURCE  // For <sched.h>

#include <sys/mman.h>
#include <stdio.h>
#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <pthread.h>
#include <sched.h>
#include "util.h"

#define HUGEPAGE_LOG_SIZE 21

#define DATASET_DEFAULT_SIZE 10000000

#define PROC_STAT_FILENAME "/proc/self/stat"

uint64_t rand_state;

void* mmap_hugepage(size_t size) {
	return mmap(0, size, PROT_READ | PROT_WRITE,
				MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | (HUGEPAGE_LOG_SIZE << MAP_HUGE_SHIFT),
				-1, 0);
}

// Align <value> up to a multiple of <n>. When <n> is a power of 2,
// the modulo operation will be compiled to a bitwise AND.
static inline uint64_t align_up(uint64_t value, uint64_t n) {
	return value + (n-1) - (value-1) % n;
}

float time_diff(struct timespec* end, struct timespec* start) {
	float time_diff = end->tv_sec - start->tv_sec;
	time_diff += (1.0 / 1000000000) * (end->tv_nsec - start->tv_nsec);
	return time_diff;
}

void stopwatch_start(stopwatch_t* stopwatch) {
	clock_gettime(CLOCK_MONOTONIC, &(stopwatch->start_time));
}

float stopwatch_value(stopwatch_t* stopwatch) {
	struct timespec end_time;
	clock_gettime(CLOCK_MONOTONIC, &end_time);
	return time_diff(&end_time, &(stopwatch->start_time));
}

void random_dataset_read_key(dataset_t* dataset, ct_key* buf) {
	int i;
	int key_len = (uintptr_t)(dataset->context);
	random_bytes(buf->bytes, key_len);
	buf->size = key_len;

	// HOT requires c-string keys. Replace zeros inside key with something else.
	for (i = 0;i < key_len;i++) {
		if (buf->bytes[i] == 0)
			buf->bytes[i] = (i % 16) + 1;
	}
}

void random_dataset_close(dataset_t* dataset) {
	// Do nothing
}

int init_random_dataset(dataset_t* dataset, uint64_t num_keys, int key_len) {
	if (num_keys == DATASET_ALL_KEYS)
		num_keys = DATASET_DEFAULT_SIZE;

	if (key_len > MAX_KEY_SIZE) {
		printf("Error: random key too long\n");
		return 0;
	}

	dataset->num_keys = num_keys;
	dataset->total_size = num_keys * key_len;
	dataset->read_key = random_dataset_read_key;
	dataset->close = random_dataset_close;
	dataset->context = (void*)(uintptr_t)key_len;

	return 1;
}

void file_dataset_read_key(dataset_t* dataset, ct_key* buf) {
	int items_read;
	FILE* dataset_file = (FILE*)(dataset->context);

	items_read = fread(&(buf->size), sizeof(buf->size), 1, dataset_file);
	if (items_read != 1) {
		printf("Error reading key\n");
		return;
	}

	if (buf->size > MAX_KEY_SIZE) {
		printf("Error: key too large\n");
		return;
	}

	items_read = fread(buf->bytes, buf->size, 1, dataset_file);
	if (items_read != 1) {
		printf("Error reading key\n");
		return;
	}
}

void file_dataset_close(dataset_t* dataset) {
	fclose((FILE*)(dataset->context));
}

// TODO: Support random datasets with random key lengths
// TODO: Support random datasets that guarantee unique keys
int init_dataset(dataset_t* dataset, const char* name, uint64_t keys_requested) {
	int items_read;
	FILE* dataset_file;

	if (!strncmp(name, "rand-", 5)) {
		return init_random_dataset(dataset, keys_requested, atoi(name + 5));
	}

	dataset_file = fopen(name, "rb");
	if (!dataset_file)
		return 0;

	items_read = fread(&(dataset->num_keys), sizeof(dataset->num_keys), 1, dataset_file);
	if (items_read != 1)
		goto close_and_fail;
	if (dataset->num_keys > keys_requested)
		dataset->num_keys = keys_requested;

	items_read = fread(&(dataset->total_size), sizeof(dataset->total_size), 1, dataset_file);
	if (items_read != 1)
		goto close_and_fail;

	dataset->read_key = file_dataset_read_key;
	dataset->close = file_dataset_close;
	dataset->context = dataset_file;
	return 1;

	close_and_fail:
	fclose(dataset_file);
	return 0;
}

ct_key* read_dataset(dataset_t* dataset) {
	const int key_align = 8;
	uint64_t i;
	uint8_t* buf;
	uint8_t* buf_pos;
	ct_key* keys;

	buf = (uint8_t*) malloc(dataset->total_size + dataset->num_keys * (key_align - 1));
	keys = (ct_key*) malloc(dataset->num_keys * sizeof(ct_key));

	buf_pos = buf;
	for (i = 0;i < dataset->num_keys;i++) {
		ct_key* key = &(keys[i]);
		key->bytes = buf_pos;
		dataset->read_key(dataset, key);

		buf_pos += key->size;
		buf_pos = (uint8_t*)align_up((uintptr_t)buf_pos, key_align);
	}

	return keys;
}

// Read all keys from the database into ct_key structures. Add a trailing zero
// after the bytes of each key.
ct_key* read_string_dataset(dataset_t* dataset) {
	const int key_align = 8;
	uint64_t i;
	uint8_t* buf;
	uint8_t* buf_pos;
	ct_key* keys;

	uint64_t keys_and_null_terminators_size = dataset->total_size + dataset->num_keys;
	buf = (uint8_t*) malloc(keys_and_null_terminators_size + dataset->num_keys * (key_align - 1));
	keys = (ct_key*) malloc(dataset->num_keys * sizeof(ct_key));

	buf_pos = buf;
	for (i = 0;i < dataset->num_keys;i++) {
		ct_key* key = &(keys[i]);
		key->bytes = buf_pos;
		dataset->read_key(dataset, key);
		key->bytes[key->size] = '\0';

		buf_pos += key->size + 1;
		buf_pos = (uint8_t*)align_up((uintptr_t)buf_pos, key_align);
	}

	return keys;
}

string_kv** create_string_kvs(dataset_t* dataset) {
	uint64_t i;
	uint64_t kv_size;
	uint64_t kv_pos;
	string_kv* kv;
	ct_key key;
	dynamic_buffer_t kvs_buf;
	uint8_t* key_buf = (uint8_t*) malloc(MAX_KEY_SIZE);
	uint8_t** kv_pointers = (uint8_t**) malloc(sizeof(string_kv*) * dataset->num_keys);
	key.bytes = key_buf;

	dynamic_buffer_init(&kvs_buf);
	for (i = 0;i < dataset->num_keys;i++) {
		dataset->read_key(dataset, &key);
		kv_size = sizeof(string_kv) + key.size + 1;  // 1 for the terminating NULL
		kv_pos = dynamic_buffer_extend(&kvs_buf, kv_size);

		kv = (string_kv*) (kvs_buf.ptr + kv_pos);
		kv->value = 0x12345678;
		memcpy(kv->key, key.bytes, key.size);
		kv->key[key.size] = 0 ;

		kv_pointers[i] = (uint8_t*) kv_pos;
	}

	for (i = 0;i < dataset->num_keys;i++)
		kv_pointers[i] += (uintptr_t) (kvs_buf.ptr);

	return (string_kv**) kv_pointers;
}

// Return one long buffer containing all keys in the dataset separated by zero bytes
uint8_t* serialize_dataset(dataset_t* dataset) {
	uint64_t i;
	ct_key key_buf;
	uint8_t* buf = (uint8_t*) malloc(dataset->total_size + dataset->num_keys);
	uint8_t* buf_pos = buf;

	for (i = 0;i < dataset->num_keys;i++) {
		key_buf.bytes = buf_pos;
		dataset->read_key(dataset, &key_buf);

		// Make sure the key doesn't contain NULL
		if (memchr(key_buf.bytes, 0, key_buf.size) != NULL) {
			free(buf);
			return 0;
		}

		key_buf.bytes[key_buf.size] = '\0';

		buf_pos += key_buf.size + 1;
	}

	return buf;
}

// Return value is in bytes
uint64_t virt_mem_usage() {
	uint64_t result;
	int items_read;
	FILE* proc_stat = fopen(PROC_STAT_FILENAME, "rb");
	if (!proc_stat)
		return 0;

	// Ignore 22 values and parse the 23rd as integer
	items_read = fscanf(proc_stat, "%*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %*s %lu",
			&result);
	if (items_read != 1)
		return 0;

	fclose(proc_stat);
	return result;
}

int get_int_flag(args_t* args, const char* name, unsigned int def_value) {
	int i;
	for (i = 0;i < args->num_flags; i++) {
		if (!strcmp(args->flags[i].name, name))
			return atoi(args->flags[i].value);
	}
	return def_value;
}

uint64_t get_uint64_flag(args_t* args, const char* name, uint64_t def_value) {
	int i;
	for (i = 0;i < args->num_flags; i++) {
		if (!strcmp(args->flags[i].name, name))
			return strtoull(args->flags[i].value, NULL, 0);
	}
	return def_value;
}

int has_flag(args_t* args, const char* name) {
	int i;
	for (i = 0;i < args->num_flags; i++) {
		if (!strcmp(args->flags[i].name, name))
			return 1;
	}
	return 0;
}

args_t* parse_args(const flag_spec_t* flags, int argc, char** argv) {
	args_t* args;
	int i;
	const flag_spec_t* flag;
	if (argc > MAX_ARGS) {
		return NULL;
	}

	args = (args_t*) malloc(sizeof(args_t));
	args->num_flags = 0;
	args->num_args = 0;
	for (i = 1;i < argc;i++) {
		flag = flags;
		while (flag->name != NULL) {
			if (strcmp(flag->name, argv[i])) {
				flag++;
				continue;
			}

			// Found a known flag
			args->flags[args->num_flags].name = argv[i];
			if (flag->has_value) {
				i++;
				if (i == argc)
					return NULL;
				args->flags[args->num_flags].value = argv[i];
			}
			args->num_flags++;
			break;
		}
		if (flag->name == NULL) {
			// argv[i] is not a known flag, so it is an argument
			args->args[args->num_args++] = argv[i];
		}
	}
	return args;
}

void dynamic_buffer_init(dynamic_buffer_t* buf) {
	buf->size = 1024;
	buf->ptr = (uint8_t*) malloc(buf->size);
	buf->pos = 0;
}

uint64_t dynamic_buffer_extend(dynamic_buffer_t* buf, uint64_t data_size) {
	uint64_t old_pos = buf->pos;
	if (buf->pos + data_size > buf->size) {
		buf->size = buf->size * 2 + data_size;
		buf->ptr = (uint8_t*) realloc(buf->ptr, buf->size);
	}
	buf->pos += data_size;
	return old_pos;
}

int choose_ycsb_op_type(const float* op_probs) {
	uint64_t i;
	float sum = 0.0;
	float rand = rand_float();
	for (i = 0;i < YCSB_NUM_OP_TYPES;i++) {
		sum += op_probs[i];
		if (sum > 1.00001) {
			printf("Error: Inconsistent YCSB probabilities\n");
			return -1;
		}

		// <rand> can be exactly 1.0 so we use "<="
		if (rand <= sum)
			return i;
	}
	printf("Error: Inconsistent YCSB probabilities\n");
	return -1;
}

uint64_t spec_read_latest_block_size(const ycsb_workload_spec* spec, int num_threads) {
	uint64_t result;
	if (spec->op_type_probs[YCSB_READ_LATEST] == 0)
		return 0;

	double inserts_per_thread = spec->op_type_probs[YCSB_INSERT] * spec->num_ops;
	double read_latest_per_thread = spec->op_type_probs[YCSB_READ_LATEST] * spec->num_ops;
	result = read_latest_per_thread / (inserts_per_thread + 1);
	result = result / num_threads + 1;
	result *= 5;

	return result;
}

#define ZIPF_ERROR_RATIO 1.01

double rand_double() {
	return ((double)rand_uint64()) / UINT64_MAX;
}

void rand_uniform_init(rand_distribution* dist, uint64_t max) {
	dist->max = max;
	dist->type = DIST_UNIFORM;
}

rand_distribution zipf_dist_cache;

void rand_zipf_init(rand_distribution* dist, uint64_t max, double skew) {
	uint64_t i;
	double total_weight = 0.0;
	uint64_t range_start = 0;
	uint64_t range_end;
	uint64_t range_num = 0;

	if (zipf_dist_cache.max == max && zipf_dist_cache.skew == skew) {
		*dist = zipf_dist_cache;
		return;
	}

	// A multiplier M s.t. the ratio between the weights of the k'th element
	// and the (k*M)'th element is at most ZIPF_ERROR_RATIO
	double range_size_multiplier = pow(ZIPF_ERROR_RATIO, 1.0 / skew);

	while (range_start < max) {
		zipf_range* range = &(dist->zipf_ranges[range_num]);
		range->start = range_start;
		range_end = (uint64_t) floor((range->start + 1) * range_size_multiplier);
		range->size = range_end - range->start;
		if (range->start + range->size > max)
			range->size = max - range->start;
		for (i = 0;i < range->size;i++)
			total_weight += 1.0 / pow(i + range->start + 1, skew);

		range->weight_cumsum = total_weight;

		// Compute start point of the next range
		range_start = range->start + range->size;
		range_num++;
	}

	dist->num_zipf_ranges = range_num;
	dist->total_weight = total_weight;
	dist->max = max;
	dist->type = DIST_ZIPF;
	dist->skew = skew;

	zipf_dist_cache = *dist;
}

void rand_zipf_rank_init(rand_distribution* dist, uint64_t max, double skew) {
	rand_zipf_init(dist, max, skew);
	dist->type = DIST_ZIPF_RANK;
}

uint64_t mix(uint64_t x) {
	x ^= x >> 33;
	x *= 0xC2B2AE3D27D4EB4FULL;  // Random prime
	x ^= x >> 29;
	x *= 0x165667B19E3779F9ULL;  // Random prime
	x ^= x >> 32;
	return x;
}

uint64_t rand_dist(rand_distribution* dist) {
	uint64_t low, high;
	uint64_t range_num;

	if (dist->type == DIST_UNIFORM)
		return rand_uint64() % dist->max;

	// Generate Zipf-distributed random
	double x = rand_double() * dist->total_weight;

	// Find which range contains x
	low = 0;
	high = dist->num_zipf_ranges;
	while (1) {
		if (high - low <= 1) {
			range_num = low;
			break;
		}
		uint64_t mid = (low + high) / 2 - 1;
		if (x < dist->zipf_ranges[mid].weight_cumsum) {
			high = mid + 1;
		} else {
			low = mid + 1;
		}
	}

	// This range contains x. Choose a random value in the range.
	zipf_range* range = &(dist->zipf_ranges[range_num]);
	uint64_t zipf_rand = (rand_uint64() % range->size) + range->start;

	if (dist->type == DIST_ZIPF) {
		// Permute the output. Otherwise, all common values will be near one another
		assert(dist->max > 1000);  // When <max> is small, collisions change the distribution considerably.
		return mix(zipf_rand) % dist->max;
	} else {
		assert(dist->type == DIST_ZIPF_RANK);
		return zipf_rand;
	}
}

typedef struct {
	void* (*thread_func)(void*);
	void* arg;
	int cpu;
} run_with_affinity_ctx;

void* run_with_affinity(void* arg) {
	run_with_affinity_ctx* ctx = (run_with_affinity_ctx*) arg;
	cpu_set_t cpu_set;

	CPU_ZERO(&cpu_set);
	CPU_SET(ctx->cpu, &cpu_set);
	sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set);
	return ctx->thread_func(ctx->arg);
}

int run_multiple_threads(void* (*thread_func)(void*), int num_threads, void* thread_contexts, int context_size) {
	uint64_t i;
	int result;
	int cpu = 0;
	run_with_affinity_ctx wrapper_contexts[num_threads];
	pthread_t threads[num_threads];
	cpu_set_t mask;

	sched_getaffinity(0, sizeof(cpu_set_t), &mask);

	for (i = 0;i < num_threads;i++) {
		run_with_affinity_ctx* wrapper_ctx = &(wrapper_contexts[i]);

		// Find next allowed CPU
		while (!CPU_ISSET(cpu, &mask)) {
			cpu++;
			if (cpu == CPU_SETSIZE) {
				printf("Not enough CPUs for all threads\n");
				return 0;
			}
		}

		wrapper_ctx->thread_func = thread_func;
		wrapper_ctx->arg = thread_contexts + context_size * i;
		wrapper_ctx->cpu = cpu;
		result = pthread_create(&(threads[i]), NULL, run_with_affinity, wrapper_ctx);
		if (result != 0) {
			printf("Thread creation error\n");
			return 0;
		}

		// Run the next thread on another CPU
		cpu++;
	}

	for (i = 0; i < num_threads; i++) {
		result = pthread_join(threads[i], NULL);
		if (result != 0) {
			printf("Thread join error\n");
			return 0;
		}
	}
	return 1;
}

void report_mt(float duration, uint64_t num_ops, int num_threads) {
	printf("Took %.2fs for %lu ops in %d threads (%.0fns/op, %.2fMops/s per thread)\n",
		   duration, num_ops, num_threads,
		   (duration / num_ops * num_threads) * 1.0e9,
		   (num_ops / duration / num_threads) / 1.0e6);

	printf("RESULT: ops=%lu threads=%d ms=%d\n", num_ops, num_threads, (int)(duration*1000));
}

void report(float duration, uint64_t num_ops) {
	printf("Took %.2fs for %lu ops (%.0fns/op)\n", duration, num_ops, duration / num_ops * 1.0e9);
	printf("RESULT: ops=%lu ms=%d\n", num_ops, (int)(duration * 1000));
}