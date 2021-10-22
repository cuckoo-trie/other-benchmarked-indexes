#ifndef _INCLUDE_UTIL_H_
#define _INCLUDE_UTIL_H_

#include <stdio.h>
#include <stdint.h>
#include <sys/time.h>
#include <assert.h>
#include <time.h>

#define MILLION 1000000

#define DATASET_ALL_KEYS 0xFFFFFFFFFFFFFFFFULL
#define MAX_KEY_SIZE 4096
#define MAX_ARGS 64
#define MAX_THREADS 64

#define YCSB_READ 0
#define YCSB_READ_LATEST 1
#define YCSB_UPDATE 2
#define YCSB_INSERT 3
#define YCSB_SCAN 4
#define YCSB_RMW 5
#define YCSB_NUM_OP_TYPES 6

#define YCSB_SKEW 0.99

#define MAX_ZIPF_RANGES 10000

// Sample a number in [0,max) with all numbers having equal probability.
#define DIST_UNIFORM 0

// Sample a number in [0,max) with Zipf probabilities: the k'th most
// common number has probability proportional to 1 / (k**skew).
#define DIST_ZIPF 1

// Same as DIST_ZIPF, but with 0 being the most common number, 1 the
// second most common, and so on.
#define DIST_ZIPF_RANK 2

extern uint64_t rand_state;

typedef struct {
	uint8_t* bytes;
	int size;
} ct_key;

typedef struct {
	uint32_t size;
	uint8_t bytes[];
} blob_t;

typedef struct {
	uint64_t value;
	char key[];
} string_kv;

typedef struct {
    struct timespec start_time;
} stopwatch_t;

typedef struct {
	// The weight of all ranges up to and including this one
	double weight_cumsum;

	uint64_t start;
	uint64_t size;
} zipf_range;

typedef struct {
	zipf_range zipf_ranges[MAX_ZIPF_RANGES];
	uint64_t num_zipf_ranges;
	double total_weight;
	double skew;
	uint64_t max;
	int type;
} rand_distribution;

typedef struct {
	int type;
	uint64_t data_pos;
} ycsb_op;

typedef struct {
	uint64_t initial_num_keys;
	uint64_t num_ops;
	ycsb_op* ops;
	uint8_t* data_buf;

	// For each thread, a pointer to an array of block-pointers. In that array,
	// the K-th block contains keys with a distribution assuming that K of the inserts
	// in this workload were done.
	uint8_t** read_latest_blocks_for_thread[MAX_THREADS];
} ycsb_workload;

typedef struct {
	float op_type_probs[YCSB_NUM_OP_TYPES];
	uint64_t num_ops;
	int distribution;
} ycsb_workload_spec;

typedef struct {
	uint8_t* ptr;
	uint64_t size;
	uint64_t pos;
} dynamic_buffer_t;

typedef struct dataset_t_struct {
	uint64_t num_keys;
	uint64_t total_size;  // The total length of all keys
	void (*read_key)(struct dataset_t_struct* dataset, ct_key* buffer);
	void (*close)(struct dataset_t_struct* dataset);
	void* context;
} dataset_t;

typedef struct {
	const char* name;
	int has_value;
} flag_spec_t;

typedef struct {
	const char* name;
	char* value;
} flag_t;

typedef struct {
	int num_args;
	int num_flags;
	flag_t flags[MAX_ARGS];
	char* args[MAX_ARGS];
} args_t;

static void rand_seed(uint64_t s) {
	rand_state = s;
}

static uint32_t rand_dword() {
	rand_state = 6364136223846793005 * rand_state + 1;
	return rand_state >> 32;
}

static uint32_t rand_dword_r(uint64_t* state) {
	*state = 6364136223846793005 * (*state) + 1;
	return (*state) >> 32;
}

static uint64_t rand_uint64() {
	return (((uint64_t)rand_dword()) << 32) + rand_dword();
}

static float rand_float() {
	return ((float)rand_dword()) / UINT32_MAX;
}

static void random_bytes(uint8_t* buf, int count) {
	int i;
	for (i = 0;i < count;i++)
		buf[i] = rand_dword() % 256;
}

static long int seed_and_print() {
	struct timeval now;
	long int seed;
	gettimeofday(&now, NULL);
	seed = now.tv_sec * 1000000 + now.tv_usec;
	printf("Using seed %ld\n", seed);
	rand_seed(seed);
	return seed;
}

// Makes the CPU wait until all preceding instructions have completed
// before it starts to execute following instructions.
// Used to make sure that calls to the benchmarked index operation
// (e.g. insert) in consecutive loop iterations aren't overlapped by
// the CPU.
static inline void speculation_barrier() {
	uint32_t unused;
	__builtin_ia32_rdtscp(&unused);
	__builtin_ia32_lfence();
}

int init_dataset(dataset_t* dataset, const char* name, uint64_t keys_requested);
ct_key* read_dataset(dataset_t* dataset);
ct_key* read_string_dataset(dataset_t* dataset);
uint8_t* serialize_dataset(dataset_t* dataset);
void stopwatch_start(stopwatch_t* stopwatch);
float stopwatch_value(stopwatch_t* stopwatch);
float time_diff(struct timespec* end, struct timespec* start);
uint64_t virt_mem_usage();
args_t* parse_args(const flag_spec_t* flags, int argc, char** argv);
int has_flag(args_t* args, const char* name);
int get_int_flag(args_t* args, const char* name, unsigned int def_value);
uint64_t get_uint64_flag(args_t* args, const char* name, uint64_t def_value);
void dynamic_buffer_init(dynamic_buffer_t* buf);
uint64_t dynamic_buffer_extend(dynamic_buffer_t* buf, uint64_t data_size);
string_kv** create_string_kvs(dataset_t* dataset);
int choose_ycsb_op_type(const float* op_probs);
uint64_t spec_read_latest_block_size(const ycsb_workload_spec* spec, int num_threads);
void rand_uniform_init(rand_distribution* dist, uint64_t max);
void rand_zipf_init(rand_distribution* dist, uint64_t max, double skew);
void rand_zipf_rank_init(rand_distribution* dist, uint64_t max, double skew);
uint64_t rand_dist(rand_distribution* dist);
int run_multiple_threads(void* (*thread_func)(void*), int num_threads, void* thread_contexts, int context_size);
void report_mt(float duration, uint64_t num_ops, int num_threads);
void report(float duration, uint64_t num_ops);
#endif
