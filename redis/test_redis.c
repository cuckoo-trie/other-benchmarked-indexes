#include "hiredis.h"
#include "../util.h"
#include <pthread.h>
#include <string.h>

#define MAX_KEY_SIZE 4096

#define CACHELINE_BYTES 64

#define INSERT_BATCH_SIZE 100
#define LOOKUP_BATCH_SIZE 100
#define YCSB_PIPELINE_DEPTH 50

// In the default sorted-set (ZSET) implementation of Redis, for lexicographic-order
// scanning to work all keys have to have the same score. Use this score.
// This is unneccessary for the alternative implementations (e.g. the Cuckoo-Trie)
#define ZSET_CONST_SCORE 0

#define TRIE_SIZE_AUTO 0xFFFFFFFFFFFFFFFFULL

#define OK 0
#define ERROR 1

typedef struct {
	double value;
	uint32_t key_size;
	uint8_t key_bytes[];
} redis_kv;

ct_key* read_dataset_keys(dataset_t* dataset) {
	uint64_t i;
	uint8_t* buf_pos;
	uint8_t* key_data_buf = malloc(dataset->total_size);
	ct_key* keys = malloc(sizeof(ct_key) * dataset->num_keys);

	buf_pos = key_data_buf;
	for (i = 0;i < dataset->num_keys;i++) {
		ct_key* key = &(keys[i]);
		key->bytes = buf_pos;
		dataset->read_key(dataset, key);

		buf_pos += key->size;
	}

	return keys;
}

int set_ct_size(redisContext* redis, dataset_t* dataset, uint64_t requested_size) {
	redisReply* reply;
	uint64_t size = requested_size;

	if (size == TRIE_SIZE_AUTO)
		size = dataset->num_keys * 5 / 2;

	reply = redisCommand(redis, "CONFIG SET ct-default-size %lu", size);
	if (reply == NULL) {
		printf("Error: Couldn't set trie size (%s)\n", redis->errstr);
		return ERROR;
	}

	return OK;
}

typedef struct {
	uint64_t num_keys;
	ct_key* keys;
} insert_thread_ctx;

void* insert_thread(void* arg) {
	uint64_t i;
	redisContext* redis;
	redisReply* reply;
	uint64_t keys_done = 0;
	char* command_argv[INSERT_BATCH_SIZE * 2 + 2];
	size_t command_arglen[INSERT_BATCH_SIZE * 2 + 2];
	insert_thread_ctx* ctx = (insert_thread_ctx*) arg;

	redis = redisConnect("127.0.0.1", 6379);
	if (redis->err) {
		printf("Error: %s\n", redis->errstr);
		return NULL;
	}

	command_argv[0] = "ZADD";
	command_arglen[0] = 4;
	command_argv[1] = "test_set";
	command_arglen[1] = 8;

	while (keys_done < ctx->num_keys) {
		for (i = 0;i < INSERT_BATCH_SIZE; i++) {
			command_argv[i*2 + 2] = "0";
			command_arglen[i*2 + 2] = 1;
			command_argv[i*2 + 3] = ctx->keys[keys_done].bytes;
			command_arglen[i*2 + 3] = ctx->keys[keys_done].size;
			keys_done++;
			if (keys_done == ctx->num_keys) {
				// Update <i> as it is passed to redisCommandArgv
				i++;
				break;
			}
		}
		reply = redisCommandArgv(redis, 2 + i*2, (const char**) command_argv, command_arglen);
		if (reply == NULL) {
			printf("Insertion error: %s\n", redis->errstr);
			return NULL;
		}
		freeReplyObject(reply);
	}
	return NULL;
}

void test_insert(redisContext* redis, char* dataset_name, uint64_t requested_ct_size) {
	const unsigned int num_threads = 4;
	uint64_t i;
	ct_key* keys;
	int result;
	dataset_t dataset;
	redisReply* reply;
	stopwatch_t timer;
	insert_thread_ctx thread_contexts[num_threads];
	pthread_t threads[num_threads];

	printf("Reading dataset...\n");
	seed_and_print();
	init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	keys = read_dataset_keys(&dataset);

	result = set_ct_size(redis, &dataset, requested_ct_size);
	if (result != OK)
		return;

	reply = redisCommand(redis, "DEL test_set");
	if (reply == NULL) {
		printf("Error: Couldn't delete key (%s)\n", redis->errstr);
		return;
	}

	uint64_t start_key = 0;
	uint64_t end_key;
	for (i = 0; i < num_threads; i++) {
		insert_thread_ctx* ctx = &(thread_contexts[i]);
		end_key = dataset.num_keys * (i+1) / num_threads;
		ctx->keys = &(keys[start_key]);
		ctx->num_keys = end_key - start_key;

		start_key = end_key;
	}

	printf("Inserting with %d IO threads...\n", num_threads);
	stopwatch_start(&timer);
	for (i = 0; i < num_threads; i++) {
		result = pthread_create(&(threads[i]), NULL, insert_thread, &(thread_contexts[i]));
		if (result != 0) {
			printf("Thread creation error\n");
			return;
		}
	}
	for (i = 0; i < num_threads; i++) {
		result = pthread_join(threads[i], NULL);
		if (result != 0) {
			printf("Thread join error\n");
			return;
		}
	}
	float time_took = stopwatch_value(&timer);
	report(time_took, dataset.num_keys);
}

typedef struct {
	uint64_t num_lookups;
	uint8_t* keys_buf;
} lookup_thread_ctx;

void create_lookup_workload(ct_key* keys, uint64_t num_keys, uint64_t workload_size, lookup_thread_ctx* workload) {
	uint64_t i;
	dynamic_buffer_t workload_data;

	dynamic_buffer_init(&workload_data);

	for (i = 0; i < workload_size; i++) {
		ct_key* key = &(keys[rand_uint64() % num_keys]);
		int data_size = sizeof(blob_t) + key->size;
		uint64_t pos = dynamic_buffer_extend(&workload_data, data_size);
		blob_t* key_blob = (blob_t*) (workload_data.ptr + pos);

		key_blob->size = key->size;
		memcpy(key_blob->bytes, key->bytes, key->size);
	}

	workload->num_lookups = workload_size;
	workload->keys_buf = workload_data.ptr;
}

void* lookup_thread(void* arg) {
	lookup_thread_ctx* ctx = (lookup_thread_ctx*) arg;
	uint64_t i;
	uint64_t batch_size;
	redisReply* reply;
	redisContext* redis;
	uint64_t keys_left = ctx->num_lookups;
	uint8_t* pos = ctx->keys_buf;
	char* command_argv[LOOKUP_BATCH_SIZE + 2];
	size_t command_arglen[LOOKUP_BATCH_SIZE + 2];

	redis = redisConnect("127.0.0.1", 6379);
	if (redis->err) {
		printf("Error: %s\n", redis->errstr);
		return NULL;
	}

	command_argv[0] = "ZMSCORE";
	command_arglen[0] = 7;
	command_argv[1] = "test_set";
	command_arglen[1] = 8;

	while (keys_left > 0) {
		batch_size = LOOKUP_BATCH_SIZE;
		if (batch_size > keys_left)
			batch_size = keys_left;

		for (i = 0; i < batch_size; i++) {
			blob_t* key = (blob_t*) pos;
			command_argv[i+2] = key->bytes;
			command_arglen[i+2] = key->size;

			pos += sizeof(blob_t) + key->size;
		}

		reply = redisCommandArgv(redis, batch_size + 2, (const char**) command_argv, command_arglen);
		if (reply == NULL) {
			printf("Lookup error: %s\n", redis->errstr);
			return NULL;
		}
		if (reply->type != REDIS_REPLY_ARRAY) {
			printf("Lookup error: Unexpected reply type %d\n", reply->type);
			return NULL;
		}
		for (i = 0; i < batch_size; i++) {
			if (reply->element[i]->type == REDIS_REPLY_NIL) {
				printf("Lookup error: Key not found\n");
				return NULL;
			}
		}
		freeReplyObject(reply);

		keys_left -= batch_size;
	}
}

void test_pos_lookup(redisContext* redis, char* dataset_name, uint64_t requested_ct_size) {
	const uint64_t num_lookups = 10 * MILLION;
	const unsigned int num_threads = 4;
	uint64_t i;
	int result;
	ct_key* keys;
	dataset_t dataset;
	stopwatch_t timer;
	redisReply* reply;
	insert_thread_ctx insert_ctx;
	lookup_thread_ctx thread_contexts[num_threads];

	printf("Reading dataset...\n");
	seed_and_print();
	init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	keys = read_dataset_keys(&dataset);

	result = set_ct_size(redis, &dataset, requested_ct_size);
	if (result != OK)
		return;

	reply = redisCommand(redis, "DEL test_set");
	if (reply == NULL) {
		printf("Error: Couldn't delete key (%s)\n", redis->errstr);
		return;
	}


	printf("Inserting dataset...\n");
	insert_ctx.num_keys = dataset.num_keys;
	insert_ctx.keys = keys;
	insert_thread(&insert_ctx);

	printf("Creating workloads...\n");
	for (i = 0; i < num_threads; i++)
		create_lookup_workload(keys, dataset.num_keys, num_lookups / num_threads, &(thread_contexts[i]));

	printf("Running with %d IO threads...\n", num_threads);
	stopwatch_start(&timer);
	run_multiple_threads(lookup_thread, num_threads, thread_contexts, sizeof(thread_contexts[0]));
	float time_took = stopwatch_value(&timer);
	report(time_took, num_lookups);
}

const ycsb_workload_spec YCSB_A_SPEC = {{0.5,  0,    0.5,  0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_B_SPEC = {{0.95, 0,    0.05, 0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_C_SPEC = {{1.0,  0,    0,    0,    0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_D_SPEC = {{0,    0.95, 0,    0.05, 0,    0  }, 10 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_E_SPEC = {{0,    0,    0,    0.05, 0.95, 0  }, 50 * MILLION, DIST_ZIPF};
const ycsb_workload_spec YCSB_F_SPEC = {{0.5,  0,    0,    0,    0,    0.5}, 10 * MILLION, DIST_ZIPF};

void generate_ycsb_workload(dataset_t* dataset, ct_key* keys, ycsb_workload* workload,
							const ycsb_workload_spec* spec, int thread_id, int num_threads) {
	uint64_t i;
	int data_size;
	ct_key* key;
	uint64_t insert_offset;
	uint64_t inserts_per_thread;
	uint64_t total_read_latest = 0;
	uint64_t failed_read_latest = 0;
	uint64_t read_latest_block_size;
	dynamic_buffer_t workload_buf;
	rand_distribution dist;
	rand_distribution backward_dist;
	uint64_t num_inserts = 0;

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
			// Cannot perform read/update before any keys were inserted
			i--;
			continue;
		}

		switch (op->type) {
			case YCSB_READ:
			case YCSB_SCAN:{
				key = &(keys[rand_dist(&dist)]);
				data_size = sizeof(blob_t) + key->size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				blob_t* data = (blob_t*) (workload_buf.ptr + op->data_pos);
				data->size = key->size;
				memcpy(data->bytes, key->bytes, key->size);
			}
			break;

			case YCSB_READ_LATEST:
				// Data for read-latest ops is generated separately
				break;

			case YCSB_RMW:
			case YCSB_UPDATE:{
				key = &(keys[rand_dist(&dist)]);
				data_size = sizeof(redis_kv) + key->size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				redis_kv* new_kv = (redis_kv*) (workload_buf.ptr + op->data_pos);

				new_kv->value = ZSET_CONST_SCORE;
				new_kv->key_size = key->size;
				memcpy(new_kv->key_bytes, key->bytes, key->size);
			}
			break;

			case YCSB_INSERT:{
				key = &(keys[insert_offset + num_inserts]);
				num_inserts++;
				data_size = sizeof(redis_kv) + key->size;
				op->data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				redis_kv* new_kv = (redis_kv*) (workload_buf.ptr + op->data_pos);
				new_kv->value = ZSET_CONST_SCORE;
				new_kv->key_size = key->size;
				memcpy(new_kv->key_bytes, key->bytes, key->size);
			}
			break;

			default:
				printf("Error: Unknown YCSB op type %d\n", op->type);
				return;
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
					key = &(keys[insert_offset + block - backwards - 1]);
				} else {
					// This read-latest op refers to a key that was loaded before the workload started
					backwards -= block * num_threads;
					key = &(keys[workload->initial_num_keys - backwards - 1]);
				}

				data_size = sizeof(blob_t) + key->size;
				uint64_t data_pos = dynamic_buffer_extend(&workload_buf, data_size);

				blob_t* data = (blob_t*) (workload_buf.ptr + data_pos);
				data->size = key->size;
				memcpy(data->bytes, key->bytes, key->size);

				if (i == 0)
					block_offsets[block] = (uint8_t*) data_pos;
			}

			uint64_t sentinel_pos = dynamic_buffer_extend(&workload_buf, sizeof(blob_t));
			blob_t* sentinel = (blob_t*) (workload_buf.ptr + sentinel_pos);
			sentinel->size = 0xFFFFFFFF;
		}
	}

	workload->data_buf = workload_buf.ptr;

	// Now that the final buffer address is known, convert the read-latest offsets to pointers
	for (thread = 0; thread < num_threads; thread++) {
		for (block = 0; block < num_inserts + 1; block++)
			workload->read_latest_blocks_for_thread[thread][block] += (uintptr_t) (workload->data_buf);
	}

	return;
}

typedef struct ycsb_thread_ctx_struct {
	union {
		uint64_t inserts_done;

		// Make sure inserts_done is on a cacheline by itself to prevent false sharing
		uint8_t padding[CACHELINE_BYTES];
	};
	uint64_t pipeline_depth;
	uint64_t thread_id;
	uint64_t num_threads;
	uint64_t total_read_latest;
	uint64_t failed_read_latest;
	uint64_t read_latest_from_thread;
	struct ycsb_thread_ctx_struct* thread_contexts;
	ycsb_workload workload;

	uint64_t last_inserts_done[MAX_THREADS];
	uint8_t* next_read_latest_key[MAX_THREADS];
	uint8_t** thread_read_latest_blocks[MAX_THREADS];
} ycsb_thread_ctx;

// Send the Redis command coresponding to a single YCSB op. Don't wait for reply.
// Returns 1 if a command was sent, 0 otherwise (e.g. a skipped read-latest command)
int send_ycsb_op(redisContext* redis, ycsb_thread_ctx* ctx, uint64_t op_num) {
	int result;
	char buf[20];
	redisReply* reply;
	ycsb_thread_ctx* inserter;
	uint8_t range_start_buf[MAX_KEY_SIZE + 1];
	char* zadd_template[] = {"ZADD", "test_set", buf, NULL};
	size_t zadd_template_lens[] = {4, 8, 0, 0};
	char* zscore_template[] = {"ZSCORE", "test_set", NULL};
	size_t zscore_template_lens[] = {6, 8, 0};
	char* zrangebylex_template[] = {"ZRANGEBYLEX", "test_set", range_start_buf, "+", "LIMIT", "0", buf};
	size_t zrangebylex_template_lens[] = {11, 8, 0, 1, 5, 1, 0};
	ycsb_op* op = &(ctx->workload.ops[op_num]);

	switch (op->type) {
		case YCSB_INSERT:
		case YCSB_UPDATE: {
			redis_kv* kv = (redis_kv*) (ctx->workload.data_buf + op->data_pos);
			zadd_template_lens[2] = sprintf(buf, "%f", kv->value);

			zadd_template[3] = kv->key_bytes;
			zadd_template_lens[3] = kv->key_size;
			redisAppendCommandArgv(redis, 4, (const char**) zadd_template, zadd_template_lens);

			// ctx->inserts_done will be updated once the reply for the command is received
		}
		break;

		case YCSB_READ: {
			blob_t* key = (blob_t*) (ctx->workload.data_buf + op->data_pos);
			zscore_template[2] = key->bytes;
			zscore_template_lens[2] = key->size;
			redisAppendCommandArgv(redis, 3, (const char**) zscore_template, zscore_template_lens);
		}
		break;

		case YCSB_RMW: {
			assert(ctx->pipeline_depth == 1);
			redis_kv* kv = (redis_kv*) (ctx->workload.data_buf + op->data_pos);

			// Read the curent value
			zscore_template[2] = kv->key_bytes;
			zscore_template_lens[2] = kv->key_size;
			redisAppendCommandArgv(redis, 3, (const char**) zscore_template, zscore_template_lens);
			result = redisGetReply(redis, (void**) (&reply));
			if (result != REDIS_OK) {
				printf("Redis error\n");
				return 0;
			}
			if (reply->type != REDIS_REPLY_STRING) {
				printf("Unexpected reply type %d\n", reply->type);
				freeReplyObject(reply);
				return 0;
			}
			freeReplyObject(reply);

			// Write the new value
			zadd_template_lens[2] = sprintf(buf, "%f", kv->value);
			zadd_template[3] = kv->key_bytes;
			zadd_template_lens[3] = kv->key_size;
			redisAppendCommandArgv(redis, 4, (const char**) zadd_template, zadd_template_lens);
			// The reply will be read by the caller
		}
		break;

		case YCSB_READ_LATEST: {
			ctx->total_read_latest++;

			blob_t* key = (blob_t*) (ctx->next_read_latest_key[ctx->read_latest_from_thread]);

			// Advancing next_read_latest_key must be done before checking whether to
			// move to another block (by comparing inserts_done). Otherwise, in the
			// single-threaded case, we'll advance next_read_latest_key[0] after it was
			// set to the block start, and by an incorrect amount.
			if (key->size != 0xFFFFFFFFU)
				ctx->next_read_latest_key[ctx->read_latest_from_thread] += sizeof(blob_t) + key->size;

			ctx->read_latest_from_thread++;
			if (ctx->read_latest_from_thread == ctx->num_threads)
				ctx->read_latest_from_thread = 0;

			inserter = &(ctx->thread_contexts[ctx->read_latest_from_thread]);
			uint64_t inserts_done = __atomic_load_n(&(inserter->inserts_done), __ATOMIC_RELAXED);
			if (inserts_done != ctx->last_inserts_done[ctx->read_latest_from_thread]) {
				ctx->last_inserts_done[ctx->read_latest_from_thread] = inserts_done;

				uint8_t* block_start = ctx->thread_read_latest_blocks[ctx->read_latest_from_thread][inserts_done];
				ctx->next_read_latest_key[ctx->read_latest_from_thread] = block_start;
				__builtin_prefetch(&(ctx->thread_read_latest_blocks[ctx->read_latest_from_thread][inserts_done+8]));
			}
			__builtin_prefetch(ctx->next_read_latest_key[ctx->read_latest_from_thread]);

			if (key->size == 0xFFFFFFFFU) {
				// Reached end-of-block sentinel
				ctx->failed_read_latest++;
				return 0;
			}

			zscore_template[2] = key->bytes;
			zscore_template_lens[2] = key->size;
			redisAppendCommandArgv(redis, 3, (const char**) zscore_template, zscore_template_lens);
		}
		break;

		case YCSB_SCAN: {
			blob_t* start_key = (blob_t*) (ctx->workload.data_buf + op->data_pos);
			uint64_t range_size = (rand_dword() % 100) + 1;

			range_start_buf[0] = '[';
			if (start_key->size > MAX_KEY_SIZE) {
				printf("Error: Key too long\n");
				return 0;
			}
			memcpy(range_start_buf + 1, start_key->bytes, start_key->size);
			zrangebylex_template_lens[2] = start_key->size + 1;

			zrangebylex_template_lens[6] = sprintf(buf, "%lu", range_size);
			redisAppendCommandArgv(redis, 7, (const char**) zrangebylex_template, zrangebylex_template_lens);
		}
		break;
	}
	return 1;
}

int process_ycsb_reply(redisContext* redis, ycsb_thread_ctx* ctx, uint64_t op_num) {
	int result;
	redisReply* reply;
	ycsb_op* op = &(ctx->workload.ops[op_num]);

	result = redisGetReply(redis, (void**) (&reply));
	if (result != REDIS_OK) {
		printf("Redis error\n");
		return 0;
	}

	if (op->type == YCSB_INSERT) {
		if (reply->type != REDIS_REPLY_INTEGER) {
			printf("Error: Unexpected reply type for insert command (%d)\n", reply->type);
			freeReplyObject(reply);
			return 0;
		}
		if (reply->integer != 1) {
			printf("Error: No key inserted\n");
			freeReplyObject(reply);
			return 0;
		}
		// Use atomic store to make sure that the store becomes visible to other threads
		// (and not, e.g., saved in a register).
		__atomic_store_n(&(ctx->inserts_done), ctx->inserts_done + 1, __ATOMIC_RELAXED);
	} else if (op->type == YCSB_UPDATE || op->type == YCSB_RMW) {
		// YCSB_RMW receives the read result by itself. Here we only have to handle the result of
		// the following write.
		if (reply->type != REDIS_REPLY_INTEGER) {
			printf("Error: Unexpected reply type for insert command (%d)\n", reply->type);
			freeReplyObject(reply);
			return 0;
		}

		// We don't check that the value in the reply is zero (meaning that we indeed updated
		// an existing key nd didn't insert a new one), because some ZSET implementations don't
		// supply that information and always return 1.
	} else {
		// Check replies for commands other than ZADD
		if (reply->type != REDIS_REPLY_STRING && reply->type != REDIS_REPLY_ARRAY) {
			// ZSCORE should return a string and ZRANGEBYLEX should return an array.
			printf("Unexpected reply type %d\n", reply->type);
			freeReplyObject(reply);
			return 0;
		}
	}

	freeReplyObject(reply);
	return 1;
}

#define QUEUE_SIZE (YCSB_PIPELINE_DEPTH + 1)
typedef struct {
	uint64_t data[QUEUE_SIZE];
	uint64_t size;
	uint64_t read_pos;
	uint64_t write_pos;
} queue_t;

static inline void queue_init(queue_t* queue) {
	queue->size = 0;
	queue->read_pos = 0;
	queue->write_pos = 0;
}

static inline void queue_put(queue_t* queue, uint64_t val) {
	queue->data[queue->write_pos++] = val;
	if (queue->write_pos == QUEUE_SIZE)
		queue->write_pos = 0;
	queue->size++;
}

static inline uint64_t queue_get(queue_t* queue) {
	uint64_t result = queue->data[queue->read_pos++];
	if (queue->read_pos == QUEUE_SIZE)
		queue->read_pos = 0;
	queue->size--;
	return result;
}

void* ycsb_thread(void* arg) {
	uint64_t i;
	int result;
	int expect_reply;
	queue_t ops_waiting_reply;
	uint64_t op_num = 0;
	ycsb_thread_ctx* ctx = (ycsb_thread_ctx*) arg;

	redisContext* redis;

	redis = redisConnect("127.0.0.1", 6379);
	if (redis->err) {
		printf("Error: %s\n", redis->errstr);
		return NULL;
	}

	ctx->total_read_latest = 0;
	ctx->failed_read_latest = 0;
	ctx->read_latest_from_thread = 0;

	for (i = 0;i < ctx->num_threads;i++) {
		ctx->last_inserts_done[i] = 0;
		ctx->thread_read_latest_blocks[i] = ctx->thread_contexts[i].workload.read_latest_blocks_for_thread[ctx->thread_id];
		ctx->next_read_latest_key[i] = ctx->thread_read_latest_blocks[i][0];
	}

	// Fill the pipeline
	queue_init(&ops_waiting_reply);
	for (i = 0;i < ctx->pipeline_depth; i++) {
		expect_reply = send_ycsb_op(redis, ctx, op_num);
		if (expect_reply)
			queue_put(&ops_waiting_reply, op_num);
		op_num++;
		if (op_num == ctx->workload.num_ops)
			break;
	}

	while (op_num < ctx->workload.num_ops) {
		assert(ops_waiting_reply.size <= ctx->pipeline_depth);
		if (ops_waiting_reply.size == ctx->pipeline_depth) {
			if (!process_ycsb_reply(redis, ctx, queue_get(&ops_waiting_reply)))
				return NULL;
		}
		expect_reply = send_ycsb_op(redis, ctx, op_num);
		if (expect_reply)
			queue_put(&ops_waiting_reply, op_num);
		op_num++;
	}

	// Drain the pipeline
	while (ops_waiting_reply.size) {
		if (!process_ycsb_reply(redis, ctx, queue_get(&ops_waiting_reply)))
			return NULL;
	}

	if (ctx->failed_read_latest > 0) {
		printf("Note: %lu / %lu (%.1f%%) of read-latest operations were skipped\n",
			ctx->failed_read_latest, ctx->total_read_latest,
			((float)(ctx->failed_read_latest)) / ctx->total_read_latest * 100.0);
	}

	return NULL;
}

void test_ycsb(redisContext* redis, char* dataset_name, uint64_t requested_ct_size,
			   const ycsb_workload_spec* spec) {
	const unsigned int num_threads = 4;
	uint64_t i;
	int result;
	ct_key* keys;
	redisReply* reply;
	dataset_t dataset;
	stopwatch_t timer;
	insert_thread_ctx insert_ctx;
	ycsb_thread_ctx thread_contexts[num_threads];
	ycsb_workload_spec per_thread_spec = *spec;

	// Divide the work evenly between the IO threads
	per_thread_spec.num_ops = spec->num_ops / num_threads;

	printf("Reading dataset...\n");
	seed_and_print();
	init_dataset(&dataset, dataset_name, DATASET_ALL_KEYS);
	keys = read_dataset_keys(&dataset);

	result = set_ct_size(redis, &dataset, requested_ct_size);
	if (result != OK)
		return;

	reply = redisCommand(redis, "DEL test_set");
	if (reply == NULL) {
		printf("Error: Couldn't delete key (%s)\n", redis->errstr);
		return;
	}

	printf("Creating workloads");
	for (i = 0;i < num_threads; i++) {
		generate_ycsb_workload(&dataset, keys, &(thread_contexts[i].workload), &per_thread_spec,
							   i, num_threads);
		if (spec->op_type_probs[YCSB_RMW] != 0.0)
			thread_contexts[i].pipeline_depth = 1;  // Read-modify-write operations canot be pipelined
		else
			thread_contexts[i].pipeline_depth = YCSB_PIPELINE_DEPTH;
		thread_contexts[i].thread_contexts = thread_contexts;
		thread_contexts[i].inserts_done = 0;
		thread_contexts[i].num_threads = num_threads;
		thread_contexts[i].thread_id = i;
		printf(".");
		fflush(stdout);
	}
	printf("\n");

	printf("Inserting dataset...\n");
	insert_ctx.num_keys = thread_contexts[0].workload.initial_num_keys;
	insert_ctx.keys = keys;
	insert_thread(&insert_ctx);

	printf("Running with %d IO threads...\n", num_threads);
	stopwatch_start(&timer);

	run_multiple_threads(ycsb_thread, num_threads, thread_contexts, sizeof(thread_contexts[0]));

	float time_took = stopwatch_value(&timer);
	report(time_took, spec->num_ops);
}

const flag_spec_t FLAGS[] = {
	{ "--use-default-zset", 0},
	{ "--use-wormhole-zset", 0},
	{ "--use-hot-zset", 0},
	{ "--use-artolc-zset", 0},
	{ "--use-stx-zset", 0},
	{ "--ycsb-uniform-dist", 0},
	{ "--ct-size", 1},
	{ NULL, 0}
};

int main(int argc, char** argv) {
	redisReply* reply;
	redisContext* c;
	char* test_name;
	char* dataset_name;
	ycsb_workload_spec spec;
	uint64_t requested_ct_size;
	int is_ycsb = 0;

	args_t* args = parse_args((flag_spec_t*) FLAGS, argc, argv);
	if (args == NULL) {
		printf("Commandline error\n");
		return 1;
	}

	if (args->num_args != 2) {
		printf("Usage: %s <test-name> <dataset>\n", argv[0]);
		return 1;
	}

	test_name = args->args[0];
	dataset_name = args->args[1];

	c = redisConnect("127.0.0.1", 6379);
	if (c->err) {
		printf("Error: %s\n", c->errstr);
		return 1;
	}

	requested_ct_size = get_uint64_flag(args, "--ct-size", TRIE_SIZE_AUTO);

	if (has_flag(args, "--use-default-zset"))
		reply = redisCommand(c, "CONFIG SET zset-impl default");
	else if (has_flag(args, "--use-wormhole-zset"))
		reply = redisCommand(c, "CONFIG SET zset-impl wormhole");
	else if (has_flag(args, "--use-hot-zset"))
		reply = redisCommand(c, "CONFIG SET zset-impl hot");
	else if (has_flag(args, "--use-artolc-zset"))
		reply = redisCommand(c, "CONFIG SET zset-impl artolc");
	else if (has_flag(args, "--use-stx-zset"))
		reply = redisCommand(c, "CONFIG SET zset-impl stx");
	else
		reply = redisCommand(c, "CONFIG SET zset-impl cuckoo-trie");

	if (reply == NULL) {
		printf("Error: Couldn't set zset-impl (%s)\n", c->errstr);
		return 1;
	}

	if (!strcmp(args->args[0], "insert")) {
		test_insert(c, dataset_name, requested_ct_size);
		return 0;
	}

	if (!strcmp(args->args[0], "pos-lookup")) {
		test_pos_lookup(c, dataset_name, requested_ct_size);
		return 0;
	}

	if (!strcmp(test_name, "ycsb-a")) {
		spec = YCSB_A_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-b")) {
		spec = YCSB_B_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-c")) {
		spec = YCSB_C_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-d")) {
		spec = YCSB_D_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-e")) {
		spec = YCSB_E_SPEC;
		is_ycsb = 1;
	}

	if (!strcmp(test_name, "ycsb-f")) {
		spec = YCSB_F_SPEC;
		is_ycsb = 1;
	}

	if (is_ycsb) {
		if (has_flag(args, "--uniform-ycsb-dist"))
			spec.distribution = DIST_UNIFORM;
		else
			spec.distribution = DIST_ZIPF;
		test_ycsb(c, dataset_name, requested_ct_size, &spec);
		return 0;
	}

	printf("Unknown test name\n");
	return 1;
}
