#include "MlpSetUInt64.h"
#include <signal.h>
#include "../util.h"

#define MILLION 1000000

#define PID_NO_PROFILER 0

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

void bench_insert(dataset_t* dataset) {
	uint64_t i;
	ct_key* keys;
	stopwatch_t timer;
	MlpSetUInt64::MlpSet mlp_set;

	keys = read_dataset(dataset);

	mlp_set.Init(dataset->num_keys);

	printf("Inserting...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	for (i = 0; i < dataset->num_keys; i++) {
		if (keys[i].size != 8) {
			printf("Error: MlpIndex can only handle 8-byte keys\n");
			return;
		}
		mlp_set.Insert(*(uint64_t*)(keys[i].bytes));
	}
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();
	report(time_took, dataset->num_keys);
}

void bench_pos_lookup(dataset_t* dataset) {
	const uint64_t num_lookups = 10 * MILLION;

	uint64_t i;
	ct_key* keys;
	stopwatch_t timer;
	MlpSetUInt64::MlpSet mlp_set;
	uint64_t* workload = (uint64_t*) malloc(sizeof(uint64_t) * num_lookups);

	keys = read_dataset(dataset);

	mlp_set.Init(dataset->num_keys);

	printf("Inserting...\n");
	for (i = 0; i < dataset->num_keys; i++) {
		if (keys[i].size != 8) {
			printf("Error: MlpIndex can only handle 8-byte keys\n");
			return;
		}
		mlp_set.Insert(*(uint64_t*)(keys[i].bytes));
	}

	printf("Creating workload...\n");
	for (i = 0; i < num_lookups; i++)
		workload[i] = *((uint64_t*)(keys[rand_uint64() % dataset->num_keys].bytes));

	printf("Performing lookups...\n");
	notify_critical_section_start();
	stopwatch_start(&timer);
	for (i = 0;i < num_lookups; i++) {
		bool found = mlp_set.Exist(workload[i]);
		if (!found) {
			printf("Error: Inserted key not found\n");
			return;
		}
		speculation_barrier();
	}
	float time_took = stopwatch_value(&timer);
	notify_critical_section_end();

	report(time_took, num_lookups);
}

bool try_insert_dataset(ct_key* keys, uint64_t num_keys, uint64_t num_cells, uint64_t* mem_required) {
	bool ok;
	uint64_t i;
	uint64_t start_mem, end_mem;
	MlpSetUInt64::MlpSet mlp_set;

	start_mem = virt_mem_usage();

	mlp_set.Init(num_cells);

	printf("Inserting with %lu cells... ", num_cells);
	for (i = 0; i < num_keys; i++) {
		ok = mlp_set.Insert(*(uint64_t*)(keys[i].bytes));
		if (!ok) {
			printf("Overflow\n");
			return false;
		}
	}

	end_mem = virt_mem_usage();

	printf("OK\n");
	*mem_required = end_mem - start_mem;
	return true;
}

void mem_usage(dataset_t* dataset) {
	bool ok;
	ct_key* keys;
	uint64_t mem;
	uint64_t best_mem;
	uint64_t size = 10000;
	uint64_t step = size;

	keys = read_dataset(dataset);

	while (1) {
		ok = try_insert_dataset(keys, dataset->num_keys, size, &mem);
		if (ok) {
			best_mem = mem;
			break;
		}
		size += step;
		step *= 2;
	}

	size -= step / 4;
	step /= 8;

	while (step > size / 100) {
		ok = try_insert_dataset(keys, dataset->num_keys, size, &mem);
		if (ok) {
			size -= step;
			if (mem < best_mem)
				best_mem = mem;
		} else {
			size += step;
		}
		step /= 2;
	}

	printf("Used %luKB (%.1fb/key)\n", best_mem / 1024, ((float)best_mem) / dataset->num_keys);
	printf("RESULT: keys=%lu bytes=%lu\n", dataset->num_keys, best_mem);
}

const flag_spec_t FLAGS[] = {
	{ "--dataset-size", 1},
	{ NULL, 0}
};

int main(int argc, char** argv) {
	dataset_t dataset;
	uint64_t dataset_size;
	args_t* args = parse_args(FLAGS, argc, argv);

	if (args == NULL || args->num_args != 2) {
		printf("Usage: %s [options] test-name dataset\n", argv[0]);
		return 1;
	}

	seed_and_print();

	profiler_pid = get_int_flag(args, "--profiler-pid", PID_NO_PROFILER);
	dataset_size = get_uint64_flag(args, "--dataset-size", DATASET_ALL_KEYS);
	init_dataset(&dataset, args->args[1], dataset_size);

	if (!strcmp(args->args[0], "insert")) {
		bench_insert(&dataset);
		return 0;
	}

	if (!strcmp(args->args[0], "pos-lookup")) {
		bench_pos_lookup(&dataset);
		return 0;
	}

	if (!strcmp(args->args[0], "mem-usage")) {
		mem_usage(&dataset);
		return 0;
	}

	printf("Unknown test name '%s'\n", argv[1]);
	return 1;
}
