# Indexes used for Cuckoo Trie benchmarks

This repository contains the code for the indexes the Cuckoo Trie was compared to in Cuckoo Trie: Exploiting Memory-Level Parallelism for Efficient DRAM Indexing. The code for the Cuckoo Trie index itself is [here](https://github.com/cuckoo-trie/cuckoo-trie-code).

The indexes included are: [HOT](https://github.com/speedskater/hot), [Wormhole](https://github.com/wuxb45/wormhole), [ARTOLC](https://github.com/wangziqi2016/index-microbench/tree/master/ARTOLC), [STX](https://github.com/tlx/tlx/tree/master/tlx/container) and [MlpIndex](https://github.com/sillycross/mlpds). For each index the repository contains:
- The version of the index used in the paper.
- A benchmarking program.
- Code to integrate the index as the sorted set implementation in Redis (not included for MlpIndex).

## Benchmarking considerations

Achieving high and consistent throughput (similar to the results in the paper), requires:
- **A good memory allocator**: Most of the indexes use `malloc` or C++ `new` to allocate memory, which makes them sensitive to the behavior of the allocator used. Achieving high throughput requires an allocator that uses huge pages and scales well to multiple threads, such as `jemalloc` (see below). The default allocator of `glibc` is *not* recommended, as it makes the indexes slower.
- **Turning off Turbo-Boost**: Intel processors have a feature called Turbo-Boost, that raises the clock frequency when only few cores are active. This is misleading when comparing single-core and multi-core results.
- **Consistent NUMA policy** (multiprocessor systems only): The performance of the indexes is affected by the distribution of their memory allocations between the different NUMA nodes of the system. A fair comparison requires that the same memory allocation policy is specified for all indexes. The memory policy can be set using `numactl`. Comparisons in the paper used an interleaved policy (`numactl --interleave=all`).
- **Consistent thread placement** (multiprocessor systems only): Some indexes are sensitive to the placement of their threads (i.e. which thread runs on which processor), even when NUMA interleaving is used. The multithreaded benchmarks pin each thread to a specific core. For single threaded benchmarks, an external command (such as `numactl -C 0`) should be used to ensure that the benchmark is always executed on the same processor.

An example commandline using the [jemalloc](http://jemalloc.net) allocator to run a single-threaded benchmark with huge pages, NUMA interleaving and consistent thread placement:
```sh
numactl --interleave=all -C 0 env LD_PRELOAD=path/to/libjemalloc.so MALLOC_CONF=thp:always ./test_wormhole insert rand-8
```

Note that `jemalloc` relies on the Transparent Huge Pages (THP) mechanism in Linux to allocate huge pages, and THP silently falls back to regular pages if no huge pages are available. Check that the counter `thp_fault_fallback` in `/proc/vmstat` is not incremented when running the benchmark to rule out this possibility.

## Benchmarking individual indexes

### Compilation

Running `make` in the directory of each index will build the index and a benchmarking program named `test_<index>`.

### Usage (all indexes except MlpIndex)

```sh
./test_<index> [flags] BENCHMARK DATASET
```

`BENCHMARK` is one of the available benchmark types and `DATASET` is the path of the file containing the dataset to be used. The special name `rand-<k>` specifies a dataset of 10M random `k`-byte keys.

The seed used for the random generator is printed at the start of each run to allow for debugging benchmarks that only fail for certain seeds.

Most benchmarks have a single-threaded and a multi-threaded version (named `mt-*`). The multi-threaded versions are only available for thread-safe indexes (HOT, Wormhole and ARTOLC). The available benchmark types are:
- `insert`, `mt-insert`: Insert all keys into the index.
- `pos-lookup`, `mt-pos-lookup`: Perform positive lookups for random keys. Positive lookups are ones that succeed (that is, ask for keys that are present in the trie).
- `mem-usage`: Report the memory usage of the index after inserting all keys. To have a fair comparison, the result includes just the index size, without the memory required to store the keys themselves.
- `ycsb-a`, `ycsb-b`, ... `ycsb-f`, `mt-ycsb-a`, ..., `mt-ycsb-f`: Run the appropriate mix of insert and lookup operations from the [YCSB](https://github.com/brianfrankcooper/YCSB/wiki/Core-Workloads) benchmark suite. By default, this runs the benchmark with a Zipfian query distribution, specify `--ycsb-uniform-dist` to use a uniform distribution instead.

The following flags can be used with each benchmark:
- `--dataset-size <N>` (`pos-lookup` only): Use only the first `N` keys of the dataset.
- `--threads <N>` (`mt-*` only): use `N` threads. Each thread is bound to a different core. The default is to use 4 threads.
- `--ycsb-uniform-dist` (`ycsb-*` only): Run YCSB benchmarks with a uniform query distribution. The default is Zipfian distribution.

### Usage (MlpIndex)

```sh
./test_mlp [--dataset-size <N>] BENCHMARK DATASET
```

`BENCHMARK` is one of the available benchmark types and `DATASET` is the path of the file containing the dataset to be used. The special name `rand-<k>` specifies a dataset of 10M random `k`-byte keys.

If `--dataset-size <N>` is specified, only the first `N` keys of the dataset are used.

The available benchmark types are `insert`, `pos-lookup` and `mem-usage`. They have the same meaning as for the other indexes.

### Dataset file format

The dataset files used with `benchmark` are binary files with the following format:
- `Number of keys`: a 64-bit little-endian number.
- `Total size of all keys`: a 64-bit little-endian number. This number does not include the size that precedes each key.
- `Keys`: Each key is encoded as a 32-bit little-endian length `L`, followed by `L` key bytes. The keys are not NULL-terminated.

## Redis integration

The Redis integration allows to replace the default sorted-set implementation of [Redis](https://redis.io) with the Cuckoo Trie or one of other benchmarked indexes. Only the functionality required for the experiments in the paper was implemented, anything else will probably crash Redis.

### Compilation

First, set up the following directory structure:

```
   <base dir>
   |
   + - redis
   |
   + - other_benchmarked_indexes  (this repo)
   |   |
   |   + - artolc
   |   |
   |   + - hot
   |   |
   |   ...
   |
   + - cuckoo_trie
       |
	   + - cuckoo_trie  (Cuckoo Trie repo)
	       |
		   + - Makefile
		   |
		   ...
```

The Cuckoo Trie can be cloned from [https://github.com/cuckoo-trie/cuckoo-trie-code](https://github.com/cuckoo-trie/cuckoo-trie-code).

Then, compile the Cuckoo Trie and all indexes in this repository according to their build instructions. The compilation of the modified Redis requires their compiled binaries to link against.

Finally, to clone, patch and build Redis, run the following in the base directory:

```sh
cd redis
git clone https://github.com/redis/redis
cd redis
git checkout c3f9e0
git apply ../../other_benchmarked_indexes/redis/redis-c3f9e0.patch
make
```

To test, start `redis-server`, connect with `redis-cli` (both should be in `redis/src`) and run the following at the `redis-cli` prompt:
```
127.0.0.1:6379> CONFIG SET zset-impl cuckoo-trie
```
The response should be `OK`.

### Usage

The `redis` directory contains a benchmark program to run benchmarks against the modified version of Redis. To build the benchmark program, simply run `make` inside the `redis` directory.

The usage is similar to the benchmark programs of the other indexes:
```sh
./test_redis [flags] BENCHMARK DATASET
```

`test_redis` assumes that a Redis server is running on the machine listening on the default port (6379). The benchmarks assume an empty Redis database and don't clean the database when they finish. 

**Note**: To prevent a benchmark from influencing subsequent ones, restart the server before each benchmark.

The supported benchmarks are `insert`, `pos-lookup`, `ycsb-a`, `ycsb-b` ... `ycsb-f`. See above for their descriptions.

The supported datasets are as in the other benchmark programs (see above).

The following flags are supported:
- `--use-default-zset`, `--use-wormhole-zset`, `--use-artolc-zset`, `--use-hot-zset`, `--use-stx-zset`: Configure Redis to use the specified data structure as its sorted set implementation. The default is to use the Cuckoo Trie.
- `--ct-size <N>`: Set the hash table size of the Cuckoo Trie to `N` cells. The default is 2.5 times the number of keys in the dataset. This flag is only relevant if the Cuckoo Trie was chosen as the sorted set implementation.
- `--ycsb-uniform-dist` (`ycsb-*` only): Run YCSB benchmarks with a uniform query distribution. The default is Zipfian distribution.

### Achieving high throughput

Use the given configuration file `in-memory-redis.conf`. It disables the periodic saving of the database to disk.

The [benchmarking considerations](#benchmarking-considerations) stated above are relevant here too. By default, Redis does not use huge pages [^1], which impacts the both the performance of its own code, and the performance of the index chosen as a sorted set implementation.

An example command line that makes Redis use huge pages and turns on NUMA interleaving:
```sh
numactl --interleave=all -C 0 env JE_MALLOC_CONF=thp:always ./redis-server path/to/in-memory-redis.conf
```

This instructs the `jemalloc` allocator embedded inside Redis to use huge pages. 

Note that `jemalloc` relies on the Transparent Huge Pages (THP) mechanism in Linux to allocate huge pages, and THP silently falls back to regular pages if no huge pages are available. Check that the counter `thp_fault_fallback` in `/proc/vmstat` is not incremented when running the benchmark to rule out this possibility.

[^1]: The Redis developers decided not to use huge pages because it slows down the saving of the database to disk. This is not a problem when benchmarking Redis as an in-memory database, with saving to the disk disabled.