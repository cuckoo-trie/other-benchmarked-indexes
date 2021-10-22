STX_SOURCES=tlx/tlx/die/core.cpp
STX_HEADERS=tlx/tlx/container/btree.hpp tlx/tlx/die/core.hpp
BENCH_SOURCES=test_stx.cpp ../util.c ${STX_SOURCES}
BENCH_HEADERS=../util.h ${STX_HEADERS}

all: test_stx stx_for_redis.so

# -lpthread is required because util.c references othread functions. STX is single-threaded
# so test_stx doesn't actually use these functions.
test_stx: ${BENCH_SOURCES} ${BENCH_HEADERS} Makefile
	g++ -g -std=c++11 -o $@ -O3 -march=native -Itlx -I.. ${BENCH_SOURCES} -lpthread

stx_for_redis.so: stx_for_redis.cpp stx_for_redis.h ${STX_SOURCES} ${STX_HEADERS} Makefile
	g++ -std=c++11 -fPIC -shared -o $@ -O3 -march=native -Itlx stx_for_redis.cpp ${STX_SOURCES}