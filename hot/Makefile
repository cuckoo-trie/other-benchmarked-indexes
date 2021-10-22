SOURCES=test_hot.cpp ../util.c
DEPS=${SOURCES} ../util.h

FLAGS=-I ./include -std=c++14 -march=native -mavx -mavx2 -mbmi2 -mlzcnt -Wno-deprecated-declarations -Wall -fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free

CXX ?= g++

all: test_hot hot_for_redis.so test_hot_debug

clean:
	rm test_hot hot_for_redis.so test_hot_debug

test_hot: ${DEPS} Makefile
	${CXX} -o $@ ${FLAGS} -O3 -DNDEBUG ${SOURCES} -ltbb -lpthread

hot_for_redis.so: hot_for_redis.cpp hot_for_redis.h Makefile
	${CXX} -o $@ ${FLAGS} -O3 -DNDEBUG -shared -fPIC hot_for_redis.cpp

test_hot_debug: ${DEPS} Makefile
	${CXX} -o $@ ${FLAGS} -g ${SOURCES} -ltbb -lpthread
