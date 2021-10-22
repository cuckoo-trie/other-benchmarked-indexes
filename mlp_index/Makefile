SOURCES=test_mlp.cpp MlpSetUInt64.cpp ../util.c
DEPS=${SOURCES} MlpSetUInt64.h common.h ../util.h

FLAGS=-std=c++14 -march=native -mavx2

all: test_mlp test_mlp_debug

# Although MlpIndex is single-threaded, we have to link with pthread as ../util.c contains
# thread-management functions (unused by MlpIndex) that refernce pthread symbols.
test_mlp: ${DEPS} Makefile
	g++ -o $@ ${FLAGS} -O3 -DNDEBUG -DBUILD_FLAVOR=RELEASE ${SOURCES} -lpthread

test_mlp_debug: ${DEPS} Makefile
	g++ -o $@ ${FLAGS} -O0 -DBUILD_FLAVOR=DEBUG -DDEBUG_BUILD ${SOURCES} -lpthread