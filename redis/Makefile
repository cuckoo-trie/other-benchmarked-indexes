SOURCES=test_redis.c ../util.c
DEPS=${SOURCES} hiredis/libhiredis.so.0.15 ../util.h

all: test_redis test_redis_debug

hiredis/libhiredis.so.0.15:
	make -C hiredis
	cp hiredis/libhiredis.so hiredis/libhiredis.so.0.15

test_redis: ${DEPS} Makefile
	gcc -O3 -o test_redis -Ihiredis -Wl,-rpath -Wl,hiredis ${SOURCES} hiredis/libhiredis.so.0.15 -lpthread -lm

test_redis_debug: ${DEPS} Makefile
	gcc -g -o test_redis_debug -Ihiredis -Wl,-rpath -Wl,hiredis ${SOURCES} hiredis/libhiredis.so.0.15 -lpthread -lm
