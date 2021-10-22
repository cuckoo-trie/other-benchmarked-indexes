SOURCES=test_wormhole.c ../util.c
DEPS=${SOURCES} ../util.h Makefile wormhole/libwh.so

CC ?= gcc

FLAGS=-Wmaybe-uninitialized

all: test_wormhole \
     test_wormhole_debug

clean:
	rm test_wormhole test_wormhole_debug

wormhole/libwh.so: Makefile make-wh.sh
	./make-wh.sh

test_wormhole: ${DEPS}
	${CC} -O2 ${FLAGS} -o $@ ${SOURCES} wormhole/libwh.so -lm -lpthread

test_wormhole_debug: ${DEPS}
	${CC} -g ${FLAGS} -o $@ ${SOURCES} wormhole/libwh.so -lm -lpthread