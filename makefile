common_obj = $(patsubst %.c, %.o,$(wildcard *.c))
TARGET = test_assign2_1 test_assign2_2
all:$(TARGET)
test_assign2_1:test_assign2_1.o $(filter-out test_assign2_2.o,$(common_obj))
	cc -o $@ $^

test_assign2_2:test_assign2_2.o $(filter-out test_assign2_1.o,$(common_obj))
	cc -o $@ $^
.PHONY: clean
clean:
	rm -f *.o
