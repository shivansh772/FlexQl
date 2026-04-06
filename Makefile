CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Wpedantic -O2 -Iinclude
DEPFLAGS := -MMD -MP
LDFLAGS :=

BUILD_DIR := build
SRC_DIR := src

STORAGE_DIR := $(SRC_DIR)/storage
NETWORK_DIR := $(SRC_DIR)/network
SERVER_DIR := $(SRC_DIR)/server
CLIENT_DIR := $(SRC_DIR)/client

COMMON_SRCS := $(STORAGE_DIR)/engine.cpp $(NETWORK_DIR)/protocol.cpp
SERVER_SRCS := $(COMMON_SRCS) $(SERVER_DIR)/server_main.cpp
CLIENT_SRCS := $(COMMON_SRCS) $(CLIENT_DIR)/api.cpp $(CLIENT_DIR)/client_main.cpp
EXAMPLE_SRCS := $(COMMON_SRCS) $(CLIENT_DIR)/api.cpp examples/api_example.c
BENCH_SRCS := $(COMMON_SRCS) $(CLIENT_DIR)/api.cpp $(CLIENT_DIR)/benchmark_main.cpp
BENCHMARK_COMPAT_SRCS := $(COMMON_SRCS) $(CLIENT_DIR)/api.cpp benchmarks/benchmark_flexql.cpp

SERVER_BIN := $(BUILD_DIR)/flexql-server
CLIENT_BIN := $(BUILD_DIR)/flexql-client
EXAMPLE_BIN := $(BUILD_DIR)/api_example
BENCH_BIN := $(BUILD_DIR)/flexql-benchmark
BENCHMARK_COMPAT_BIN := $(BUILD_DIR)/benchmark
DEPS := $(SERVER_BIN).d $(CLIENT_BIN).d $(EXAMPLE_BIN).d $(BENCH_BIN).d $(BENCHMARK_COMPAT_BIN).d

.PHONY: all clean clean-data benchmark-run benchmark-run-compat benchmark-unit-test

all: $(SERVER_BIN) $(CLIENT_BIN) $(EXAMPLE_BIN) $(BENCH_BIN) $(BENCHMARK_COMPAT_BIN)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SERVER_BIN): $(SERVER_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -MF $@.d -o $@ $(SERVER_SRCS) $(LDFLAGS)

$(CLIENT_BIN): $(CLIENT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -MF $@.d -o $@ $(CLIENT_SRCS) $(LDFLAGS)

$(EXAMPLE_BIN): $(EXAMPLE_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -MF $@.d -x c++ -o $@ $(COMMON_SRCS) $(CLIENT_DIR)/api.cpp examples/api_example.c $(LDFLAGS)

$(BENCH_BIN): $(BENCH_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -MF $@.d -o $@ $(BENCH_SRCS) $(LDFLAGS)

$(BENCHMARK_COMPAT_BIN): $(BENCHMARK_COMPAT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(DEPFLAGS) -MF $@.d -o $@ $(BENCHMARK_COMPAT_SRCS) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)

clean-data:
	rm -rf data/tables/*
	mkdir -p data/tables
	touch data/tables/.gitkeep

benchmark-run: all
	./scripts/run_benchmark.sh --api

benchmark-run-compat: all
	./scripts/run_benchmark.sh --compat

benchmark-unit-test: all
	./scripts/run_benchmark.sh --unit-test

-include $(DEPS)
