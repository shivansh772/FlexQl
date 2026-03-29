CXX := g++
CXXFLAGS := -std=c++17 -Wall -Wextra -Wpedantic -O2 -Iinclude
LDFLAGS :=

BUILD_DIR := build
SRC_DIR := src

COMMON_SRCS := $(SRC_DIR)/engine.cpp $(SRC_DIR)/protocol.cpp
SERVER_SRCS := $(COMMON_SRCS) $(SRC_DIR)/server_main.cpp
CLIENT_SRCS := $(COMMON_SRCS) $(SRC_DIR)/api.cpp $(SRC_DIR)/client_main.cpp
EXAMPLE_SRCS := $(COMMON_SRCS) $(SRC_DIR)/api.cpp examples/api_example.c
BENCH_SRCS := $(COMMON_SRCS) $(SRC_DIR)/api.cpp $(SRC_DIR)/benchmark_main.cpp
BENCHMARK_COMPAT_SRCS := $(COMMON_SRCS) $(SRC_DIR)/api.cpp benchmarks/benchmark_flexql.cpp

SERVER_BIN := $(BUILD_DIR)/flexql-server
CLIENT_BIN := $(BUILD_DIR)/flexql-client
EXAMPLE_BIN := $(BUILD_DIR)/api_example
BENCH_BIN := $(BUILD_DIR)/flexql-benchmark
BENCHMARK_COMPAT_BIN := $(BUILD_DIR)/benchmark

.PHONY: all clean clean-data

all: $(SERVER_BIN) $(CLIENT_BIN) $(EXAMPLE_BIN) $(BENCH_BIN) $(BENCHMARK_COMPAT_BIN)

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SERVER_BIN): $(SERVER_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(SERVER_SRCS) $(LDFLAGS)

$(CLIENT_BIN): $(CLIENT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(CLIENT_SRCS) $(LDFLAGS)

$(EXAMPLE_BIN): $(EXAMPLE_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -x c++ -o $@ $(COMMON_SRCS) $(SRC_DIR)/api.cpp examples/api_example.c $(LDFLAGS)

$(BENCH_BIN): $(BENCH_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(BENCH_SRCS) $(LDFLAGS)

$(BENCHMARK_COMPAT_BIN): $(BENCHMARK_COMPAT_SRCS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS) -o $@ $(BENCHMARK_COMPAT_SRCS) $(LDFLAGS)

clean:
	rm -rf $(BUILD_DIR)

clean-data:
	rm -rf data/tables/*
	mkdir -p data/tables
	touch data/tables/.gitkeep
