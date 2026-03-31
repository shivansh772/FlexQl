#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>

#include "flexql.h"

namespace {

constexpr int kInsertBatchSize = 250;

int count_rows(void *data, int, char **, char **) {
    int *count = static_cast<int *>(data);
    ++(*count);
    return 0;
}

bool exec_or_print(FlexQL *db, const std::string &sql) {
    char *errmsg = nullptr;
    const int rc = flexql_exec(db, sql.c_str(), nullptr, nullptr, &errmsg);
    if (rc != FLEXQL_OK) {
        std::cerr << "SQL error: " << (errmsg ? errmsg : "unknown") << "\n";
        flexql_free(errmsg);
        return false;
    }
    return true;
}

void append_row_values(std::string &sql, int row_id) {
    sql += "(" + std::to_string(row_id) + ", 'user_" + std::to_string(row_id) + "', " +
           std::to_string(row_id % 100) + ")";
}

}

int main(int argc, char **argv) {
    const char *host = argc >= 2 ? argv[1] : "127.0.0.1";
    const int port = argc >= 3 ? std::atoi(argv[2]) : 9000;
    const int rows = argc >= 4 ? std::atoi(argv[3]) : 10000;

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server\n";
        return 1;
    }

    const std::string suffix = std::to_string(static_cast<long long>(std::time(nullptr)));
    const std::string table = "BENCH_" + suffix;

    if (!exec_or_print(db, "CREATE TABLE " + table + "(ID DECIMAL, NAME VARCHAR, SCORE DECIMAL);")) {
        flexql_close(db);
        return 1;
    }

    const auto insert_start = std::chrono::steady_clock::now();
    for (int start = 1; start <= rows; start += kInsertBatchSize) {
        const int end = std::min(start + kInsertBatchSize - 1, rows);
        std::string sql = "INSERT INTO " + table + " VALUES ";
        for (int i = start; i <= end; ++i) {
            if (i > start) {
                sql += ",";
            }
            append_row_values(sql, i);
        }
        sql += ";";

        if (!exec_or_print(db, sql)) {
            flexql_close(db);
            return 1;
        }
    }
    const auto insert_end = std::chrono::steady_clock::now();

    int returned_rows = 0;
    char *errmsg = nullptr;
    const auto select_start = std::chrono::steady_clock::now();
    const int select_rc = flexql_exec(
        db,
        ("SELECT NAME, SCORE FROM " + table + " WHERE ID = " + std::to_string(rows) + ";").c_str(),
        count_rows,
        &returned_rows,
        &errmsg
    );
    const auto select_end = std::chrono::steady_clock::now();

    if (select_rc != FLEXQL_OK) {
        std::cerr << "SQL error: " << (errmsg ? errmsg : "unknown") << "\n";
        flexql_free(errmsg);
        flexql_close(db);
        return 1;
    }

    const auto insert_ms = std::chrono::duration_cast<std::chrono::milliseconds>(insert_end - insert_start).count();
    const auto select_us = std::chrono::duration_cast<std::chrono::microseconds>(select_end - select_start).count();

    std::cout << "Benchmark rows: " << rows << "\n";
    std::cout << "Insert time (ms): " << insert_ms << "\n";
    std::cout << "Indexed select time (us): " << select_us << "\n";
    std::cout << "Returned rows: " << returned_rows << "\n";

    flexql_close(db);
    return 0;
}
