#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cctype>
#include <iostream>
#include <sstream>
#include <string>

#include "flexql.h"

namespace {

struct PrintStats {
    std::size_t rows_printed = 0;
};

std::string trim(std::string text) {
    const auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
    text.erase(text.begin(), std::find_if(text.begin(), text.end(), not_space));
    text.erase(std::find_if(text.rbegin(), text.rend(), not_space).base(), text.end());
    return text;
}

std::string upper(std::string text) {
    for (char &ch : text) {
        ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
    }
    return text;
}

std::size_t count_insert_values(const std::string &sql) {
    const std::string sql_up = upper(sql);
    const std::size_t values_pos = sql_up.find("VALUES");
    if (values_pos == std::string::npos) {
        return 0;
    }

    bool in_quote = false;
    int depth = 0;
    std::size_t groups = 0;
    for (std::size_t i = values_pos + 6; i < sql.size(); ++i) {
        const char ch = sql[i];
        if (ch == '\'') {
            in_quote = !in_quote;
            continue;
        }
        if (in_quote) {
            continue;
        }
        if (ch == '(') {
            if (depth == 0) {
                ++groups;
            }
            ++depth;
        } else if (ch == ')' && depth > 0) {
            --depth;
        }
    }
    return groups;
}

std::size_t estimate_affected_rows(const std::string &sql) {
    const std::string text = trim(sql);
    const std::string text_up = upper(text);
    if (text_up.rfind("BULK INSERT ", 0) == 0) {
        std::istringstream iss(text);
        std::string bulk_kw;
        std::string insert_kw;
        std::string table_name;
        std::size_t row_count = 0;
        iss >> bulk_kw >> insert_kw >> table_name >> row_count;
        return row_count;
    }
    if (text_up.rfind("INSERT INTO ", 0) == 0) {
        return count_insert_values(text);
    }
    return 0;
}

int print_row(void *arg, int column_count, char **values, char **column_names) {
    PrintStats *stats = static_cast<PrintStats *>(arg);
    for (int i = 0; i < column_count; ++i) {
        std::cout << column_names[i] << " = " << (values[i] != nullptr ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    if (stats != nullptr) {
        ++stats->rows_printed;
    }
    return 0;
}

}

int main(int argc, char **argv) {
    const char *host = argc >= 2 ? argv[1] : "127.0.0.1";
    const int port = argc >= 3 ? std::atoi(argv[2]) : 9000;

    FlexQL *db = nullptr;
    if (flexql_open(host, port, &db) != FLEXQL_OK) {
        std::cerr << "Cannot connect to FlexQL server\n";
        return 1;
    }

    std::cout << "Connected to FlexQL server\n";

    std::string line;
    while (true) {
        std::cout << "flexql> " << std::flush;
        if (!std::getline(std::cin, line)) {
            break;
        }
        if (line == ".exit" || line == "exit" || line == "quit") {
            break;
        }
        if (line.empty()) {
            continue;
        }

        PrintStats stats;
        char *errmsg = nullptr;
        const auto start = std::chrono::steady_clock::now();
        const int rc = flexql_exec(db, line.c_str(), print_row, &stats, &errmsg);
        const auto end = std::chrono::steady_clock::now();
        const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        if (rc != FLEXQL_OK) {
            std::cerr << "SQL error: " << (errmsg != nullptr ? errmsg : "unknown error") << "\n";
            flexql_free(errmsg);
            std::cout << "Time: " << elapsed_ms << " ms\n";
            continue;
        }

        const std::string line_up = upper(trim(line));
        if (line_up.rfind("SELECT ", 0) == 0) {
            std::cout << stats.rows_printed << " rows in set";
        } else {
            std::cout << "Query OK, " << estimate_affected_rows(line) << " rows affected";
        }
        std::cout << " (" << elapsed_ms << " ms)\n";
    }

    flexql_close(db);
    std::cout << "Connection closed\n";
    return 0;
}
