#include <cstdlib>
#include <iostream>
#include <string>

#include "flexql.h"

namespace {

int print_row(void *, int column_count, char **values, char **column_names) {
    for (int i = 0; i < column_count; ++i) {
        std::cout << column_names[i] << " = " << (values[i] != nullptr ? values[i] : "NULL") << "\n";
    }
    std::cout << "\n";
    return 0;
}

}  // namespace

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

        char *errmsg = nullptr;
        const int rc = flexql_exec(db, line.c_str(), print_row, nullptr, &errmsg);
        if (rc != FLEXQL_OK) {
            std::cerr << "SQL error: " << (errmsg != nullptr ? errmsg : "unknown error") << "\n";
            flexql_free(errmsg);
        }
    }

    flexql_close(db);
    std::cout << "Connection closed\n";
    return 0;
}
