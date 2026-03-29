#include "protocol.hpp"

#include <sys/socket.h>
#include <unistd.h>

#include <cstring>

namespace flexql {

bool write_all(int fd, const char *data, std::size_t size) {
    std::size_t sent = 0;
    while (sent < size) {
        const ssize_t rc = send(fd, data + sent, size - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(rc);
    }
    return true;
}

bool read_exact(int fd, char *buffer, std::size_t size) {
    std::size_t received = 0;
    while (received < size) {
        const ssize_t rc = recv(fd, buffer + received, size - received, 0);
        if (rc <= 0) {
            return false;
        }
        received += static_cast<std::size_t>(rc);
    }
    return true;
}

bool read_line(int fd, std::string &line) {
    line.clear();
    char ch = '\0';
    while (true) {
        const ssize_t rc = recv(fd, &ch, 1, 0);
        if (rc <= 0) {
            return false;
        }
        if (ch == '\n') {
            return true;
        }
        line.push_back(ch);
    }
}

static bool send_block(int fd, const std::string &text) {
    const std::string header = std::to_string(text.size()) + "\n";
    return write_all(fd, header.c_str(), header.size()) && write_all(fd, text.data(), text.size());
}

static bool receive_block(int fd, std::string &text) {
    std::string header;
    if (!read_line(fd, header)) {
        return false;
    }
    const std::size_t size = static_cast<std::size_t>(std::stoull(header));
    text.assign(size, '\0');
    return read_exact(fd, text.data(), size);
}

bool send_request(int fd, const std::string &sql) {
    if (!write_all(fd, "EXEC\n", 5)) {
        return false;
    }
    return send_block(fd, sql);
}

bool receive_request(int fd, std::string &sql) {
    std::string tag;
    if (!read_line(fd, tag)) {
        return false;
    }
    if (tag != "EXEC") {
        return false;
    }
    return receive_block(fd, sql);
}

bool send_response(int fd, const ExecuteResult &response) {
    if (!response.ok) {
        if (!write_all(fd, "ERR\n", 4)) {
            return false;
        }
        return send_block(fd, response.error);
    }

    const std::string header = "OK " + std::to_string(response.result.columns.size()) + " " +
                               std::to_string(response.result.rows.size()) + "\n";
    if (!write_all(fd, header.c_str(), header.size())) {
        return false;
    }
    for (const std::string &column : response.result.columns) {
        if (!send_block(fd, column)) {
            return false;
        }
    }
    for (const auto &row : response.result.rows) {
        for (const std::string &value : row) {
            if (!send_block(fd, value)) {
                return false;
            }
        }
    }
    return true;
}

bool receive_response(int fd, ExecuteResult &response) {
    std::string line;
    if (!read_line(fd, line)) {
        return false;
    }
    if (line == "ERR") {
        response.ok = false;
        return receive_block(fd, response.error);
    }

    if (line.rfind("OK ", 0) != 0) {
        return false;
    }

    response.ok = true;
    response.error.clear();
    response.result = {};

    std::size_t cols = 0;
    std::size_t rows = 0;
    std::sscanf(line.c_str(), "OK %zu %zu", &cols, &rows);

    for (std::size_t i = 0; i < cols; ++i) {
        std::string column;
        if (!receive_block(fd, column)) {
            return false;
        }
        response.result.columns.push_back(std::move(column));
    }

    for (std::size_t r = 0; r < rows; ++r) {
        std::vector<std::string> row;
        row.reserve(cols);
        for (std::size_t c = 0; c < cols; ++c) {
            std::string value;
            if (!receive_block(fd, value)) {
                return false;
            }
            row.push_back(std::move(value));
        }
        response.result.rows.push_back(std::move(row));
    }

    return true;
}

}  // namespace flexql
