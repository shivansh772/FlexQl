#include "flexql.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <string>

#include "network/protocol.hpp"

struct FlexQL {
    int socket_fd;
};

namespace {

char *dup_cstr(const std::string &text) {
    char *copy = static_cast<char *>(std::malloc(text.size() + 1));
    if (copy == nullptr) {
        return nullptr;
    }
    std::memcpy(copy, text.c_str(), text.size() + 1);
    return copy;
}

}

extern "C" {

int flexql_open(const char *host, int port, FlexQL **db) {
    if (host == nullptr || db == nullptr || port <= 0) {
        return FLEXQL_ERROR;
    }

    *db = nullptr;

    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    struct addrinfo *result = nullptr;
    const std::string port_text = std::to_string(port);
    if (getaddrinfo(host, port_text.c_str(), &hints, &result) != 0) {
        return FLEXQL_ERROR;
    }

    int socket_fd = -1;
    for (struct addrinfo *rp = result; rp != nullptr; rp = rp->ai_next) {
        socket_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (socket_fd < 0) {
            continue;
        }
        if (connect(socket_fd, rp->ai_addr, rp->ai_addrlen) == 0) {
            int nodelay = 1;
            setsockopt(socket_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
            break;
        }
        close(socket_fd);
        socket_fd = -1;
    }
    freeaddrinfo(result);

    if (socket_fd < 0) {
        return FLEXQL_ERROR;
    }

    FlexQL *handle = new FlexQL{socket_fd};
    *db = handle;
    return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (db == nullptr) {
        return FLEXQL_ERROR;
    }
    close(db->socket_fd);
    delete db;
    return FLEXQL_OK;
}

int flexql_exec(
    FlexQL *db,
    const char *sql,
    int (*callback)(void *, int, char **, char **),
    void *arg,
    char **errmsg
) {
    if (errmsg != nullptr) {
        *errmsg = nullptr;
    }
    if (db == nullptr || sql == nullptr) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("Invalid database handle or SQL string");
        }
        return FLEXQL_ERROR;
    }

    if (!flexql::send_request(db->socket_fd, sql)) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("Failed to send request to server");
        }
        return FLEXQL_ERROR;
    }

    flexql::ExecuteResult response;
    if (!flexql::receive_response(db->socket_fd, response)) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr("Failed to receive response from server");
        }
        return FLEXQL_ERROR;
    }

    if (!response.ok) {
        if (errmsg != nullptr) {
            *errmsg = dup_cstr(response.error);
        }
        return FLEXQL_ERROR;
    }

    if (callback != nullptr) {
        std::vector<char *> column_names;
        column_names.reserve(response.result.columns.size());
        for (std::string &name : response.result.columns) {
            column_names.push_back(name.data());
        }

        for (auto &row : response.result.rows) {
            std::vector<char *> values;
            values.reserve(row.size());
            for (std::string &value : row) {
                values.push_back(value.data());
            }
            if (callback(arg, static_cast<int>(values.size()), values.data(), column_names.data()) != 0) {
                break;
            }
        }
    }

    return FLEXQL_OK;
}

void flexql_free(void *ptr) {
    std::free(ptr);
}

}
