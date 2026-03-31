#ifndef FLEXQL_PROTOCOL_HPP
#define FLEXQL_PROTOCOL_HPP

#include <string>
#include <vector>

#include "engine.hpp"

namespace flexql {

bool write_all(int fd, const char *data, std::size_t size);
bool read_exact(int fd, char *buffer, std::size_t size);
bool read_line(int fd, std::string &line);
bool send_request(int fd, const std::string &sql);
bool receive_request(int fd, std::string &sql);
bool send_response(int fd, const ExecuteResult &response);
bool receive_response(int fd, ExecuteResult &response);

}

#endif
