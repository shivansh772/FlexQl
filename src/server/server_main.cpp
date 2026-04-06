#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>
#include <string>

#include "storage/engine.hpp"
#include "network/protocol.hpp"

namespace {

void handle_client(int client_fd, flexql::Engine &engine) {
    while (true) {
        std::string sql;
        if (!flexql::receive_request(client_fd, sql)) {
            break;
        }
        const flexql::ExecuteResult result = engine.execute(sql);
        if (!flexql::send_response(client_fd, result)) {
            break;
        }
    }
    close(client_fd);
}

}

int main(int argc, char **argv) {
    const int port = (argc >= 2) ? std::atoi(argv[1]) : 9000;
    if (port <= 0) {
        std::cerr << "Invalid port\n";
        return 1;
    }

    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));

    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
        std::perror("bind");
        close(server_fd);
        return 1;
    }
    if (listen(server_fd, 16) != 0) {
        std::perror("listen");
        close(server_fd);
        return 1;
    }

    std::cout << "FlexQL server listening on port " << port << "\n";
    std::cout << "Server mode: multithreaded (one thread per client)\n";

    flexql::Engine engine;
    while (true) {
        sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        const int client_fd = accept(server_fd, reinterpret_cast<sockaddr *>(&client_addr), &client_len);
        if (client_fd < 0) {
            continue;
        }
        std::thread([client_fd, &engine]() {
            int nodelay = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
            handle_client(client_fd, engine);
        }).detach();
    }

    close(server_fd);
    return 0;
}
