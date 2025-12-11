// server.cpp
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <stdexcept>

// JSON parser
std::map<std::string, std::string> parse_config(const std::string& filename) {
    std::map<std::string, std::string> config;
    std::ifstream file(filename);
    std::string line;
    while (std::getline(file, line)) {
        size_t quote1 = line.find('\"');
        if (quote1 == std::string::npos) continue;
        size_t quote2 = line.find('\"', quote1 + 1);
        if (quote2 == std::string::npos) continue;

        std::string key = line.substr(quote1 + 1, quote2 - quote1 - 1);

        size_t colon = line.find(':', quote2);
        if (colon == std::string::npos) continue;

        size_t val_start = line.find_first_not_of(" \t,", colon + 1);
        if (val_start == std::string::npos) continue;

        size_t val_end = line.find_last_not_of(" \t,");
        std::string value = line.substr(val_start, val_end - val_start + 1);
        
        // If value is a string literal, remove quotes
        if (value.front() == '\"' && value.back() == '\"') {
            value = value.substr(1, value.length() - 2);
        }
        config[key] = value;
    }
    return config;
}

std::vector<std::string> read_words(const std::string& filename) {
    std::ifstream file(filename);
    std::vector<std::string> words;
    std::string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    
    // String splitting
    size_t start = 0;
    size_t end = content.find(',');
    while (end != std::string::npos) {
        words.push_back(content.substr(start, end - start));
        start = end + 1;
        end = content.find(',', start);
    }
    words.push_back(content.substr(start)); // Add the last word
    return words;
}

// Function to handle a client connection
void handle_client(int client_socket, const std::vector<std::string>& words) {
    char buffer[1024] = {0};
    while (true) {
        ssize_t bytes_read = read(client_socket, buffer, 1023);
        if (bytes_read <= 0) {
            // Client closed connection or error occurred
            break;
        }

        std::string req(buffer, bytes_read);
        std::string response;
        try {
            size_t comma_pos = req.find(',');
            if (comma_pos == std::string::npos) throw std::invalid_argument("Invalid request format");

            int p = std::stoi(req.substr(0, comma_pos));
            int k = std::stoi(req.substr(comma_pos + 1));

            if (p >= static_cast<int>(words.size()) || p < 0) {
                response = "EOF\n";
            } else {
                std::string partial_response;
                bool eof_reached = false;
                for (int i = 0; i < k; ++i) {
                    int current_pos = p + i;
                    if (current_pos < static_cast<int>(words.size())) {
                        if (i > 0) partial_response += ",";
                        partial_response += words[current_pos];
                    } else {
                        partial_response += ",EOF";
                        eof_reached = true;
                        break;
                    }
                }
                response = partial_response + "\n";
            }
        } catch (const std::exception& e) {
            response = "EOF\n"; // Send EOF for any parsing errors
        }

        send(client_socket, response.c_str(), response.length(), 0);
    }
    close(client_socket);
    // std::cout << "Client disconnected." << std::endl;
}


int main(int argc, char* argv[]) {
    std::string config_path = "config.json";
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--config" && i + 1 < argc) {
            config_path = argv[i + 1];
        }
    }

    auto config = parse_config(config_path);
    if (config.find("server_port") == config.end() || config.find("filename") == config.end()) {
        std::cerr << "Error: Missing required config parameters." << std::endl;
        return 1;
    }
    
    int port = std::stoi(config["server_port"]);
    std::string filename = config["filename"];
    std::vector<std::string> words = read_words(filename);
    
    int server_fd;
    struct sockaddr_in address;
    int opt = 1;
    socklen_t addrlen = sizeof(address);

    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
        perror("socket failed");
        exit(EXIT_FAILURE);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) {
        perror("setsockopt");
        exit(EXIT_FAILURE);
    }
    
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY; // Listen on all available interfaces
    address.sin_port = htons(port);

    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        perror("bind failed");
        exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 10) < 0) {
        perror("listen");
        exit(EXIT_FAILURE);
    }

    std::cout << "Server listening on port " << port << std::endl;

    while (true) {
        int new_socket;
        if ((new_socket = accept(server_fd, (struct sockaddr *)&address, &addrlen)) < 0) {
            perror("accept");
            continue; // Continue to next iteration
        }
        // std::cout << "Client connected." << std::endl;
        handle_client(new_socket, words);
    }

    return 0;
}