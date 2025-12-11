// client.cpp
#include <iostream>
#include <string>
#include <vector>
#include <map>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <cstdlib>

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
        
        if (value.front() == '\"' && value.back() == '\"') {
            value = value.substr(1, value.length() - 2);
        }
        config[key] = value;
    }
    return config;
}

// String splitting
void split(const std::string& s, char delimiter, std::vector<std::string>& tokens) {
    if (s.empty()) return;
    size_t start = 0;
    size_t end = s.find(delimiter);
    while (end != std::string::npos) {
        tokens.push_back(s.substr(start, end - start));
        start = end + 1;
        end = s.find(delimiter, start);
    }
    tokens.push_back(s.substr(start));
}


int main(int argc, char* argv[]) {
    std::string config_path = "config.json";
    int k_override = -1;
    bool quiet = false;
    
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--config" && i + 1 < argc) {
            config_path = argv[i + 1];
        } else if (std::string(argv[i]) == "--k" && i + 1 < argc) {
            k_override = std::stoi(argv[i + 1]);
        } else if (std::string(argv[i]) == "--quiet") {
            quiet = true;
        }
    }
    
    const char* env_k = getenv("K");
    const char* env_p = getenv("P");
    
    auto config = parse_config(config_path);
    
    std::string server_ip = config["server_ip"];
    int port = std::stoi(config["server_port"]);
    int k = (k_override != -1) ? k_override : (env_k ? std::stoi(env_k) : std::stoi(config["k"]));
    int p = env_p ? std::stoi(env_p) : std::stoi(config["p"]);

    auto start_time = std::chrono::high_resolution_clock::now();
    
    // Persistent Connection Logic
    int sock = 0;
    struct sockaddr_in serv_addr;
    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) { return -1; }
    
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, server_ip.c_str(), &serv_addr.sin_addr) <= 0) { return -1; }
    if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) { return -1; }

    std::vector<std::string> all_words;
    int current_offset = p;
    bool download_complete = false;

    while (!download_complete) {
        std::string request = std::to_string(current_offset) + "," + std::to_string(k) + "\n";
        send(sock, request.c_str(), request.length(), 0);
        
        char buffer[4096] = {0};
        ssize_t bytes_read = read(sock, buffer, 4095);
        
        if (bytes_read <= 0) {
            break; 
        }
        
        std::string response(buffer, bytes_read);
        if (!response.empty() && response.back() == '\n') response.pop_back();

        if (response.find("EOF") != std::string::npos) {
            download_complete = true;
            std::string final_part = response.substr(0, response.find("EOF"));
            if (!final_part.empty() && final_part.back() == ',') final_part.pop_back();
            if (!final_part.empty()) split(final_part, ',', all_words);
        } else {
            split(response, ',', all_words);
            current_offset += k;
        }
    }
    close(sock); // Close the single, persistent connection
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    
    if (!quiet) {
        std::map<std::string, int> freq_map;
        for (const auto& word : all_words) {
            if(!word.empty()) freq_map[word]++;
        }
        for (const auto& pair : freq_map) {
            std::cout << pair.first << "," << pair.second << std::endl;
        }
    }
    
    std::cout << "ELAPSED_MS:" << elapsed_ms << std::endl;
    
    return 0;
}