#include <iostream>
#include <cstring>
#include "../include/utils.h"

void print_execution_info(std::chrono::time_point<std::chrono::system_clock> start_time,
                          std::chrono::time_point<std::chrono::system_clock> end_time) {
    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count() / 1000 << " ms" << std::endl;
    printf("memory: %lu kB\n", get_memory_usage());
}

int next_int(const std::string& line, unsigned long* begin, unsigned long limit, char sep) {
    int result = 0;
    bool is_negative = false;

    if (*begin < limit) {
        char c = line.at(*begin);
        while (c != sep) {
            if (c == '-') {
                is_negative = true;
            } else {
                result = result * 10 + (c - '0');
            }
            *begin = *begin + 1;
            if (*begin >= limit)
                break;
            c = line.at(*begin);
        }
        *begin = *begin + 1;
    }
    result = is_negative ? -result : result;

    return result;
}

long next_long(const std::string& line, unsigned long* begin, unsigned long limit, char sep) {
    long result = 0;
    bool is_negative = false;

    if (*begin < limit) {
        char c = line.at(*begin);
        while (c != sep) {
            if (c == '-') {
                is_negative = true;
            } else {
                result = result * 10 + (c - '0');
            }
            *begin = *begin + 1;
            if (*begin >= limit)
                break;
            c = line.at(*begin);
        }
        *begin = *begin + 1;
    }
    result = is_negative ? -result : result;

    return result;
}

unsigned long next_unsigned_long(const std::string& line, unsigned long* begin, unsigned long limit, char sep) {
    unsigned long result = 0;

    if (*begin < limit) {
        char c = line.at(*begin);
        while (c != sep) {
            if (c == '-') {
                printf("invalid input for unsigned_long.\n");
                exit(1);
            } else {
                result = result * 10 + (c - '0');
            }
            *begin = *begin + 1;
            if (*begin >= limit)
                break;
            c = line.at(*begin);
        }
        *begin = *begin + 1;
    }

    return result;
}

double next_double(const std::string& line, unsigned long* begin, char sep) {
    if (line.at(*begin) == ',') {
        *begin += 1;
        return 0;
    } else {
        unsigned long end = line.find(sep, (*begin));
        double d = std::stod(line.substr((*begin), (end - *begin)));
        *begin = end + 1;
        return d;
    }
}

std::string next_date(const std::string& line, unsigned long* begin, char sep) {
    if (line.at(*begin) == ',') {
        *begin += 1;
        return "";
    } else {
        unsigned long end = line.find(sep, (*begin));
        std::string d = line.substr((*begin), (end - *begin));
        *begin = end + 1;
        return d;
    }
}

std::string next_string(const std::string& line, unsigned long* begin) {
    if (line.at(*begin) == ',') {
        *begin += 1;
        return "";
    } else if (line.at(*begin) == '"') {
        unsigned long end = line.find('"', (*begin) + 1);
        std::string s = line.substr((*begin) + 1, (end - *begin - 1));
        *begin = end + 2;
        return s;
    } else {
        printf("next_string parsing error: %s\n", line.c_str());
        exit(1);
    }
}

unsigned long get_memory_usage() {
    FILE* file = fopen("/proc/self/status", "r");
    if (file != NULL) {
        char line[128];

        while (fgets(line, 128, file) != nullptr) {
            if (!strncmp(line, "VmPeak:", 7)) {
                return strtoul(line + 7, nullptr, 0);
            }
        }
    }

    return 0;
}

int compute_edit_distance(const std::string& s1, const std::string& s2) {
    unsigned long m = s1.size();
    unsigned long n = s2.size();

    auto dp = std::vector<int>(n + 1, 0);

    for (int j = 0; j <= n; j++) {
        dp[j] = j;
    }

    for (int i = 1; i <= m; i++) {
        int prev = dp[0];
        dp[0] = i;
        for (int j = 1; j <= n; j++) {
            int tmp = dp[j];
            if (s1.at(i - 1) == s2.at(j - 1)) {
                dp[j] = prev;
            } else {
                int min1 = dp[j] < dp[j-1] ? dp[j] : dp[j-1];
                int min2 = prev < min1 ? prev : min1;
                dp[j] = min2 + 1;
            }
            prev = tmp;
        }
    }

    return dp[n];
}
