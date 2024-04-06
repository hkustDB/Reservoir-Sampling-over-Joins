#ifndef RESERVOIR_UTILS_H
#define RESERVOIR_UTILS_H

#include <string>
#include <chrono>
#include <vector>

void print_execution_info(std::chrono::time_point<std::chrono::system_clock> start_time, std::chrono::time_point<std::chrono::system_clock> end_time);
int next_int(const std::string& line, unsigned long* begin, unsigned long limit, char sep);
long next_long(const std::string& line, unsigned long* begin, unsigned long limit, char sep);
unsigned long next_unsigned_long(const std::string& line, unsigned long* begin, unsigned long limit, char sep);
double next_double(const std::string& line, unsigned long* begin, char sep);
std::string next_date(const std::string& line, unsigned long* begin, char sep);
std::string next_string(const std::string& line, unsigned long* begin);
unsigned long get_memory_usage();

int compute_edit_distance(const std::string& s1, const std::string& s2);

#endif
