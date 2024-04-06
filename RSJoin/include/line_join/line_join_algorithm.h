#ifndef RESERVOIR_LINE_JOIN_ALGORITHM_H
#define RESERVOIR_LINE_JOIN_ALGORITHM_H

#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

template<typename T>
class Reservoir;
class LineJoinAttribute;
struct LineJoinRow;
struct GraphRow;

class LineJoinAlgorithm {
public:
    ~LineJoinAlgorithm();
    void init(int n, const std::string& file, int k);
    Reservoir<LineJoinRow>* run(bool print_info);

private:
    int n;
    std::string input;
    Reservoir<LineJoinRow>* reservoir = nullptr;
    std::vector<std::unordered_map<int, std::vector<GraphRow*>*>*>* relation_indices;
    std::vector<LineJoinAttribute*>* line_join_attributes;

    void process(const std::string& line);
};

#endif
