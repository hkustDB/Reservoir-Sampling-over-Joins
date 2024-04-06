#ifndef RESERVOIR_LINE_JOIN_UPDATE_TIME_ALGORITHM_H
#define RESERVOIR_LINE_JOIN_UPDATE_TIME_ALGORITHM_H


#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

template<typename T>
class Reservoir;
class LineJoinAttribute;
struct GraphRow;

class LineJoinUpdateTimeAlgorithm {
public:
    ~LineJoinUpdateTimeAlgorithm();
    void init(int n, const std::string& file);
    void run(bool print_info);

private:
    int n;
    std::string input;
    std::vector<std::unordered_map<int, std::vector<GraphRow*>*>*>* relation_indices;
    std::vector<LineJoinAttribute*>* line_join_attributes;

    void process(const std::string& line);
};


#endif
