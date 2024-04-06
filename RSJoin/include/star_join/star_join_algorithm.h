#ifndef RESERVOIR_STAR_JOIN_ALGORITHM_H
#define RESERVOIR_STAR_JOIN_ALGORITHM_H

#include <string>
#include <vector>
#include <unordered_map>
#include <chrono>

template<typename T>
class Reservoir;
class StarJoinBatch;
struct StarJoinRow;
struct GraphRow;

class StarJoinAlgorithm {
public:
    ~StarJoinAlgorithm();
    void init(int n, const std::string& file, int k);
    Reservoir<StarJoinRow>* run(bool print_info);

private:
    int n;
    std::string input;
    Reservoir<StarJoinRow>* reservoir = nullptr;
    std::unordered_map<int, std::vector<std::vector<GraphRow*>*>*>* relation_indices;
    StarJoinBatch* batch;

    void process(const std::string& line);
};


#endif
