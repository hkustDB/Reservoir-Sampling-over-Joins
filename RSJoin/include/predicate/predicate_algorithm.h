#ifndef RESERVOIR_PREDICATE_ALGORITHM_H
#define RESERVOIR_PREDICATE_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

template<typename T>
class Reservoir;

class PredicateAlgorithm {
public:
    ~PredicateAlgorithm();
    void init(const std::string& file, int k, const std::string& s, int th);
    Reservoir<std::string>* run(bool print_info);

private:
    std::string input;
    Reservoir<std::string>* reservoir = nullptr;
    std::string str;
    int threshold;

    void process(const std::string& line);
};


#endif //RESERVOIR_PREDICATE_ALGORITHM_H
