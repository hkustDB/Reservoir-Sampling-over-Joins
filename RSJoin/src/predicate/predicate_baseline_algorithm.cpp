#include <fstream>
#include <iostream>
#include <vector>
#include "reservoir.h"
#include "utils.h"
#include "predicate/predicate_baseline_algorithm.h"

PredicateBaselineAlgorithm::~PredicateBaselineAlgorithm() {}

void PredicateBaselineAlgorithm::init(const std::string &file, int k, const std::string &s, int th) {
    this->input = file;
    this->reservoir = new Reservoir<std::string>(k);
    this->str = s;
    this->threshold = th;
}

Reservoir<std::string>* PredicateBaselineAlgorithm::run(bool print_info) {
    std::ifstream in(input);
    std::string line;

    auto start_time = std::chrono::system_clock::now();
    while (getline(in, line), !line.empty()) {
        if (line.at(0) == '-') {
            if (print_info) {
                auto end_time = std::chrono::system_clock::now();
                print_execution_info(start_time, end_time);
            }
        } else {
            process(line);
        }
    }

    return reservoir;
}

void PredicateBaselineAlgorithm::process(const std::string &line) {
    int edit_distance = compute_edit_distance(str, line);
    if (edit_distance <= threshold) {
        if (!reservoir->is_full()) {
            reservoir->insert(new std::string(line));

            if (reservoir->is_full()) {
                reservoir->update_w();
                reservoir->update_t();
            }
        } else {
            if (reservoir->get_t() > 0) {
                reservoir->t_subtract(1);
            } else {
                reservoir->replace(new std::string(line));
                reservoir->update_w();
                reservoir->update_t();
            }
        }
    }
}