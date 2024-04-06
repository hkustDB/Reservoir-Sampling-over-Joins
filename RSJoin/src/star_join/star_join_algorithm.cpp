#include <fstream>
#include <iostream>
#include <vector>
#include "reservoir.h"
#include "utils.h"
#include "graph_row.h"
#include "star_join/star_join_algorithm.h"
#include "star_join/star_join_row.h"
#include "star_join/star_join_batch.h"


StarJoinAlgorithm::~StarJoinAlgorithm() {
    delete reservoir;
    delete batch;

    for (auto kv : *relation_indices) {
        auto group = kv.second;
        for (auto vec : *group) {
            for (auto row : *vec) {
                delete row;
            }
            vec->clear();
            delete vec;
        }
        group->clear();
        delete group;
    }
    relation_indices->clear();
    delete relation_indices;
}

void StarJoinAlgorithm::init(int length, const std::string &file, int k) {
    this->n = length;
    this->input = file;
    this->reservoir = new Reservoir<StarJoinRow>(k);
    this->relation_indices = new std::unordered_map<int, std::vector<std::vector<GraphRow*>*>*>;
    this->batch = new StarJoinBatch(relation_indices, n);
}

Reservoir<StarJoinRow>* StarJoinAlgorithm::run(bool print_info) {
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

void StarJoinAlgorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    int src = next_int(line, &begin, lineSize, ',');
    int dst = next_int(line, &begin, lineSize, ',');
    auto* row = new GraphRow(src, dst);
    auto iter = relation_indices->find(src);
    if (iter != relation_indices->end()) {
        iter->second->at(sid)->emplace_back(row);
    } else {
        auto group = new std::vector<std::vector<GraphRow*>*>;
        for (int i=0; i<n; i++) {
            if (i == sid) {
                group->emplace_back(new std::vector<GraphRow*>{row});
            } else {
                group->emplace_back(new std::vector<GraphRow*>);
            }
        }
        relation_indices->emplace(src, group);
    }

    batch->set_row(row, sid);
    batch->fill(reservoir);
}
