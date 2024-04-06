#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/line_join/line_join_algorithm.h"
#include "../../include/line_join/line_join_attribute.h"
#include "../../include/line_join/line_join_row.h"
#include "../../include/line_join/line_join_batch.h"


LineJoinAlgorithm::~LineJoinAlgorithm() {
    delete reservoir;

    for (int i=0; i<relation_indices->size(); i++) {
        auto relation = relation_indices->at(i);
        for (auto &iter : *relation) {
            auto vec = iter.second;
            if (i % 2 == 0 || i == relation_indices->size()-1) {
                for (auto row: *vec) {
                    delete row;
                }
            }
            vec->clear();
            delete vec;
        }
        relation->clear();
        delete relation;
    }
    relation_indices->clear();
    delete relation_indices;

    for (auto p: *line_join_attributes) {
        delete p;
    }
    line_join_attributes->clear();
    delete line_join_attributes;
}

void LineJoinAlgorithm::init(int length, const std::string &file, int k) {
    this->n = length;
    this->input = file;
    this->reservoir = new Reservoir<LineJoinRow>(k);
    this->relation_indices = new std::vector<std::unordered_map<int, std::vector<GraphRow*>*>*>;
    this->line_join_attributes = new std::vector<LineJoinAttribute*>;

    for (int i=0; i<2*n-2; i++) {
        relation_indices->emplace_back(new std::unordered_map<int, std::vector<GraphRow*>*>);
    }

    for (int i=0; i<n-1; i++) {
        line_join_attributes->emplace_back(new LineJoinAttribute());
    }

    for (int i=0; i<n-1; i++) {
        if (i > 0) {
            line_join_attributes->at(i)->set_left_attribute(line_join_attributes->at(i-1));
        }

        if (i < n-2) {
            line_join_attributes->at(i)->set_right_attribute(line_join_attributes->at(i+1));
        }

        line_join_attributes->at(i)->set_left_relation_index(relation_indices->at(2*i));
        line_join_attributes->at(i)->set_right_relation_index(relation_indices->at(2*i+1));
    }
}

Reservoir<LineJoinRow>* LineJoinAlgorithm::run(bool print_info) {
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

void LineJoinAlgorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    int src = next_int(line, &begin, lineSize, ',');
    int dst = next_int(line, &begin, lineSize, ',');
    auto* row = new GraphRow(src, dst);
    if (sid == 0) {
        auto relation_index = relation_indices->at(0);
        auto iter = relation_index->find(dst);
        if (iter != relation_index->end()) {
            iter->second->emplace_back(row);
        } else {
            relation_index->emplace(dst, new std::vector<GraphRow *>{row});
        }
        line_join_attributes->at(0)->update_from_left(row, 0, 1);

        auto batch = LineJoinBatch(line_join_attributes, n, row, sid);
        batch.fill(reservoir);
    } else if (sid == n-1) {
        auto relation_index = relation_indices->at(2*n-3);
        auto iter = relation_index->find(src);
        if (iter != relation_index->end()) {
            iter->second->emplace_back(row);
        } else {
            relation_index->emplace(src, new std::vector<GraphRow *>{row});
        }
        line_join_attributes->at(n-2)->update_from_right(row, 0, 1);

        auto batch = LineJoinBatch(line_join_attributes, n, row, sid);
        batch.fill(reservoir);
    } else if (sid < n-1) {
        auto left_relation_index = relation_indices->at(sid*2-1);
        auto right_relation_index = relation_indices->at(sid*2);
        auto iter1 = left_relation_index->find(src);
        if (iter1 != left_relation_index->end()) {
            iter1->second->emplace_back(row);
        } else {
            left_relation_index->emplace(src, new std::vector<GraphRow *>{row});
        }

        auto iter2 = right_relation_index->find(dst);
        if (iter2 != right_relation_index->end()) {
            iter2->second->emplace_back(row);
        } else {
            right_relation_index->emplace(dst, new std::vector<GraphRow *>{row});
        }

        auto left_attribute = line_join_attributes->at(sid-1);
        auto right_attribute = line_join_attributes->at(sid);
        unsigned long wlcnt = left_attribute->get_wlcnt(src);
        if (wlcnt > 0) {
            right_attribute->update_from_left(row, 0, wlcnt);
        }

        unsigned long wrcnt = right_attribute->get_wrcnt(dst);
        if (wrcnt > 0) {
            left_attribute->update_from_right(row, 0, wrcnt);
        }

        auto batch = LineJoinBatch(line_join_attributes, n, row, sid);
        batch.fill(reservoir);
    }
}
