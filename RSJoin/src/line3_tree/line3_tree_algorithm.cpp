#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/acyclic/join_tree.h"
#include "../../include/line3_tree/line3_tree_algorithm.h"
#include "../../include/line3_tree/line3_tree_rows.h"

Line3TreeAlgorithm::~Line3TreeAlgorithm() {
    delete tree1_node_r;
    delete tree1_node_s;
    delete tree1_node_t;
    delete tree2_node_r;
    delete tree2_node_s;
    delete tree2_node_t;
    delete tree3_node_r;
    delete tree3_node_s;
    delete tree3_node_t;
    delete reservoir;

    for (auto graph_row : graph_rows) {
        delete graph_row;
    }
}

void Line3TreeAlgorithm::init(const std::string &file, int k) {
    this->input = file;
    this->reservoir = new Reservoir<Line3TreeRow>(k);

    // init join tree rooted at R
    this->tree1_node_r = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row1 = row; });
    this->tree1_node_s = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row2 = row; });
    this->tree1_node_t = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row3 = row; });
    // R.dst = S.src
    auto tree1_node_s_id = tree1_node_r->register_child(tree1_node_s, [](Line3TreeGraphRow* row){ return row->dst; });
    tree1_node_s->register_parent(tree1_node_r, tree1_node_s_id, [](Line3TreeGraphRow* row){ return row->src;});

    // S.dst = T.src
    auto tree1_node_t_id = tree1_node_s->register_child(tree1_node_t, [](Line3TreeGraphRow* row){ return row->dst; });
    tree1_node_t->register_parent(tree1_node_s, tree1_node_t_id, [](Line3TreeGraphRow* row){ return row->src;});

    // init join tree rooted at S
    this->tree2_node_r = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row1 = row; });
    this->tree2_node_s = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row2 = row; });
    this->tree2_node_t = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row3 = row; });
    // S.src = R.dst
    auto tree2_node_r_id = tree2_node_s->register_child(tree2_node_r, [](Line3TreeGraphRow* row){ return row->src; });
    tree2_node_r->register_parent(tree2_node_s, tree2_node_r_id, [](Line3TreeGraphRow* row){ return row->dst;});

    // S.dst = T.src
    auto tree2_node_t_id = tree2_node_s->register_child(tree2_node_t, [](Line3TreeGraphRow* row){ return row->dst; });
    tree2_node_t->register_parent(tree2_node_s, tree2_node_t_id, [](Line3TreeGraphRow* row){ return row->src;});

    // init join tree rooted at S
    this->tree3_node_r = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row1 = row; });
    this->tree3_node_s = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row2 = row; });
    this->tree3_node_t = new Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>(
                                              [](Line3TreeRow* buffer, Line3TreeGraphRow* row){ buffer->row3 = row; });

    // T.src = S.dst
    auto tree3_node_s_id = tree3_node_t->register_child(tree3_node_s, [](Line3TreeGraphRow* row){ return row->src; });
    tree3_node_s->register_parent(tree3_node_t, tree3_node_s_id, [](Line3TreeGraphRow* row){ return row->dst; });

    // S.src = R.dst
    auto tree3_node_r_id = tree3_node_s->register_child(tree3_node_r, [](Line3TreeGraphRow* row){ return row->src; });
    tree3_node_r->register_parent(tree3_node_s, tree3_node_r_id, [](Line3TreeGraphRow* row){ return row->dst;});
}

Reservoir<Line3TreeRow> *Line3TreeAlgorithm::run(bool print_info) {
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

void Line3TreeAlgorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    if (line.at(0) == '-')
        return;

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    switch (sid) {
        case 0: {
            // insert into R
            unsigned long src = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long dst = next_unsigned_long(line, &begin, lineSize, ',');
            auto graph_row = new Line3TreeGraphRow(src, dst);
            graph_rows.emplace_back(graph_row);
            auto batch = tree1_node_r->insert(graph_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            tree2_node_r->insert(graph_row);
            tree3_node_r->insert(graph_row);
            break;
        }
        case 1: {
            // insert into S
            unsigned long src = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long dst = next_unsigned_long(line, &begin, lineSize, ',');
            auto graph_row = new Line3TreeGraphRow(src, dst);
            graph_rows.emplace_back(graph_row);
            tree1_node_s->insert(graph_row);
            auto batch = tree2_node_s->insert(graph_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            tree3_node_s->insert(graph_row);
            break;
        }
        case 2: {
            // insert into T
            unsigned long src = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long dst = next_unsigned_long(line, &begin, lineSize, ',');
            auto graph_row = new Line3TreeGraphRow(src, dst);
            graph_rows.emplace_back(graph_row);
            tree1_node_t->insert(graph_row);
            tree2_node_t->insert(graph_row);
            auto batch = tree3_node_t->insert(graph_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            break;
        }
    }
}