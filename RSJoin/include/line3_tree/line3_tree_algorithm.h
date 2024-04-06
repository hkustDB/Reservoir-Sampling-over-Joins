#ifndef RESERVOIR_LINE3_TREE_ALGORITHM_H
#define RESERVOIR_LINE3_TREE_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include "line3_tree_rows.h"

template<typename T>
class Reservoir;
template<typename R, typename T, typename H>
class Relation;

class Line3TreeAlgorithm {
public:
    ~Line3TreeAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<Line3TreeRow>* run(bool print_info);

private:
    // join_tree rooted at R
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree1_node_r;
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree1_node_s;
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree1_node_t;

    // join_tree rooted at S
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree2_node_r;
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree2_node_s;
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree2_node_t;

    // join_tree rooted at T
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree3_node_r;
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree3_node_s;
    Relation<Line3TreeRow, Line3TreeGraphRow, Line3TreeGraphRowHash>* tree3_node_t;

    std::string input;
    Reservoir<Line3TreeRow>* reservoir = nullptr;

    // for destruction
    std::vector<Line3TreeGraphRow*> graph_rows;

    void process(const std::string& line);
};


#endif
