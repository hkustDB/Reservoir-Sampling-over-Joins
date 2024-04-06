#ifndef RESERVOIR_DUMBBELL_ATTRIBUTE_MIDDLE_RIGHT_H
#define RESERVOIR_DUMBBELL_ATTRIBUTE_MIDDLE_RIGHT_H

#include <unordered_map>
#include <vector>
#include "dumbbell_rows.h"

class DumbbellAttributeLeftMiddle;

class DumbbellAttributeMiddleRight {
public:
    ~DumbbellAttributeMiddleRight();

    void set_left_attribute(DumbbellAttributeLeftMiddle* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<GraphRow*>*>* relation_index);

    void update_from_left(GraphRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(TriangleRow* row);

    unsigned long get_lcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<GraphRow*>*>* get_left_layer(int key);
    std::vector<TriangleRow*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);

private:
    DumbbellAttributeLeftMiddle* left_attribute;

    std::unordered_map<int, std::vector<GraphRow*>*>* left_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<GraphRow*>*>*> left_layers;
    std::unordered_map<int, std::vector<TriangleRow*>*> right_layers;
    std::unordered_map<GraphRow, unsigned long, GraphRowHash> left_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;

    void propagate_to_left(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, GraphRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, TriangleRow* row);
};


#endif
