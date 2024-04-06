#ifndef RESERVOIR_DUMBBELL_ATTRIBUTE_LEFT_MIDDLE_H
#define RESERVOIR_DUMBBELL_ATTRIBUTE_LEFT_MIDDLE_H

#include <unordered_map>
#include <vector>
#include "dumbbell_rows.h"

class DumbbellAttributeMiddleRight;

class DumbbellAttributeLeftMiddle {
public:
    ~DumbbellAttributeLeftMiddle();

    void set_right_attribute(DumbbellAttributeMiddleRight* attribute);

    void set_right_relation_index(std::unordered_map<int, std::vector<GraphRow*>*>* relation_index);

    void update_from_left(TriangleRow* row);
    void update_from_right(GraphRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(int key);
    unsigned long get_wlcnt(int key);
    unsigned long get_rcnt(int key);

    std::vector<TriangleRow*>* get_left_layer(int key);
    std::unordered_map<unsigned long, std::vector<GraphRow*>*>* get_right_layer(int key);

    unsigned long get_right_layer_max_unit_size(int key);

private:
    DumbbellAttributeMiddleRight* right_attribute;

    std::unordered_map<int, std::vector<GraphRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> wlcnt;
    std::unordered_map<int, unsigned long> rcnt;

    std::unordered_map<int, std::vector<TriangleRow*>*> left_layers;
    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<GraphRow*>*>*> right_layers;
    std::unordered_map<GraphRow, unsigned long, GraphRowHash> right_layer_indices;
    std::unordered_map<int, unsigned long> right_layer_max_unit_sizes;

    void propagate_to_right(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, TriangleRow* row);
    void update_right_layer(int key, GraphRow* row, unsigned long old_count, unsigned long new_count);
};


#endif
