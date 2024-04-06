#ifndef RESERVOIR_QZ_FK_ATTRIBUTE_MIDDLE_RIGHT_H
#define RESERVOIR_QZ_FK_ATTRIBUTE_MIDDLE_RIGHT_H

#include <unordered_map>
#include <vector>
#include "qz_rows.h"

class QzFkAttributeItemMiddle;

class QzFkAttributeMiddleRight {
public:
    ~QzFkAttributeMiddleRight();

    void set_left_attribute(QzFkAttributeItemMiddle* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<QzFkMiddleRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<QzFkRightRow*>*>* relation_index);

    void update_from_left(QzFkMiddleRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(QzFkRightRow* row);

    unsigned long get_lcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<QzFkMiddleRow*>*>* get_left_layer(int key);
    std::vector<QzFkRightRow*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);

    unsigned long get_propagation_cnt();

private:
    QzFkAttributeItemMiddle* left_attribute;

    std::unordered_map<int, std::vector<QzFkMiddleRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<QzFkRightRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<QzFkMiddleRow*>*>*> left_layers;
    std::unordered_map<int, std::vector<QzFkRightRow*>*> right_layers;
    std::unordered_map<long, unsigned long> left_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;

    unsigned long propagation_cnt = 0;

    void propagate_to_left(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, QzFkMiddleRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, QzFkRightRow* row);
};


#endif