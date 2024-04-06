#ifndef RESERVOIR_QZ_FK_GROUP_ATTRIBUTE_MIDDLE_RIGHT_H
#define RESERVOIR_QZ_FK_GROUP_ATTRIBUTE_MIDDLE_RIGHT_H

#include <unordered_map>
#include <vector>
#include "qz_rows.h"

class QzFkGroupAttributeItemMiddle;

class QzFkGroupAttributeMiddleRight {
public:
    ~QzFkGroupAttributeMiddleRight();

    void set_left_attribute(QzFkGroupAttributeItemMiddle* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<QzFkRightRow*>*>* relation_index);

    void update_from_left(QzFkGroupMiddleRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(QzFkRightRow* row);

    unsigned long get_lcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>* get_left_layer(int key);
    std::vector<QzFkRightRow*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);

    unsigned long get_propagation_cnt();

private:
    QzFkGroupAttributeItemMiddle* left_attribute;

    std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<QzFkRightRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>*> left_layers;
    std::unordered_map<int, std::vector<QzFkRightRow*>*> right_layers;
    std::unordered_map<QzFkGroupMiddleRow, unsigned long, QzFkGroupMiddleRowHash> left_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;

    unsigned long propagation_cnt = 0;

    void propagate_to_left(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, QzFkGroupMiddleRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, QzFkRightRow* row);
};


#endif
