#ifndef RESERVOIR_QZ_FK_GROUP_ATTRIBUTE_ITEM_MIDDLE_H
#define RESERVOIR_QZ_FK_GROUP_ATTRIBUTE_ITEM_MIDDLE_H

#include <unordered_map>
#include <vector>
#include "qz_rows.h"

class QzFkGroupAttributeMiddleRight;

class QzFkGroupAttributeItemMiddle {
public:
    ~QzFkGroupAttributeItemMiddle();

    void set_right_attribute(QzFkGroupAttributeMiddleRight* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<ItemRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* relation_index);

    void update_from_left(ItemRow* row);
    void update_from_right(QzFkGroupMiddleRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(int key);
    unsigned long get_wlcnt(int key);
    unsigned long get_rcnt(int key);

    std::vector<ItemRow*>* get_left_layer(int key);
    std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>* get_right_layer(int key);

    unsigned long get_right_layer_max_unit_size(int key);

    unsigned long get_propagation_cnt();

private:
    QzFkGroupAttributeMiddleRight* right_attribute;

    std::unordered_map<int, std::vector<ItemRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> wlcnt;
    std::unordered_map<int, unsigned long> rcnt;

    std::unordered_map<int, std::vector<ItemRow*>*> left_layers;
    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>*> right_layers;
    std::unordered_map<QzFkGroupMiddleRow, unsigned long, QzFkGroupMiddleRowHash> right_layer_indices;
    std::unordered_map<int, unsigned long> right_layer_max_unit_sizes;

    unsigned long propagation_cnt = 0;

    void propagate_to_right(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, ItemRow* row);
    void update_right_layer(int key, QzFkGroupMiddleRow* row, unsigned long old_count, unsigned long new_count);
};


#endif 
