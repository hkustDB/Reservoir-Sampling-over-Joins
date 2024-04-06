#ifndef RESERVOIR_QZ_ATTRIBUTE_ITEM_ITEM_H
#define RESERVOIR_QZ_ATTRIBUTE_ITEM_ITEM_H

#include <unordered_map>
#include <vector>
#include "../tpc_ds_rows.h"

class QzAttributeItemStoreSales;

class QzAttributeItemItem {
public:
    ~QzAttributeItemItem();

    void set_right_attribute(QzAttributeItemStoreSales* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<ItemRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<ItemRow*>*>* relation_index);

    void update_from_left(ItemRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(ItemRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(int key);
    unsigned long get_wlcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<ItemRow*>*>* get_left_layer(int key);
    std::unordered_map<unsigned long, std::vector<ItemRow*>*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);
    unsigned long get_right_layer_max_unit_size(int key);

    unsigned long get_propagation_cnt();

private:
    QzAttributeItemStoreSales* right_attribute;

    std::unordered_map<int, std::vector<ItemRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<ItemRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> wlcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<ItemRow*>*>*> left_layers;
    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<ItemRow*>*>*> right_layers;
    std::unordered_map<ItemRow, unsigned long, ItemRowHash> left_layer_indices;
    std::unordered_map<ItemRow, unsigned long, ItemRowHash> right_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;
    std::unordered_map<int, unsigned long> right_layer_max_unit_sizes;

    unsigned long propagation_cnt = 0;

    void propagate_to_right(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, ItemRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, ItemRow* row, unsigned long old_count, unsigned long new_count);
};


#endif
