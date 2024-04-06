#include "../../include/qz/qz_fk_group_attribute_item_middle.h"
#include "../../include/qz/qz_fk_group_attribute_middle_right.h"

QzFkGroupAttributeItemMiddle::~QzFkGroupAttributeItemMiddle() {
    for (auto &left_layer : left_layers) {
        auto vec = left_layer.second;
        vec->clear();
        delete vec;
    }
    left_layers.clear();

    for (auto &right_layer : right_layers) {
        auto layer = right_layer.second;
        for (auto & iter2 : *layer) {
            auto vec = iter2.second;
            vec->clear();
            delete vec;
        }
        layer->clear();
        delete layer;
    }
    right_layers.clear();
}

void QzFkGroupAttributeItemMiddle::set_right_attribute(QzFkGroupAttributeMiddleRight *attribute) {
    this->right_attribute = attribute;
}

void QzFkGroupAttributeItemMiddle::set_left_relation_index(std::unordered_map<int, std::vector<ItemRow*>*>* relation_index) {
    this->left_relation_index = relation_index;
}

void QzFkGroupAttributeItemMiddle::set_right_relation_index(std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* relation_index) {
    this->right_relation_index = relation_index;
}

void QzFkGroupAttributeItemMiddle::update_from_left(ItemRow* row) {
    propagation_cnt += 1;
    int key = row->i_category_id;
    unsigned long lcnt_value;
    auto iter1 = lcnt.find(key);
    if (iter1 != lcnt.end()) {
        lcnt_value = iter1->second + 1;
        iter1->second = lcnt_value;
    } else {
        lcnt.emplace(key, 1);
        lcnt_value = 1;
    }

    auto iter2 = wlcnt.find(key);
    if (iter2 == wlcnt.end()) {
        wlcnt.emplace(key, 1);
        propagate_to_right(key, 0, 1);
    } else {
        unsigned long wlcnt_value = iter2->second;
        if (lcnt_value > wlcnt_value) {
            wlcnt.at(key) = wlcnt_value * 2;
            propagate_to_right(key, wlcnt_value, wlcnt_value * 2);
        }
    }

    update_left_layer(key, row);
}

void QzFkGroupAttributeItemMiddle::update_from_right(QzFkGroupMiddleRow* row, unsigned long old_count, unsigned long new_count) {
    propagation_cnt += 1;
    int key = row->i_category_id;
    auto iter1 = rcnt.find(key);
    if (iter1 != rcnt.end()) {
        iter1->second += (new_count - old_count);
    } else {
        rcnt.emplace(key, new_count);
    }

    update_right_layer(key, row, old_count, new_count);
}

unsigned long QzFkGroupAttributeItemMiddle::get_lcnt(int key) {
    auto iter = lcnt.find(key);
    if (iter != lcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QzFkGroupAttributeItemMiddle::get_wlcnt(int key) {
    auto iter = wlcnt.find(key);
    if (iter != wlcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QzFkGroupAttributeItemMiddle::get_rcnt(int key) {
    auto iter = rcnt.find(key);
    if (iter != rcnt.end())
        return iter->second;
    else
        return 0;
}

std::vector<ItemRow*>* QzFkGroupAttributeItemMiddle::get_left_layer(int key) {
    return left_layers.at(key);
}

std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>* QzFkGroupAttributeItemMiddle::get_right_layer(int key) {
    return right_layers.at(key);
}

unsigned long QzFkGroupAttributeItemMiddle::get_right_layer_max_unit_size(int key) {
    return right_layer_max_unit_sizes.at(key);
}

void QzFkGroupAttributeItemMiddle::propagate_to_right(int key, unsigned long old_count, unsigned long new_count) {
    if (right_attribute != nullptr) {
        auto iter = right_relation_index->find(key);
        if (iter != right_relation_index->end()) {
            for (auto &row : *iter->second) {
                right_attribute->update_from_left(row, old_count * row->wcnt, new_count * row->wcnt);
            }
        }
    }
}

void QzFkGroupAttributeItemMiddle::update_left_layer(int key, ItemRow* row) {
    auto iter = left_layers.find(key);
    if (iter != left_layers.end()) {
        iter->second->emplace_back(row);
    } else {
        left_layers.emplace(key, new std::vector<ItemRow*>{row});
    }
}

void QzFkGroupAttributeItemMiddle::update_right_layer(int key, QzFkGroupMiddleRow* row, unsigned long old_count, unsigned long new_count) {
    auto iter = right_layers.find(key);
    if (iter != right_layers.end()) {
        std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>* map = iter->second;
        if (old_count > 0) {
            // remove in previous layer
            std::vector<QzFkGroupMiddleRow*>* old_layer = map->at(old_count);
            if (old_layer->size() == 1) {
                map->erase(old_count);
            } else {
                // swap with the last element
                unsigned long last_index = old_layer->size() - 1;
                QzFkGroupMiddleRow* lastElement = old_layer->at(last_index);
                unsigned long row_index = right_layer_indices.at(*row);
                old_layer->at(row_index) = lastElement;
                right_layer_indices.at(*lastElement) = row_index;
                old_layer->pop_back();
            }
        }

        auto iter2 = map->find(new_count);
        if (iter2 != map->end()) {
            iter2->second->emplace_back(row);
            right_layer_indices[*row] = (iter2->second->size() - 1);
        } else {
            auto* layer = new std::vector<QzFkGroupMiddleRow*>{row};
            map->emplace(new_count, layer);
            right_layer_indices[*row] = 0;
            if (new_count > right_layer_max_unit_sizes.at(key)) {
                right_layer_max_unit_sizes.at(key) = new_count;
            }
        }
    } else {
        auto* layer = new std::vector<QzFkGroupMiddleRow*>{row};
        right_layers.emplace(key, new std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*> {{new_count, layer}});
        right_layer_indices[*row] = 0;
        right_layer_max_unit_sizes.emplace(key, new_count);
    }
}

unsigned long QzFkGroupAttributeItemMiddle::get_propagation_cnt() {
    return propagation_cnt;
}
