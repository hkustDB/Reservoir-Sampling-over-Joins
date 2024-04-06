#include "../../include/qz/qz_fk_attribute_item_middle.h"
#include "../../include/qz/qz_fk_attribute_middle_right.h"

QzFkAttributeMiddleRight::~QzFkAttributeMiddleRight() {
    for (auto &left_layer : left_layers) {
        auto layer = left_layer.second;
        for (auto & iter2 : *layer) {
            auto vec = iter2.second;
            vec->clear();
            delete vec;
        }
        layer->clear();
        delete layer;
    }
    left_layers.clear();

    for (auto &right_layer : right_layers) {
        auto vec = right_layer.second;
        vec->clear();
        delete vec;
    }
    right_layers.clear();
}

void QzFkAttributeMiddleRight::set_left_attribute(QzFkAttributeItemMiddle *attribute) {
    this->left_attribute = attribute;
}

void QzFkAttributeMiddleRight::set_left_relation_index(std::unordered_map<int, std::vector<QzFkMiddleRow*>*>* relation_index) {
    this->left_relation_index = relation_index;
}

void QzFkAttributeMiddleRight::set_right_relation_index(std::unordered_map<int, std::vector<QzFkRightRow*>*>* relation_index) {
    this->right_relation_index = relation_index;
}

void QzFkAttributeMiddleRight::update_from_left(QzFkMiddleRow* row, unsigned long old_count, unsigned long new_count) {
    propagation_cnt += 1;
    int key = row->household_demographics_row->hd_income_band_sk;
    auto iter1 = lcnt.find(key);
    if (iter1 != lcnt.end()) {
        lcnt.at(key) += (new_count - old_count);
    } else {
        lcnt.emplace(key, new_count);
    }

    update_left_layer(key, row, old_count, new_count);
}

void QzFkAttributeMiddleRight::update_from_right(QzFkRightRow* row) {
    propagation_cnt += 1;
    int key = row->household_demographics_row2->hd_income_band_sk;
    unsigned rcnt_value;
    auto iter1 = rcnt.find(key);
    if (iter1 != rcnt.end()) {
        rcnt_value = rcnt.at(key) + 1;
        rcnt.at(key) = rcnt_value;
    } else {
        rcnt.emplace(key, 1);
        rcnt_value = 1;
    }

    auto iter2 = wrcnt.find(key);
    if (iter2 == wrcnt.end()) {
        wrcnt.emplace(key, 1);
        propagate_to_left(key, 0, 1);
    } else {
        unsigned long wrcnt_value = iter2->second;
        if (rcnt_value > wrcnt_value) {
            wrcnt.at(key) = wrcnt_value * 2;
            propagate_to_left(key, wrcnt_value, wrcnt_value * 2);
        }
    }

    update_right_layer(key, row);
}

unsigned long QzFkAttributeMiddleRight::get_lcnt(int key) {
    auto iter = lcnt.find(key);
    if (iter != lcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QzFkAttributeMiddleRight::get_rcnt(int key) {
    auto iter = rcnt.find(key);
    if (iter != rcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QzFkAttributeMiddleRight::get_wrcnt(int key) {
    auto iter = wrcnt.find(key);
    if (iter != wrcnt.end())
        return iter->second;
    else
        return 0;
}

std::unordered_map<unsigned long, std::vector<QzFkMiddleRow*>*>* QzFkAttributeMiddleRight::get_left_layer(int key) {
    return left_layers.at(key);
}

std::vector<QzFkRightRow*>* QzFkAttributeMiddleRight::get_right_layer(int key) {
    return right_layers.at(key);
}

unsigned long QzFkAttributeMiddleRight::get_left_layer_max_unit_size(int key) {
    return left_layer_max_unit_sizes.at(key);
}

void QzFkAttributeMiddleRight::propagate_to_left(int key, unsigned long old_count, unsigned long new_count) {
    if (left_attribute != nullptr) {
        auto iter = left_relation_index->find(key);
        if (iter != left_relation_index->end()) {
            for (auto &row : *iter->second) {
                left_attribute->update_from_right(row, old_count, new_count);
            }
        }
    }
}

void QzFkAttributeMiddleRight::update_left_layer(int key, QzFkMiddleRow* row, unsigned long old_count, unsigned long new_count) {
    auto iter = left_layers.find(key);
    if (iter != left_layers.end()) {
        std::unordered_map<unsigned long, std::vector<QzFkMiddleRow*>*>* map = iter->second;
        if (old_count > 0) {
            // remove in previous layer
            std::vector<QzFkMiddleRow*>* old_layer = map->at(old_count);
            if (old_layer->size() == 1) {
                map->erase(old_count);
            } else {
                // swap with the last element
                unsigned long last_index = old_layer->size() - 1;
                QzFkMiddleRow* lastElement = old_layer->at(last_index);
                unsigned long row_index = left_layer_indices.at(row->store_sales_row->ss_item_sk_with_ticket_number);
                old_layer->at(row_index) = lastElement;
                left_layer_indices.at(lastElement->store_sales_row->ss_item_sk_with_ticket_number) = row_index;
                old_layer->pop_back();
            }
        }

        auto iter2 = map->find(new_count);
        if (iter2 != map->end()) {
            iter2->second->emplace_back(row);
            left_layer_indices[row->store_sales_row->ss_item_sk_with_ticket_number] = (iter2->second->size() - 1);
        } else {
            auto* layer = new std::vector<QzFkMiddleRow*>{row};
            map->emplace(new_count, layer);
            left_layer_indices[row->store_sales_row->ss_item_sk_with_ticket_number] = 0;
            if (new_count > left_layer_max_unit_sizes.at(key)) {
                left_layer_max_unit_sizes.at(key) = new_count;
            }
        }
    } else {
        auto* layer = new std::vector<QzFkMiddleRow*>{row};
        left_layers.emplace(key, new std::unordered_map<unsigned long, std::vector<QzFkMiddleRow*>*> {{new_count, layer}});
        left_layer_indices[row->store_sales_row->ss_item_sk_with_ticket_number] = 0;
        left_layer_max_unit_sizes.emplace(key, new_count);
    }
}

void QzFkAttributeMiddleRight::update_right_layer(int key, QzFkRightRow* row) {
    auto iter = right_layers.find(key);
    if (iter != right_layers.end()) {
        iter->second->emplace_back(row);
    } else {
        right_layers.emplace(key, new std::vector<QzFkRightRow*>{row});
    }
}

unsigned long QzFkAttributeMiddleRight::get_propagation_cnt() {
    return propagation_cnt;
}