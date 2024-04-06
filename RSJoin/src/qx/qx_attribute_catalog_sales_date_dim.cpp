#include "../../include/qx/qx_attribute_store_returns_catalog_sales.h"
#include "../../include/qx/qx_attribute_catalog_sales_date_dim.h"

QxAttributeCatalogSalesDateDim::~QxAttributeCatalogSalesDateDim() {
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

void QxAttributeCatalogSalesDateDim::set_left_attribute(QxAttributeStoreReturnsCatalogSales *attribute) {
    this->left_attribute = attribute;
}

void QxAttributeCatalogSalesDateDim::set_left_relation_index(std::unordered_map<int, std::vector<CatalogSalesRow*>*>* relation_index) {
    this->left_relation_index = relation_index;
}

void QxAttributeCatalogSalesDateDim::set_right_relation_index(std::unordered_map<int, std::vector<DateDimRow*>*>* relation_index) {
    this->right_relation_index = relation_index;
}

void QxAttributeCatalogSalesDateDim::update_from_left(CatalogSalesRow *row, unsigned long old_count, unsigned long new_count) {
    int key = row->cs_sold_date_sk;
    if (lcnt.find(key) != lcnt.end()) {
        lcnt.at(key) = (lcnt.at(key) - old_count + new_count);
    } else {
        lcnt.emplace(key, new_count);
    }

    if (wlcnt.find(key) == wlcnt.end()) {
        wlcnt.emplace(key, new_count);
    } else if (lcnt.at(key) > wlcnt.at(key)) {
        while (lcnt.at(key) > wlcnt.at(key)) {
            wlcnt.at(key) *= 2;
        }
    }

    update_left_layer(key, row, old_count, new_count);
}

void QxAttributeCatalogSalesDateDim::update_from_right(DateDimRow *row, unsigned long old_count, unsigned long new_count) {
    int key = row->d_date_sk;
    if (rcnt.find(key) != rcnt.end()) {
        rcnt.at(key) = (rcnt.at(key) - old_count + new_count);
    } else {
        rcnt.emplace(key, new_count);
    }

    if (wrcnt.find(key) == wrcnt.end()) {
        wrcnt.emplace(key, new_count);
        propagate_to_left(key, 0, new_count);
    } else if (rcnt.at(key) > wrcnt.at(key)) {
        unsigned long previous_value = wrcnt.at(key);
        while (rcnt.at(key) > wrcnt.at(key)) {
            wrcnt.at(key) *= 2;
        }
        propagate_to_left(key, previous_value, wrcnt.at(key));
    }

    update_right_layer(key, row, old_count, new_count);
}

unsigned long QxAttributeCatalogSalesDateDim::get_lcnt(int key) {
    auto iter = lcnt.find(key);
    if (iter != lcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QxAttributeCatalogSalesDateDim::get_wlcnt(int key) {
    auto iter = wlcnt.find(key);
    if (iter != wlcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QxAttributeCatalogSalesDateDim::get_rcnt(int key) {
    auto iter = rcnt.find(key);
    if (iter != rcnt.end())
        return iter->second;
    else
        return 0;
}

unsigned long QxAttributeCatalogSalesDateDim::get_wrcnt(int key) {
    auto iter = wrcnt.find(key);
    if (iter != wrcnt.end())
        return iter->second;
    else
        return 0;
}

std::unordered_map<unsigned long, std::vector<CatalogSalesRow*>*>* QxAttributeCatalogSalesDateDim::get_left_layer(int key) {
    return left_layers.at(key);
}

std::unordered_map<unsigned long, std::vector<DateDimRow*>*>* QxAttributeCatalogSalesDateDim::get_right_layer(int key) {
    return right_layers.at(key);
}

unsigned long QxAttributeCatalogSalesDateDim::get_left_layer_max_unit_size(int key) {
    return left_layer_max_unit_sizes.at(key);
}

unsigned long QxAttributeCatalogSalesDateDim::get_right_layer_max_unit_size(int key) {
    return right_layer_max_unit_sizes.at(key);
}

void QxAttributeCatalogSalesDateDim::propagate_to_left(int key, unsigned long old_count, unsigned long new_count) {
    if (left_attribute != nullptr) {
        auto iter = left_relation_index->find(key);
        if (iter != left_relation_index->end()) {
            for (auto &row : *iter->second) {
                left_attribute->update_from_right(row, old_count, new_count);
            }
        }
    }
}

void QxAttributeCatalogSalesDateDim::update_left_layer(int key, CatalogSalesRow *row, unsigned long old_count, unsigned long new_count) {
    auto iter = left_layers.find(key);
    if (iter != left_layers.end()) {
        std::unordered_map<unsigned long, std::vector<CatalogSalesRow*>*>* map = iter->second;
        if (old_count > 0) {
            // remove in previous layer
            std::vector<CatalogSalesRow*>* old_layer = map->at(old_count);
            if (old_layer->size() == 1) {
                map->erase(old_count);
            } else {
                // swap with the last element
                unsigned long last_index = old_layer->size() - 1;
                CatalogSalesRow* lastElement = old_layer->at(last_index);
                unsigned long row_index = left_layer_indices.at(*row);
                old_layer->at(row_index) = lastElement;
                left_layer_indices.at(*lastElement) = row_index;
                old_layer->pop_back();
            }
        }

        auto iter2 = map->find(new_count);
        if (iter2 != map->end()) {
            iter2->second->emplace_back(row);
            left_layer_indices[*row] = (iter2->second->size() - 1);
        } else {
            auto* layer = new std::vector<CatalogSalesRow*>{row};
            map->emplace(new_count, layer);
            left_layer_indices[*row] = 0;
            if (new_count > left_layer_max_unit_sizes.at(key)) {
                left_layer_max_unit_sizes.at(key) = new_count;
            }
        }
    } else {
        auto* layer = new std::vector<CatalogSalesRow*>{row};
        left_layers.emplace(key, new std::unordered_map<unsigned long, std::vector<CatalogSalesRow*>*> {{new_count, layer}});
        left_layer_indices[*row] = 0;
        left_layer_max_unit_sizes.emplace(key, new_count);
    }
}

void QxAttributeCatalogSalesDateDim::update_right_layer(int key, DateDimRow *row, unsigned long old_count, unsigned long new_count) {
    auto iter = right_layers.find(key);
    if (iter != right_layers.end()) {
        std::unordered_map<unsigned long, std::vector<DateDimRow*>*>* map = iter->second;
        if (old_count > 0) {
            // remove in previous layer
            std::vector<DateDimRow*>* old_layer = map->at(old_count);
            if (old_layer->size() == 1) {
                map->erase(old_count);
            } else {
                // swap with the last element
                unsigned long last_index = old_layer->size() - 1;
                DateDimRow* lastElement = old_layer->at(last_index);
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
            auto* layer = new std::vector<DateDimRow*>{row};
            map->emplace(new_count, layer);
            right_layer_indices[*row] = 0;
            if (new_count > right_layer_max_unit_sizes.at(key)) {
                right_layer_max_unit_sizes.at(key) = new_count;
            }
        }
    } else {
        auto* layer = new std::vector<DateDimRow*>{row};
        right_layers.emplace(key, new std::unordered_map<unsigned long, std::vector<DateDimRow*>*> {{new_count, layer}});
        right_layer_indices[*row] = 0;
        right_layer_max_unit_sizes.emplace(key, new_count);
    }
}