#ifndef RESERVOIR_QX_ATTRIBUTE_STORE_RETURNS_CATALOG_SALES_H
#define RESERVOIR_QX_ATTRIBUTE_STORE_RETURNS_CATALOG_SALES_H

#include <unordered_map>
#include <vector>
#include "../tpc_ds_rows.h"

class QxAttributeStoreSalesStoreReturns;
class QxAttributeCatalogSalesDateDim;

class QxAttributeStoreReturnsCatalogSales {
public:
    ~QxAttributeStoreReturnsCatalogSales();

    void set_left_attribute(QxAttributeStoreSalesStoreReturns* attribute);
    void set_right_attribute(QxAttributeCatalogSalesDateDim* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<StoreReturnsRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<CatalogSalesRow*>*>* relation_index);

    void update_from_left(StoreReturnsRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(CatalogSalesRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(int key);
    unsigned long get_wlcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<StoreReturnsRow*>*>* get_left_layer(int key);
    std::unordered_map<unsigned long, std::vector<CatalogSalesRow*>*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);
    unsigned long get_right_layer_max_unit_size(int key);

private:
    QxAttributeStoreSalesStoreReturns* left_attribute;
    QxAttributeCatalogSalesDateDim* right_attribute;

    std::unordered_map<int, std::vector<StoreReturnsRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<CatalogSalesRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> wlcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<StoreReturnsRow*>*>*> left_layers;
    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<CatalogSalesRow*>*>*> right_layers;
    std::unordered_map<StoreReturnsRow, unsigned long, StoreReturnsRowHash> left_layer_indices;
    std::unordered_map<CatalogSalesRow, unsigned long, CatalogSalesRowHash> right_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;
    std::unordered_map<int, unsigned long> right_layer_max_unit_sizes;

    void propagate_to_left(int key, unsigned long old_count, unsigned long new_count);
    void propagate_to_right(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, StoreReturnsRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, CatalogSalesRow* row, unsigned long old_count, unsigned long new_count);
};


#endif