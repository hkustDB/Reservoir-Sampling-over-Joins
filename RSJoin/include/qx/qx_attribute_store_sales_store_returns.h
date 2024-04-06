#ifndef RESERVOIR_QX_ATTRIBUTE_STORE_SALES_STORE_RETURNS_H
#define RESERVOIR_QX_ATTRIBUTE_STORE_SALES_STORE_RETURNS_H

#include <unordered_map>
#include <vector>
#include "../tpc_ds_rows.h"

class QxAttributeDateDimStoreSales;
class QxAttributeStoreReturnsCatalogSales;

class QxAttributeStoreSalesStoreReturns {
public:
    ~QxAttributeStoreSalesStoreReturns();

    void set_left_attribute(QxAttributeDateDimStoreSales* attribute);
    void set_right_attribute(QxAttributeStoreReturnsCatalogSales* attribute);

    void set_left_relation_index(std::unordered_map<long, std::vector<StoreSalesRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<long, std::vector<StoreReturnsRow*>*>* relation_index);

    void update_from_left(StoreSalesRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(StoreReturnsRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(long key);
    unsigned long get_wlcnt(long key);
    unsigned long get_rcnt(long key);
    unsigned long get_wrcnt(long key);

    std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>* get_left_layer(long key);
    std::unordered_map<unsigned long, std::vector<StoreReturnsRow*>*>* get_right_layer(long key);

    unsigned long get_left_layer_max_unit_size(long key);
    unsigned long get_right_layer_max_unit_size(long key);

private:
    QxAttributeDateDimStoreSales* left_attribute;
    QxAttributeStoreReturnsCatalogSales* right_attribute;

    std::unordered_map<long, std::vector<StoreSalesRow*>*>* left_relation_index;
    std::unordered_map<long, std::vector<StoreReturnsRow*>*>* right_relation_index;

    std::unordered_map<long, unsigned long> lcnt;
    std::unordered_map<long, unsigned long> wlcnt;
    std::unordered_map<long, unsigned long> rcnt;
    std::unordered_map<long, unsigned long> wrcnt;

    std::unordered_map<long, std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>*> left_layers;
    std::unordered_map<long, std::unordered_map<unsigned long, std::vector<StoreReturnsRow*>*>*> right_layers;
    std::unordered_map<StoreSalesRow, unsigned long, StoreSalesRowHash> left_layer_indices;
    std::unordered_map<StoreReturnsRow, unsigned long, StoreReturnsRowHash> right_layer_indices;
    std::unordered_map<long, unsigned long> left_layer_max_unit_sizes;
    std::unordered_map<long, unsigned long> right_layer_max_unit_sizes;

    void propagate_to_left(long key, unsigned long old_count, unsigned long new_count);
    void propagate_to_right(long key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(long key, StoreSalesRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(long key, StoreReturnsRow* row, unsigned long old_count, unsigned long new_count);
};

#endif
