#ifndef RESERVOIR_QZ_ATTRIBUTE_STORE_SALES_CUSTOMER_H
#define RESERVOIR_QZ_ATTRIBUTE_STORE_SALES_CUSTOMER_H

#include <unordered_map>
#include <vector>
#include "../tpc_ds_rows.h"

class QzAttributeItemStoreSales;
class QzAttributeCustomerHouseholdDemographics;

class QzAttributeStoreSalesCustomer {
public:
    ~QzAttributeStoreSalesCustomer();

    void set_left_attribute(QzAttributeItemStoreSales* attribute);
    void set_right_attribute(QzAttributeCustomerHouseholdDemographics* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<StoreSalesRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<CustomerRow*>*>* relation_index);

    void update_from_left(StoreSalesRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(CustomerRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(int key);
    unsigned long get_wlcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>* get_left_layer(int key);
    std::unordered_map<unsigned long, std::vector<CustomerRow*>*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);
    unsigned long get_right_layer_max_unit_size(int key);

    unsigned long get_propagation_cnt();

private:
    QzAttributeItemStoreSales* left_attribute;
    QzAttributeCustomerHouseholdDemographics* right_attribute;

    std::unordered_map<int, std::vector<StoreSalesRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<CustomerRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> wlcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>*> left_layers;
    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<CustomerRow*>*>*> right_layers;
    std::unordered_map<StoreSalesRow, unsigned long, StoreSalesRowHash> left_layer_indices;
    std::unordered_map<CustomerRow, unsigned long, CustomerRowHash> right_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;
    std::unordered_map<int, unsigned long> right_layer_max_unit_sizes;

    unsigned long propagation_cnt = 0;

    void propagate_to_left(int key, unsigned long old_count, unsigned long new_count);
    void propagate_to_right(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, StoreSalesRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, CustomerRow* row, unsigned long old_count, unsigned long new_count);
};


#endif //RESERVOIR_QZ_ATTRIBUTE_STORE_SALES_CUSTOMER_H
