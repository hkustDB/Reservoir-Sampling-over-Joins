#ifndef RESERVOIR_QY_ATTRIBUTE_HOUSEHOLD_DEMOGRAPHICS_CUSTOMER_H
#define RESERVOIR_QY_ATTRIBUTE_HOUSEHOLD_DEMOGRAPHICS_CUSTOMER_H


#include <unordered_map>
#include <vector>
#include "../tpc_ds_rows.h"

class QyAttributeHouseholdDemographicsHouseholdDemographics;

class QyAttributeHouseholdDemographicsCustomer {
public:
    ~QyAttributeHouseholdDemographicsCustomer();

    void set_left_attribute(QyAttributeHouseholdDemographicsHouseholdDemographics* attribute);

    void set_left_relation_index(std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<int, std::vector<CustomerRow*>*>* relation_index);

    void update_from_left(HouseholdDemographicsRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(CustomerRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(int key);
    unsigned long get_wlcnt(int key);
    unsigned long get_rcnt(int key);
    unsigned long get_wrcnt(int key);

    std::unordered_map<unsigned long, std::vector<HouseholdDemographicsRow*>*>* get_left_layer(int key);
    std::unordered_map<unsigned long, std::vector<CustomerRow*>*>* get_right_layer(int key);

    unsigned long get_left_layer_max_unit_size(int key);
    unsigned long get_right_layer_max_unit_size(int key);

private:
    QyAttributeHouseholdDemographicsHouseholdDemographics* left_attribute;

    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* left_relation_index;
    std::unordered_map<int, std::vector<CustomerRow*>*>* right_relation_index;

    std::unordered_map<int, unsigned long> lcnt;
    std::unordered_map<int, unsigned long> wlcnt;
    std::unordered_map<int, unsigned long> rcnt;
    std::unordered_map<int, unsigned long> wrcnt;

    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<HouseholdDemographicsRow*>*>*> left_layers;
    std::unordered_map<int, std::unordered_map<unsigned long, std::vector<CustomerRow*>*>*> right_layers;
    std::unordered_map<HouseholdDemographicsRow, unsigned long, HouseholdDemographicsRowHash> left_layer_indices;
    std::unordered_map<CustomerRow, unsigned long, CustomerRowHash> right_layer_indices;
    std::unordered_map<int, unsigned long> left_layer_max_unit_sizes;
    std::unordered_map<int, unsigned long> right_layer_max_unit_sizes;

    void propagate_to_left(int key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(int key, HouseholdDemographicsRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(int key, CustomerRow* row, unsigned long old_count, unsigned long new_count);
};

#endif