#ifndef RESERVOIR_QY_BATCH_H
#define RESERVOIR_QY_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QyRow;
struct StoreSalesRow;
struct CustomerRow;
struct HouseholdDemographicsRow;
class QyAttributeStoreSalesCustomer;
class QyAttributeCustomerHouseholdDemographics;
class QyAttributeHouseholdDemographicsHouseholdDemographics;
class QyAttributeHouseholdDemographicsCustomer;

class QyBatch {
public:
    QyBatch(QyAttributeStoreSalesCustomer* qy_attribute_store_sales_customer,
            QyAttributeCustomerHouseholdDemographics* qy_attribute_customer_household_demographics,
            QyAttributeHouseholdDemographicsHouseholdDemographics* qy_attribute_household_demographics_household_demographics,
            QyAttributeHouseholdDemographicsCustomer* qy_attribute_household_demographics_customer);
    ~QyBatch();

    void clear();
    void set_store_sales_row(StoreSalesRow* row);
    void set_customer_row(CustomerRow* row);
    void set_household_demographics_row(HouseholdDemographicsRow* row);
    void set_household_demographics_row2(HouseholdDemographicsRow* row);
    void set_customer_row2(CustomerRow* row);
    void fill(Reservoir<QyRow>* reservoir);

private:
    // relation id of the base relation
    // store_sales -> 0, customer1 -> 1, household_demographics1 -> 2, household_demographics2 -> 3, customer2 -> 4
    int rid = 0;

    QyAttributeStoreSalesCustomer* qy_attribute_store_sales_customer = nullptr;
    QyAttributeCustomerHouseholdDemographics* qy_attribute_customer_household_demographics = nullptr;
    QyAttributeHouseholdDemographicsHouseholdDemographics* qy_attribute_household_demographics_household_demographics = nullptr;
    QyAttributeHouseholdDemographicsCustomer* qy_attribute_household_demographics_customer = nullptr;

    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;
    StoreSalesRow* store_sales_row = nullptr;
    CustomerRow* customer_row = nullptr;
    HouseholdDemographicsRow* household_demographics_row = nullptr;
    HouseholdDemographicsRow* household_demographics_row2 = nullptr;
    CustomerRow* customer_row2 = nullptr;
    QyRow* next_row = nullptr;

    unsigned long* layer_unit_sizes;
    unsigned long* layer_indices;

    std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>* store_sales_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<CustomerRow*>*>* customer_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<HouseholdDemographicsRow*>*>* household_demographics_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<HouseholdDemographicsRow*>*>* household_demographics_layer2 = nullptr;
    std::unordered_map<unsigned long, std::vector<CustomerRow*>*>* customer_layer2 = nullptr;

    void init_real_iterator();
    bool has_next_real();
    QyRow* next_real();
    void compute_next_real();

    void search_layer_unit_size(int i);
    void reset_layer_unit_size(int i);
    void next_layer_unit_size(int i);
    void reset_layer_index(int i);
    void update_row(int i);
    void update_layer(int i, bool from_left);
    unsigned long get_layer_size(int i);
    unsigned long get_lcnt(int i);
    unsigned long get_wlcnt(int i);
    unsigned long get_rcnt(int i);
    unsigned long get_wrcnt(int i);
    unsigned long get_left_layer_max_unit_size(int i);
    unsigned long get_right_layer_max_unit_size(int i);
    bool layer_nonempty(int i);

    void fill_with_skip(Reservoir<QyRow>* reservoir);
};


#endif
