#ifndef RESERVOIR_QZ_BATCH_H
#define RESERVOIR_QZ_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QzRow;
struct StoreSalesRow;
struct CustomerRow;
struct HouseholdDemographicsRow;
struct ItemRow;
class QzAttributeItemItem;
class QzAttributeItemStoreSales;
class QzAttributeStoreSalesCustomer;
class QzAttributeCustomerHouseholdDemographics;
class QzAttributeHouseholdDemographicsHouseholdDemographics;
class QzAttributeHouseholdDemographicsCustomer;

class QzBatch {
public:
    QzBatch(QzAttributeItemItem *qzAttributeItemItem, QzAttributeItemStoreSales *qzAttributeItemStoreSales,
            QzAttributeStoreSalesCustomer *qzAttributeStoreSalesCustomer,
            QzAttributeCustomerHouseholdDemographics *qzAttributeCustomerHouseholdDemographics,
            QzAttributeHouseholdDemographicsHouseholdDemographics *qzAttributeHouseholdDemographicsHouseholdDemographics,
            QzAttributeHouseholdDemographicsCustomer *qzAttributeHouseholdDemographicsCustomer);

    ~QzBatch();

    void clear();
    void set_item_row(ItemRow* row);
    void set_item_row2(ItemRow* row);
    void set_store_sales_row(StoreSalesRow* row);
    void set_customer_row(CustomerRow* row);
    void set_household_demographics_row(HouseholdDemographicsRow* row);
    void set_household_demographics_row2(HouseholdDemographicsRow* row);
    void set_customer_row2(CustomerRow* row);
    void fill(Reservoir<QzRow>* reservoir);

private:
    // relation id of the base relation
    // item_row -> 0, item_row2 -> 1, store_sales_row -> 2, customer_row -> 3,
    // household_demographics1 -> 4, household_demographics2 -> 5, customer2 -> 6
    int rid = 0;

    QzAttributeItemItem* qz_attribute_item_item = nullptr;
    QzAttributeItemStoreSales* qz_attribute_item_store_sales = nullptr;
    QzAttributeStoreSalesCustomer* qz_attribute_store_sales_customer = nullptr;
    QzAttributeCustomerHouseholdDemographics* qz_attribute_customer_household_demographics = nullptr;
    QzAttributeHouseholdDemographicsHouseholdDemographics* qz_attribute_household_demographics_household_demographics = nullptr;
    QzAttributeHouseholdDemographicsCustomer* qz_attribute_household_demographics_customer = nullptr;

    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;
    ItemRow* item_row = nullptr;
    ItemRow* item_row2 = nullptr;
    StoreSalesRow* store_sales_row = nullptr;
    CustomerRow* customer_row = nullptr;
    HouseholdDemographicsRow* household_demographics_row = nullptr;
    HouseholdDemographicsRow* household_demographics_row2 = nullptr;
    CustomerRow* customer_row2 = nullptr;
    QzRow* next_row = nullptr;

    unsigned long* layer_unit_sizes;
    unsigned long* layer_indices;

    std::unordered_map<unsigned long, std::vector<ItemRow*>*>* item_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<ItemRow*>*>* item_layer2 = nullptr;
    std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>* store_sales_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<CustomerRow*>*>* customer_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<HouseholdDemographicsRow*>*>* household_demographics_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<HouseholdDemographicsRow*>*>* household_demographics_layer2 = nullptr;
    std::unordered_map<unsigned long, std::vector<CustomerRow*>*>* customer_layer2 = nullptr;

    void init_real_iterator();
    bool has_next_real();
    QzRow* next_real();
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

    void fill_with_skip(Reservoir<QzRow>* reservoir);
};


#endif
