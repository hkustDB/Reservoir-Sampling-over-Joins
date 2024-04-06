#ifndef RESERVOIR_QZ_ALGORITHM_H
#define RESERVOIR_QZ_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

template<typename T>
class Reservoir;
class QzBatch;
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

class QzAlgorithm {
public:
    ~QzAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QzRow>* run(bool print_info);

private:
    std::string input;
    QzBatch* batch = nullptr;
    Reservoir<QzRow>* reservoir = nullptr;

    std::unordered_map<int, std::vector<ItemRow*>*>* category_id_to_item_row = nullptr;
    std::unordered_map<int, std::vector<ItemRow*>*>* category_id_to_item_row2 = nullptr;
    std::unordered_map<int, std::vector<ItemRow*>*>* item_sk_to_item_row2 = nullptr;
    std::unordered_map<int, std::vector<StoreSalesRow*>*>* item_sk_to_store_sales_row = nullptr;
    std::unordered_map<int, std::vector<StoreSalesRow*>*>* customer_sk_to_store_sales_row = nullptr;
    std::unordered_map<int, std::vector<CustomerRow*>*>* customer_sk_to_customer_row = nullptr;
    std::unordered_map<int, std::vector<CustomerRow*>*>* hdemo_sk_to_customer_row = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* hdemo_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* income_band_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* income_band_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* hdemo_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, std::vector<CustomerRow*>*>* hdemo_sk_to_customer_row2 = nullptr;

    QzAttributeItemItem* qz_attribute_item_item = nullptr;
    QzAttributeItemStoreSales* qz_attribute_item_store_sales = nullptr;
    QzAttributeStoreSalesCustomer* qz_attribute_store_sales_customer = nullptr;
    QzAttributeCustomerHouseholdDemographics* qz_attribute_customer_household_demographics = nullptr;
    QzAttributeHouseholdDemographicsHouseholdDemographics* qz_attribute_household_demographics_household_demographics = nullptr;
    QzAttributeHouseholdDemographicsCustomer* qz_attribute_household_demographics_customer = nullptr;

    void process(const std::string& line);
};


#endif
