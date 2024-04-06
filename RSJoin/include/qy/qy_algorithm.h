#ifndef RESERVOIR_QY_ALGORITHM_H
#define RESERVOIR_QY_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

template<typename T>
class Reservoir;
class QyBatch;
struct QyRow;
struct StoreSalesRow;
struct CustomerRow;
struct HouseholdDemographicsRow;
class QyAttributeStoreSalesCustomer;
class QyAttributeCustomerHouseholdDemographics;
class QyAttributeHouseholdDemographicsHouseholdDemographics;
class QyAttributeHouseholdDemographicsCustomer;

class QyAlgorithm {
public:
    ~QyAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QyRow>* run(bool print_info);

private:
    std::string input;
    QyBatch* batch = nullptr;
    Reservoir<QyRow>* reservoir = nullptr;

    std::unordered_map<int, std::vector<StoreSalesRow*>*>* customer_sk_to_store_sales_row = nullptr;
    std::unordered_map<int, std::vector<CustomerRow*>*>* customer_sk_to_customer_row = nullptr;
    std::unordered_map<int, std::vector<CustomerRow*>*>* hdemo_sk_to_customer_row = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* hdemo_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* income_band_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* income_band_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>* hdemo_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, std::vector<CustomerRow*>*>* hdemo_sk_to_customer_row2 = nullptr;

    QyAttributeStoreSalesCustomer* qy_attribute_store_sales_customer = nullptr;
    QyAttributeCustomerHouseholdDemographics* qy_attribute_customer_household_demographics = nullptr;
    QyAttributeHouseholdDemographicsHouseholdDemographics* qy_attribute_household_demographics_household_demographics = nullptr;
    QyAttributeHouseholdDemographicsCustomer* qy_attribute_household_demographics_customer = nullptr;

    void process(const std::string& line);
};


#endif
