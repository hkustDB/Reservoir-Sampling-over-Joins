#ifndef RESERVOIR_QZ_FK_GROUP_ALGORITHM_H
#define RESERVOIR_QZ_FK_GROUP_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QzRow;
struct StoreSalesRow;
struct CustomerRow;
struct HouseholdDemographicsRow;
struct ItemRow;
struct QzFkMiddleRow;
struct QzFkGroupMiddleRow;
struct QzFkRightRow;
class QzFkGroupLeftBatch;
class QzFkMiddleBatch;
class QzFkGroupRightBatch;
class QzFkGroupAttributeItemMiddle;
class QzFkGroupAttributeMiddleRight;

class QzFkGroupAlgorithm {
public:
    ~QzFkGroupAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QzRow>* run(bool print_info);

private:
    std::string input;
    QzFkGroupLeftBatch* left_batch = nullptr;
    QzFkMiddleBatch* middle_batch = nullptr;
    QzFkGroupRightBatch* right_batch = nullptr;
    Reservoir<QzRow>* reservoir = nullptr;

    std::unordered_map<int, std::vector<ItemRow*>*>* category_id_to_item_row = nullptr;
    std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* category_id_to_group_middle_row = nullptr;
    std::unordered_map<int, std::vector<QzFkGroupMiddleRow*>*>* income_band_sk_to_group_middle_row = nullptr;
    std::unordered_map<int, std::vector<QzFkRightRow*>*>* income_band_sk_to_right_row = nullptr;

    std::unordered_map<int, HouseholdDemographicsRow*>* hdemo_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, HouseholdDemographicsRow*>* hdemo_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, CustomerRow*>* customer_sk_to_customer_row = nullptr;
    std::unordered_map<int, ItemRow*>* item_sk_to_item_row2 = nullptr;

    // use (category_id << 32) + income_band_sk as key here
    std::unordered_map<unsigned long, QzFkGroupMiddleRow*>* category_id_with_income_band_sk_to_group_middle_row = nullptr;

    QzFkGroupAttributeItemMiddle* qz_fk_group_attribute_item_middle;
    QzFkGroupAttributeMiddleRight* qz_fk_group_attribute_middle_right;

    void process(const std::string& line);
};


#endif
