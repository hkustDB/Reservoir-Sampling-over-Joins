#ifndef RESERVOIR_QZ_FK_ALGORITHM_H
#define RESERVOIR_QZ_FK_ALGORITHM_H

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
struct QzFkRightRow;
class QzFkLeftBatch;
class QzFkMiddleBatch;
class QzFkRightBatch;
class QzFkAttributeItemMiddle;
class QzFkAttributeMiddleRight;

class QzFkAlgorithm {
public:
    ~QzFkAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QzRow>* run(bool print_info);

private:
    std::string input;
    QzFkLeftBatch* left_batch = nullptr;
    QzFkMiddleBatch* middle_batch = nullptr;
    QzFkRightBatch* right_batch = nullptr;
    Reservoir<QzRow>* reservoir = nullptr;

    std::unordered_map<int, std::vector<ItemRow*>*>* category_id_to_item_row = nullptr;
    std::unordered_map<int, std::vector<QzFkMiddleRow*>*>* category_id_to_middle_row = nullptr;
    std::unordered_map<int, std::vector<QzFkMiddleRow*>*>* income_band_sk_to_middle_row = nullptr;
    std::unordered_map<int, std::vector<QzFkRightRow*>*>* income_band_sk_to_right_row = nullptr;

    std::unordered_map<int, HouseholdDemographicsRow*>* hdemo_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, HouseholdDemographicsRow*>* hdemo_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, CustomerRow*>* customer_sk_to_customer_row = nullptr;
    std::unordered_map<int, ItemRow*>* item_sk_to_item_row2 = nullptr;

    QzFkAttributeItemMiddle* qz_fk_attribute_item_middle;
    QzFkAttributeMiddleRight* qz_fk_attribute_middle_right;

    void process(const std::string& line);
};


#endif
