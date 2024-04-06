#ifndef RESERVOIR_QY_FK_ALGORITHM_H
#define RESERVOIR_QY_FK_ALGORITHM_H

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
struct QyFkLeftRow;
struct QyFkRightRow;
class QyFkLeftBatch;
class QyFkRightBatch;

class QyFkAlgorithm {
public:
    ~QyFkAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QyRow>* run(bool print_info);

private:
    std::string input;
    QyFkLeftBatch* left_batch = nullptr;
    QyFkRightBatch* right_batch = nullptr;
    Reservoir<QyRow>* reservoir = nullptr;

    std::unordered_map<int, HouseholdDemographicsRow*>* hdemo_sk_to_household_demographics_row = nullptr;
    std::unordered_map<int, HouseholdDemographicsRow*>* hdemo_sk_to_household_demographics_row2 = nullptr;
    std::unordered_map<int, CustomerRow*>* customer_sk_to_customer_row = nullptr;

    std::unordered_map<int, std::vector<QyFkLeftRow*>*>* income_band_sk_to_left_row;
    std::unordered_map<int, std::vector<QyFkRightRow*>*>* income_band_sk_to_right_row;

    void process(const std::string& line);
};

#endif
