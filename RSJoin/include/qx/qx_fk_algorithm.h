#ifndef RESERVOIR_QX_FK_ALGORITHM_H
#define RESERVOIR_QX_FK_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QxRow;
struct DateDimRow;
struct StoreSalesRow;
struct StoreReturnsRow;
struct CatalogSalesRow;
struct QxFkLeftRow;
struct QxFkRightRow;
class QxFkLeftBatch;
class QxFkRightBatch;

class QxFkAlgorithm {
public:
    ~QxFkAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QxRow>* run(bool print_info);

private:
    std::string input;
    QxFkLeftBatch* left_batch = nullptr;
    QxFkRightBatch* right_batch = nullptr;
    Reservoir<QxRow>* reservoir = nullptr;

    std::unordered_map<int, DateDimRow*>* date_sk_to_date_dim_row = nullptr;
    std::unordered_map<long, StoreSalesRow*>* item_sk_with_ticket_number_to_store_sales_row = nullptr;
    std::unordered_map<long, StoreReturnsRow*>* item_sk_with_ticket_number_to_store_returns_row = nullptr;
    std::unordered_map<int, DateDimRow*>* date_sk_to_date_dim_row2 = nullptr;

    std::unordered_map<int, std::vector<QxFkLeftRow*>*>* customer_sk_to_left_row;
    std::unordered_map<int, std::vector<QxFkRightRow*>*>* customer_sk_to_right_row;

    void process(const std::string& line);
};


#endif
