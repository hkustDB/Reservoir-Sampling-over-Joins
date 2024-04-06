#ifndef RESERVOIR_QX_ALGORITHM_H
#define RESERVOIR_QX_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>

template<typename T>
class Reservoir;
class QxBatch;
struct QxRow;
struct DateDimRow;
struct StoreSalesRow;
struct StoreReturnsRow;
struct CatalogSalesRow;
class QxAttributeDateDimStoreSales;
class QxAttributeStoreSalesStoreReturns;
class QxAttributeStoreReturnsCatalogSales;
class QxAttributeCatalogSalesDateDim;

class QxAlgorithm {
public:
    ~QxAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<QxRow>* run(bool print_info);

private:
    std::string input;
    QxBatch* batch = nullptr;
    Reservoir<QxRow>* reservoir = nullptr;

    std::unordered_map<int, std::vector<DateDimRow*>*>* date_sk_to_date_dim_row = nullptr;
    std::unordered_map<int, std::vector<StoreSalesRow*>*>* date_sk_to_store_sales_row = nullptr;
    std::unordered_map<long, std::vector<StoreSalesRow*>*>* item_sk_with_ticket_number_to_store_sales_row = nullptr;
    std::unordered_map<long, std::vector<StoreReturnsRow*>*>* item_sk_with_ticket_number_to_store_returns_row = nullptr;
    std::unordered_map<int, std::vector<StoreReturnsRow*>*>* customer_sk_to_store_returns_row = nullptr;
    std::unordered_map<int, std::vector<CatalogSalesRow*>*>* customer_sk_to_catalog_sales_row = nullptr;
    std::unordered_map<int, std::vector<CatalogSalesRow*>*>* date_sk_to_catalog_sales_row = nullptr;
    std::unordered_map<int, std::vector<DateDimRow*>*>* date_sk_to_date_dim_row2 = nullptr;

    QxAttributeDateDimStoreSales* qx_attribute_date_dim_store_sales = nullptr;
    QxAttributeStoreSalesStoreReturns* qx_attribute_store_sales_store_returns = nullptr;
    QxAttributeStoreReturnsCatalogSales* qx_attribute_store_returns_catalog_sales = nullptr;
    QxAttributeCatalogSalesDateDim* qx_attribute_catalog_sales_date_dim = nullptr;

    void process(const std::string& line);
};

#endif
