#ifndef RESERVOIR_QX_BATCH_H
#define RESERVOIR_QX_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QxRow;
struct DateDimRow;
struct StoreSalesRow;
struct StoreReturnsRow;
struct CatalogSalesRow;
class QxAttributeDateDimStoreSales;
class QxAttributeStoreSalesStoreReturns;
class QxAttributeStoreReturnsCatalogSales;
class QxAttributeCatalogSalesDateDim;

class QxBatch {
public:
    QxBatch(QxAttributeDateDimStoreSales* qx_attribute_date_dim_store_sales,
             QxAttributeStoreSalesStoreReturns* qx_attribute_store_sales_store_returns,
             QxAttributeStoreReturnsCatalogSales* qx_attribute_store_returns_catalog_sales,
             QxAttributeCatalogSalesDateDim* qx_attribute_catalog_sales_date_dim);
    ~QxBatch();

    void clear();
    void set_date_dim_row(DateDimRow* row);
    void set_store_sales_row(StoreSalesRow* row);
    void set_store_returns_row(StoreReturnsRow* row);
    void set_catalog_sales_row(CatalogSalesRow* row);
    void set_date_dim_row2(DateDimRow* row);
    void fill(Reservoir<QxRow>* reservoir);

private:
    // relation id of the base relation
    // date_dim -> 0, store_sales -> 1, store_returns -> 2, catalog_sales -> 3, date_dim2 -> 4
    int rid = 0;

    QxAttributeDateDimStoreSales* qx_attribute_date_dim_store_sales = nullptr;
    QxAttributeStoreSalesStoreReturns* qx_attribute_store_sales_store_returns = nullptr;
    QxAttributeStoreReturnsCatalogSales* qx_attribute_store_returns_catalog_sales = nullptr;
    QxAttributeCatalogSalesDateDim* qx_attribute_catalog_sales_date_dim = nullptr;

    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;
    DateDimRow* date_dim_row = nullptr;
    StoreSalesRow* store_sales_row = nullptr;
    StoreReturnsRow* store_returns_row = nullptr;
    CatalogSalesRow* catalog_sales_row = nullptr;
    DateDimRow* date_dim_row2 = nullptr;
    QxRow* next_row = nullptr;

    unsigned long* layer_unit_sizes;
    unsigned long* layer_indices;

    std::unordered_map<unsigned long, std::vector<DateDimRow*>*>* date_dim_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<StoreSalesRow*>*>* store_sales_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<StoreReturnsRow*>*>* store_returns_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<CatalogSalesRow*>*>* catalog_sales_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<DateDimRow*>*>* date_dim_layer2 = nullptr;

    void init_real_iterator();
    bool has_next_real();
    QxRow* next_real();
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

    void fill_with_skip(Reservoir<QxRow>* reservoir);
};


#endif
