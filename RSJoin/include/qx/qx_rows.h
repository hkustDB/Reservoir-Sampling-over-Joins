#ifndef RESERVOIR_QX_ROWS_H
#define RESERVOIR_QX_ROWS_H

#include <string>
#include <utility>
#include "../tpc_ds_rows.h"

struct QxRow {
    DateDimRow* date_dim_row;
    StoreSalesRow* store_sales_row;
    StoreReturnsRow* store_returns_row;
    CatalogSalesRow* catalog_sales_row;
    DateDimRow* date_dim_row2;

    QxRow(DateDimRow *date_dim_row, StoreSalesRow *store_sales_row, StoreReturnsRow *store_returns_row,
              CatalogSalesRow *catalog_sales_row, DateDimRow *date_dim_row2) : date_dim_row(date_dim_row),
                                                                           store_sales_row(store_sales_row),
                                                                           store_returns_row(store_returns_row),
                                                                           catalog_sales_row(catalog_sales_row),
                                                                           date_dim_row2(date_dim_row2) {}

    bool operator==(const QxRow &rhs) const {
        return date_dim_row->d_date_sk == rhs.date_dim_row->d_date_sk
        && store_sales_row->ss_item_sk_with_ticket_number == rhs.store_sales_row->ss_item_sk_with_ticket_number
        && catalog_sales_row->cs_item_sk == rhs.catalog_sales_row->cs_item_sk
        && catalog_sales_row->cs_order_number == rhs.catalog_sales_row->cs_order_number
        && date_dim_row2->d_date_sk == rhs.date_dim_row2->d_date_sk;
    }
};

struct QxRowHash
{
    std::size_t operator()(const QxRow& row) const
    {
        using std::size_t;
        using std::hash;

        return ((hash<int>()(row.date_dim_row->d_date_sk))
            ^ (hash<long>()(row.store_sales_row->ss_item_sk_with_ticket_number))
            ^ (hash<int>()(row.catalog_sales_row->cs_item_sk))
            ^ (hash<int>()(row.catalog_sales_row->cs_order_number))
            ^ (hash<int>()(row.date_dim_row2->d_date_sk)));
    }
};

struct QxFkLeftRow {
    DateDimRow* date_dim_row;
    StoreSalesRow* store_sales_row;
    StoreReturnsRow* store_returns_row;

    QxFkLeftRow(DateDimRow *date_dim_row, StoreSalesRow *store_sales_row, StoreReturnsRow *store_returns_row):
        date_dim_row(date_dim_row), store_sales_row(store_sales_row), store_returns_row(store_returns_row) {}
};

struct QxFkRightRow {
    CatalogSalesRow* catalog_sales_row;
    DateDimRow* date_dim_row2;

    QxFkRightRow(CatalogSalesRow *catalog_sales_row, DateDimRow *date_dim_row2):
        catalog_sales_row(catalog_sales_row), date_dim_row2(date_dim_row2) {}
};

#endif
