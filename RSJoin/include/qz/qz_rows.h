#ifndef RESERVOIR_QZ_ROWS_H
#define RESERVOIR_QZ_ROWS_H

#include <string>
#include <utility>
#include <vector>
#include "../tpc_ds_rows.h"

struct QzRow {
    ItemRow* item_row;
    ItemRow* item_row2;
    StoreSalesRow* store_sales_row;
    CustomerRow* customer_row;
    HouseholdDemographicsRow* household_demographics_row;
    HouseholdDemographicsRow* household_demographics_row2;
    CustomerRow* customer_row2;

    QzRow(ItemRow* item_row, ItemRow* item_row2, StoreSalesRow *store_sales_row, CustomerRow *customer_row, HouseholdDemographicsRow *household_demographics_row,
          HouseholdDemographicsRow *household_demographics_row2, CustomerRow *customer_row2):
          item_row(item_row), item_row2(item_row2), store_sales_row(store_sales_row), customer_row(customer_row),
          household_demographics_row(household_demographics_row),household_demographics_row2(household_demographics_row2),
          customer_row2(customer_row2) {}
};

struct QzFkMiddleRow {
    ItemRow* item_row2;
    StoreSalesRow* store_sales_row;
    CustomerRow* customer_row;
    HouseholdDemographicsRow* household_demographics_row;

    QzFkMiddleRow(ItemRow* item_row2, StoreSalesRow* store_sales_row, CustomerRow* customer_row,
                  HouseholdDemographicsRow* household_demographics_row):
                  item_row2(item_row2), store_sales_row(store_sales_row), customer_row(customer_row),
                  household_demographics_row(household_demographics_row) {}

    bool operator==(const QzFkMiddleRow &rhs) const {
        return store_sales_row->ss_item_sk_with_ticket_number == rhs.store_sales_row->ss_item_sk_with_ticket_number;
    }
};

struct QzFkGroupMiddleRow {
    int i_category_id;
    int hd_income_band_sk;
    std::vector<QzFkMiddleRow*> group;
    unsigned long wcnt;

    QzFkGroupMiddleRow(int iCategoryId, int hdIncomeBandSk) : i_category_id(iCategoryId),
                                                                hd_income_band_sk(hdIncomeBandSk) {
        wcnt = 0;
    }

    bool operator==(const QzFkGroupMiddleRow &rhs) const {
        return i_category_id == rhs.i_category_id &&
               hd_income_band_sk == rhs.hd_income_band_sk;
    }
};

struct QzFkGroupMiddleRowHash {
    std::size_t operator()(const QzFkGroupMiddleRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<int>()(row.i_category_id)) ^ (hash<int>()(row.hd_income_band_sk));
    }
};

struct QzFkRightRow {
    HouseholdDemographicsRow* household_demographics_row2;
    CustomerRow* customer_row2;

    QzFkRightRow(HouseholdDemographicsRow* household_demographics_row2, CustomerRow* customer_row2):
        household_demographics_row2(household_demographics_row2), customer_row2(customer_row2) {}

    bool operator==(const QzFkRightRow &rhs) const {
        return customer_row2->c_customer_sk == rhs.customer_row2->c_customer_sk;
    }
};

#endif
