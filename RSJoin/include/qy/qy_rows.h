#ifndef RESERVOIR_QY_ROWS_H
#define RESERVOIR_QY_ROWS_H

#include <string>
#include <utility>
#include "../tpc_ds_rows.h"

struct QyRow {
    StoreSalesRow* store_sales_row;
    CustomerRow* customer_row;
    HouseholdDemographicsRow* household_demographics_row;
    HouseholdDemographicsRow* household_demographics_row2;
    CustomerRow* customer_row2;

    QyRow(StoreSalesRow *store_sales_row, CustomerRow *customer_row, HouseholdDemographicsRow *household_demographics_row,
          HouseholdDemographicsRow *household_demographics_row2, CustomerRow *customer_row2) :
          store_sales_row(store_sales_row), customer_row(customer_row), household_demographics_row(household_demographics_row),
          household_demographics_row2(household_demographics_row2), customer_row2(customer_row2) {}

    bool operator==(const QyRow &rhs) const {
        return store_sales_row->ss_item_sk_with_ticket_number == rhs.store_sales_row->ss_item_sk_with_ticket_number
               && customer_row->c_customer_sk == rhs.customer_row->c_customer_sk
               && household_demographics_row->hd_demo_sk == rhs.household_demographics_row->hd_demo_sk
               && household_demographics_row2->hd_demo_sk == rhs.household_demographics_row2->hd_demo_sk
               && customer_row2->c_customer_sk == rhs.customer_row2->c_customer_sk;
    }
};

struct QyRowHash
{
    std::size_t operator()(const QyRow& row) const
    {
        using std::size_t;
        using std::hash;

        return ((hash<long>()(row.store_sales_row->ss_item_sk_with_ticket_number))
                ^ (hash<int>()(row.customer_row->c_customer_sk))
                ^ (hash<int>()(row.household_demographics_row->hd_demo_sk))
                ^ (hash<int>()(row.household_demographics_row2->hd_demo_sk))
                ^ (hash<int>()(row.customer_row2->c_customer_sk)));
    }
};

struct QyFkLeftRow {
    StoreSalesRow* store_sales_row;
    CustomerRow* customer_row;
    HouseholdDemographicsRow* household_demographics_row;

    QyFkLeftRow(StoreSalesRow* store_sales_row, CustomerRow* customer_row, HouseholdDemographicsRow* household_demographics_row):
            store_sales_row(store_sales_row), customer_row(customer_row), household_demographics_row(household_demographics_row) {}
};

struct QyFkRightRow {
    HouseholdDemographicsRow* household_demographics_row2;
    CustomerRow* customer_row2;

    QyFkRightRow(HouseholdDemographicsRow* household_demographics_row2, CustomerRow* customer_row2):
            household_demographics_row2(household_demographics_row2), customer_row2(customer_row2) {}
};

#endif
