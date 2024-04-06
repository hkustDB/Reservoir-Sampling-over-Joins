#ifndef RESERVOIR_TPC_DS_ROWS_H
#define RESERVOIR_TPC_DS_ROWS_H

#include <string>
#include <utility>
struct GraphRow;

struct DateDimRow {
    int d_date_sk;
    std::string d_date_id;
    std::string d_date;
    int d_month_seq;
    int d_week_seq;
    int d_quarter_seq;
    int d_year;
    int d_dow;
    int d_moy;
    int d_dom;
    int d_qoy;

    DateDimRow(int d_date_sk, std::string d_date_id, std::string d_date, int d_month_seq, int d_week_seq,
               int d_quarter_seq, int d_year, int d_dow, int d_moy, int d_dom, int d_qoy) : d_date_sk(d_date_sk),
                                                                                     d_date_id(std::move(d_date_id)), d_date(std::move(d_date)),
                                                                                     d_month_seq(d_month_seq),
                                                                                     d_week_seq(d_week_seq),
                                                                                     d_quarter_seq(d_quarter_seq),
                                                                                     d_year(d_year), d_dow(d_dow),
                                                                                     d_moy(d_moy), d_dom(d_dom),
                                                                                     d_qoy(d_qoy) {}

    bool operator==(const DateDimRow &rhs) const {
        return d_date_sk == rhs.d_date_sk;
    }
};

struct DateDimRowHash {
    std::size_t operator()(const DateDimRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<int>()(row.d_date_sk));
    }
};

struct StoreSalesRow {
    int ss_sold_date_sk;
    int ss_sold_time_sk;
    int ss_item_sk;
    int ss_customer_sk;
    int ss_cdemo_sk;
    int ss_hdemo_sk;
    int ss_addr_sk;
    int ss_store_sk;
    int ss_promo_sk;
    int ss_ticket_number;
    int ss_quantity;
    long ss_item_sk_with_ticket_number;

    StoreSalesRow(int ss_sold_date_sk, int ss_sold_time_sk, int ss_item_sk, int ss_customer_sk, int ss_cdemo_sk, int ss_hdemo_sk,
                  int ss_addr_sk, int ss_store_sk, int ss_promo_sk, int ss_ticket_number, int ss_quantity,
                  long ss_item_sk_with_ticket_number) : ss_sold_date_sk(ss_sold_date_sk), ss_sold_time_sk(ss_sold_time_sk),
                                                   ss_item_sk(ss_item_sk), ss_customer_sk(ss_customer_sk),
                                                   ss_cdemo_sk(ss_cdemo_sk), ss_hdemo_sk(ss_hdemo_sk), ss_addr_sk(ss_addr_sk),
                                                   ss_store_sk(ss_store_sk), ss_promo_sk(ss_promo_sk),
                                                   ss_ticket_number(ss_ticket_number), ss_quantity(ss_quantity),
                                                   ss_item_sk_with_ticket_number(ss_item_sk_with_ticket_number) {}

    bool operator==(const StoreSalesRow &rhs) const {
        return ss_item_sk_with_ticket_number == rhs.ss_item_sk_with_ticket_number;
    }
};

struct StoreSalesRowHash {
    std::size_t operator()(const StoreSalesRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<long>()(row.ss_item_sk_with_ticket_number));
    }
};

struct StoreReturnsRow {
    int sr_returned_date_sk;
    int sr_return_time_sk;
    int sr_item_sk;
    int sr_customer_sk;
    int sr_cdemo_sk;
    int sr_hdemo_sk;
    int sr_addr_sk;
    int sr_store_sk;
    int sr_reason_sk;
    int sr_ticket_number;
    long sr_item_sk_with_ticket_number;

    StoreReturnsRow(int sr_returned_date_sk, int sr_return_time_sk, int sr_item_sk, int sr_customer_sk, int sr_cdemo_sk,
                    int sr_hdemo_sk, int sr_addr_sk, int sr_store_sk, int sr_reason_sk, int sr_ticket_number,
                    long sr_item_sk_with_ticket_number) : sr_returned_date_sk(sr_returned_date_sk),
                                                                           sr_return_time_sk(sr_return_time_sk),
                                                                           sr_item_sk(sr_item_sk),
                                                                           sr_customer_sk(sr_customer_sk),
                                                                           sr_cdemo_sk(sr_cdemo_sk),
                                                                           sr_hdemo_sk(sr_hdemo_sk), sr_addr_sk(sr_addr_sk),
                                                                           sr_store_sk(sr_store_sk),
                                                                           sr_reason_sk(sr_reason_sk),
                                                                           sr_ticket_number(sr_ticket_number),
                                                                           sr_item_sk_with_ticket_number(sr_item_sk_with_ticket_number) {}

    bool operator==(const StoreReturnsRow &rhs) const {
        return sr_item_sk_with_ticket_number == rhs.sr_item_sk_with_ticket_number;
    }
};

struct StoreReturnsRowHash {
    std::size_t operator()(const StoreReturnsRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<long>()(row.sr_item_sk_with_ticket_number));
    }
};

struct CatalogSalesRow {
    int cs_sold_date_sk;
    int cs_sold_time_sk;
    int cs_ship_date_sk;
    int cs_bill_customer_sk;
    int cs_bill_cdemo_sk;
    int cs_bill_hdemo_sk;
    int cs_bill_addr_sk;
    int cs_ship_customer_sk;
    int cs_ship_cdemo_sk;
    int cs_ship_hdemo_sk;
    int cs_ship_addr_sk;
    int cs_call_center_sk;
    int cs_catalog_page_sk;
    int cs_ship_mode_sk;
    int cs_warehouse_sk;
    int cs_item_sk;
    int cs_promo_sk;
    int cs_order_number;

    CatalogSalesRow(int cs_sold_date_sk, int cs_sold_time_sk, int cs_ship_date_sk, int cs_bill_customer_sk, int cs_bill_cdemo_sk,
                    int cs_bill_hdemo_sk, int cs_bill_addr_sk, int cs_ship_customer_sk, int cs_ship_cdemo_sk, int cs_ship_hdemo_sk,
                    int cs_ship_addr_sk, int cs_call_center_sk, int cs_catalog_page_sk, int cs_ship_mode_sk, int cs_warehouse_sk,
                    int cs_item_sk, int cs_promo_sk, int cs_order_number) : cs_sold_date_sk(cs_sold_date_sk),
                                                                      cs_sold_time_sk(cs_sold_time_sk),
                                                                      cs_ship_date_sk(cs_ship_date_sk),
                                                                      cs_bill_customer_sk(cs_bill_customer_sk),
                                                                      cs_bill_cdemo_sk(cs_bill_cdemo_sk),
                                                                      cs_bill_hdemo_sk(cs_bill_hdemo_sk),
                                                                      cs_bill_addr_sk(cs_bill_addr_sk),
                                                                      cs_ship_customer_sk(cs_ship_customer_sk),
                                                                      cs_ship_cdemo_sk(cs_ship_cdemo_sk),
                                                                      cs_ship_hdemo_sk(cs_ship_hdemo_sk),
                                                                      cs_ship_addr_sk(cs_ship_addr_sk),
                                                                      cs_call_center_sk(cs_call_center_sk),
                                                                      cs_catalog_page_sk(cs_catalog_page_sk),
                                                                      cs_ship_mode_sk(cs_ship_mode_sk),
                                                                      cs_warehouse_sk(cs_warehouse_sk),
                                                                      cs_item_sk(cs_item_sk), cs_promo_sk(cs_promo_sk),
                                                                      cs_order_number(cs_order_number) {}

    bool operator==(const CatalogSalesRow &rhs) const {
        return cs_item_sk == rhs.cs_item_sk &&
               cs_order_number == rhs.cs_order_number;
    }
};

struct CatalogSalesRowHash {
    std::size_t operator()(const CatalogSalesRow& row) const
    {
        using std::size_t;
        using std::hash;

        return ((hash<int>()(row.cs_item_sk)) ^ (hash<int>()(row.cs_order_number)));
    }
};

struct CustomerRow {
    int c_customer_sk;
    std::string c_customer_id;
    int c_current_cdemo_sk;
    int c_current_hdemo_sk;

    CustomerRow(int c_customer_sk, std::string c_customer_id, int c_current_cdemo_sk, int c_current_hdemo_sk):
            c_customer_sk(c_customer_sk),
            c_customer_id(std::move(c_customer_id)),
            c_current_cdemo_sk(c_current_cdemo_sk),
            c_current_hdemo_sk(c_current_hdemo_sk) {}

    bool operator==(const CustomerRow &rhs) const {
        return c_customer_sk == rhs.c_customer_sk;
    }
};

struct CustomerRowHash {
    std::size_t operator()(const CustomerRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<int>()(row.c_customer_sk));
    }
};

struct HouseholdDemographicsRow {
    int hd_demo_sk;
    int hd_income_band_sk;
    std::string hd_buy_potential;
    int hd_dep_count;
    int hd_vehicle_count;

    HouseholdDemographicsRow(int hd_demo_sk, int hd_income_band_sk, std::string hd_buy_potential, int hd_dep_count,
                             int hd_vehicle_count) : hd_demo_sk(hd_demo_sk), hd_income_band_sk(hd_income_band_sk),
                                                hd_buy_potential(std::move(hd_buy_potential)), hd_dep_count(hd_dep_count),
                                                hd_vehicle_count(hd_vehicle_count) {}

    bool operator==(const HouseholdDemographicsRow &rhs) const {
        return hd_demo_sk == rhs.hd_demo_sk;
    }
};

struct HouseholdDemographicsRowHash {
    std::size_t operator()(const HouseholdDemographicsRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<int>()(row.hd_demo_sk));
    }
};

struct ItemRow {
    int i_item_sk;
    std::string i_item_id;
    std::string i_rec_start_date;
    std::string i_rec_end_date;
    std::string i_item_desc;
    double i_current_price;
    double i_wholesale_cost;
    int i_brand_id;
    std::string i_brand;
    int i_class_id;
    std::string i_class;
    int i_category_id;
    std::string i_category;
    int i_manufact_id;
    std::string i_manufact;

    ItemRow(int i_item_sk, std::string i_item_id, std::string i_rec_start_date, std::string i_rec_end_date,
            std::string i_item_desc, double i_current_price, double i_wholesale_cost, int i_brand_id,
            std::string i_brand, int i_class_id, std::string i_class, int i_category_id,
            std::string i_category, int i_manufact_id, std::string i_manufact) : i_item_sk(i_item_sk),
                                                                                           i_item_id(std::move(i_item_id)),
                                                                                           i_rec_start_date(std::move(i_rec_start_date)),
                                                                                           i_rec_end_date(std::move(i_rec_end_date)),
                                                                                           i_item_desc(std::move(i_item_desc)),
                                                                                           i_current_price(i_current_price),
                                                                                           i_wholesale_cost(i_wholesale_cost),
                                                                                           i_brand_id(i_brand_id),
                                                                                           i_brand(std::move(i_brand)),
                                                                                           i_class_id(i_class_id),
                                                                                           i_class(std::move(i_class)),
                                                                                           i_category_id(i_category_id),
                                                                                           i_category(std::move(i_category)),
                                                                                           i_manufact_id(i_manufact_id),
                                                                                           i_manufact(std::move(i_manufact)) {}

    bool operator==(const ItemRow &rhs) const {
        return i_item_sk == rhs.i_item_sk;
    }
};

struct ItemRowHash {
    std::size_t operator()(const ItemRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<int>()(row.i_item_sk));
    }
};

#endif
