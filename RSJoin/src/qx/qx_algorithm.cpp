#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/qx/qx_algorithm.h"
#include "../../include/qx/qx_attribute_date_dim_store_sales.h"
#include "../../include/qx/qx_attribute_store_sales_store_returns.h"
#include "../../include/qx/qx_attribute_store_returns_catalog_sales.h"
#include "../../include/qx/qx_attribute_catalog_sales_date_dim.h"
#include "../../include/qx/qx_batch.h"
#include "../../include/qx/qx_rows.h"


QxAlgorithm::~QxAlgorithm() {
    delete reservoir;
    delete batch;

    for (auto &iter : *date_sk_to_date_dim_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
                delete row;
        }
        vec->clear();
        delete vec;
    }
    date_sk_to_date_dim_row->clear();
    delete date_sk_to_date_dim_row;

    for (auto &iter : *date_sk_to_store_sales_row) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    date_sk_to_store_sales_row->clear();
    delete date_sk_to_store_sales_row;

    for (auto &iter : *item_sk_with_ticket_number_to_store_sales_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    item_sk_with_ticket_number_to_store_sales_row->clear();
    delete item_sk_with_ticket_number_to_store_sales_row;

    for (auto &iter : *item_sk_with_ticket_number_to_store_returns_row) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    item_sk_with_ticket_number_to_store_returns_row->clear();
    delete item_sk_with_ticket_number_to_store_returns_row;

    for (auto &iter : *customer_sk_to_store_returns_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    customer_sk_to_store_returns_row->clear();
    delete customer_sk_to_store_returns_row;

    for (auto &iter : *customer_sk_to_catalog_sales_row) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    customer_sk_to_catalog_sales_row->clear();
    delete customer_sk_to_catalog_sales_row;

    for (auto &iter : *date_sk_to_catalog_sales_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    date_sk_to_catalog_sales_row->clear();
    delete date_sk_to_catalog_sales_row;

    for (auto &iter : *date_sk_to_date_dim_row2) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    date_sk_to_date_dim_row2->clear();
    delete date_sk_to_date_dim_row2;

    delete qx_attribute_date_dim_store_sales;
    delete qx_attribute_store_sales_store_returns;
    delete qx_attribute_store_returns_catalog_sales;
    delete qx_attribute_catalog_sales_date_dim;
}

void QxAlgorithm::init(const std::string &file, int k) {
    this->input = file;

    this->reservoir = new Reservoir<QxRow>(k);
    this->date_sk_to_date_dim_row = new std::unordered_map<int, std::vector<DateDimRow*>*>;
    this->date_sk_to_store_sales_row = new std::unordered_map<int, std::vector<StoreSalesRow*>*>;
    this->item_sk_with_ticket_number_to_store_sales_row = new std::unordered_map<long, std::vector<StoreSalesRow*>*>;
    this->item_sk_with_ticket_number_to_store_returns_row = new std::unordered_map<long, std::vector<StoreReturnsRow*>*>;
    this->customer_sk_to_store_returns_row = new std::unordered_map<int, std::vector<StoreReturnsRow*>*>;
    this->customer_sk_to_catalog_sales_row = new std::unordered_map<int, std::vector<CatalogSalesRow*>*>;
    this->date_sk_to_catalog_sales_row = new std::unordered_map<int, std::vector<CatalogSalesRow*>*>;
    this->date_sk_to_date_dim_row2 = new std::unordered_map<int, std::vector<DateDimRow*>*>;

    this->qx_attribute_date_dim_store_sales = new QxAttributeDateDimStoreSales;
    this->qx_attribute_store_sales_store_returns = new QxAttributeStoreSalesStoreReturns;
    this->qx_attribute_store_returns_catalog_sales = new QxAttributeStoreReturnsCatalogSales;
    this->qx_attribute_catalog_sales_date_dim = new QxAttributeCatalogSalesDateDim;

    qx_attribute_date_dim_store_sales->set_left_relation_index(date_sk_to_date_dim_row);
    qx_attribute_date_dim_store_sales->set_right_relation_index(date_sk_to_store_sales_row);
    qx_attribute_date_dim_store_sales->set_right_attribute(qx_attribute_store_sales_store_returns);

    qx_attribute_store_sales_store_returns->set_left_relation_index(item_sk_with_ticket_number_to_store_sales_row);
    qx_attribute_store_sales_store_returns->set_right_relation_index(item_sk_with_ticket_number_to_store_returns_row);
    qx_attribute_store_sales_store_returns->set_left_attribute(qx_attribute_date_dim_store_sales);
    qx_attribute_store_sales_store_returns->set_right_attribute(qx_attribute_store_returns_catalog_sales);

    qx_attribute_store_returns_catalog_sales->set_left_relation_index(customer_sk_to_store_returns_row);
    qx_attribute_store_returns_catalog_sales->set_right_relation_index(customer_sk_to_catalog_sales_row);
    qx_attribute_store_returns_catalog_sales->set_left_attribute(qx_attribute_store_sales_store_returns);
    qx_attribute_store_returns_catalog_sales->set_right_attribute(qx_attribute_catalog_sales_date_dim);

    qx_attribute_catalog_sales_date_dim->set_left_relation_index(date_sk_to_catalog_sales_row);
    qx_attribute_catalog_sales_date_dim->set_right_relation_index(date_sk_to_date_dim_row2);
    qx_attribute_catalog_sales_date_dim->set_left_attribute(qx_attribute_store_returns_catalog_sales);

    this->batch = new QxBatch(qx_attribute_date_dim_store_sales, qx_attribute_store_sales_store_returns,
                              qx_attribute_store_returns_catalog_sales, qx_attribute_catalog_sales_date_dim);
}

Reservoir<QxRow>* QxAlgorithm::run(bool print_info) {
    std::ifstream in(input);
    std::string line;

    auto start_time = std::chrono::system_clock::now();
    while (getline(in, line), !line.empty()) {
        if (line.at(0) == '-') {
            if (print_info) {
                auto end_time = std::chrono::system_clock::now();
                print_execution_info(start_time, end_time);
            }
        } else {
            process(line);
        }
    }

    return reservoir;
}

void QxAlgorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    if (line.at(0) == '-')
        return;

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    switch (sid) {
        case 0: {
            int sr_returned_date_sk = next_int(line, &begin, lineSize, ',');
            int sr_return_time_sk = next_int(line, &begin, lineSize, ',');
            int sr_item_sk = next_int(line, &begin, lineSize, ',');
            int sr_customer_sk = next_int(line, &begin, lineSize, ',');
            int sr_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int sr_hdemo_sk = next_int(line, &begin, lineSize, ',');
            int sr_addr_sk = next_int(line, &begin, lineSize, ',');
            int sr_store_sk = next_int(line, &begin, lineSize, ',');
            int sr_reason_sk = next_int(line, &begin, lineSize, ',');
            int sr_ticket_number = next_int(line, &begin, lineSize, ',');
            long sr_item_sk_with_ticket_number = next_long(line, &begin, lineSize, ',');

            auto row = new StoreReturnsRow(sr_returned_date_sk, sr_return_time_sk,
                                           sr_item_sk, sr_customer_sk, sr_cdemo_sk,
                                           sr_hdemo_sk, sr_addr_sk, sr_store_sk,
                                           sr_reason_sk, sr_ticket_number,
                                           sr_item_sk_with_ticket_number);
            auto iter1 = item_sk_with_ticket_number_to_store_returns_row->find(sr_item_sk_with_ticket_number);
            if (iter1 != item_sk_with_ticket_number_to_store_returns_row->end()) {
                iter1->second->emplace_back(row);
            } else {
                item_sk_with_ticket_number_to_store_returns_row->emplace(sr_item_sk_with_ticket_number, new std::vector<StoreReturnsRow *>{row});
            }

            auto iter2 = customer_sk_to_store_returns_row->find(sr_customer_sk);
            if (iter2 != customer_sk_to_store_returns_row->end()) {
                iter2->second->emplace_back(row);
            } else {
                customer_sk_to_store_returns_row->emplace(sr_customer_sk, new std::vector<StoreReturnsRow *>{row});
            }

            unsigned long wlcnt = qx_attribute_store_sales_store_returns->get_wlcnt(sr_item_sk_with_ticket_number);
            if (wlcnt > 0) {
                qx_attribute_store_returns_catalog_sales->update_from_left(row, 0, wlcnt);
            }

            unsigned long wrcnt = qx_attribute_store_returns_catalog_sales->get_wrcnt(sr_customer_sk);
            if (wrcnt > 0) {
                qx_attribute_store_sales_store_returns->update_from_right(row, 0, wrcnt);
            }

            batch->set_store_returns_row(row);
            batch->fill(reservoir);
            break;
        }
        case 1: {
            int cs_sold_date_sk = next_int(line, &begin, lineSize, ',');
            int cs_sold_time_sk = next_int(line, &begin, lineSize, ',');
            int cs_ship_date_sk = next_int(line, &begin, lineSize, ',');
            int cs_bill_customer_sk = next_int(line, &begin, lineSize, ',');
            int cs_bill_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int cs_bill_hdemo_sk = next_int(line, &begin, lineSize, ',');
            int cs_bill_addr_sk = next_int(line, &begin, lineSize, ',');
            int cs_ship_customer_sk = next_int(line, &begin, lineSize, ',');
            int cs_ship_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int cs_ship_hdemo_sk = next_int(line, &begin, lineSize, ',');
            int cs_ship_addr_sk = next_int(line, &begin, lineSize, ',');
            int cs_call_center_sk = next_int(line, &begin, lineSize, ',');
            int cs_catalog_page_sk = next_int(line, &begin, lineSize, ',');
            int cs_ship_mode_sk = next_int(line, &begin, lineSize, ',');
            int cs_warehouse_sk = next_int(line, &begin, lineSize, ',');
            int cs_item_sk = next_int(line, &begin, lineSize, ',');
            int cs_promo_sk = next_int(line, &begin, lineSize, ',');
            int cs_order_number = next_int(line, &begin, lineSize, ',');

            auto row = new CatalogSalesRow(cs_sold_date_sk, cs_sold_time_sk,
                                           cs_ship_date_sk,cs_bill_customer_sk,
                                           cs_bill_cdemo_sk, cs_bill_hdemo_sk,
                                           cs_bill_addr_sk,cs_ship_customer_sk,
                                           cs_ship_cdemo_sk, cs_ship_hdemo_sk,
                                           cs_ship_addr_sk,cs_call_center_sk,
                                           cs_catalog_page_sk, cs_ship_mode_sk,
                                           cs_warehouse_sk, cs_item_sk, cs_promo_sk,
                                           cs_order_number);
            auto iter1 = customer_sk_to_catalog_sales_row->find(cs_bill_customer_sk);
            if (iter1 != customer_sk_to_catalog_sales_row->end()) {
                iter1->second->emplace_back(row);
            } else {
                customer_sk_to_catalog_sales_row->emplace(cs_bill_customer_sk, new std::vector<CatalogSalesRow *>{row});
            }

            auto iter2 = date_sk_to_catalog_sales_row->find(cs_sold_date_sk);
            if (iter2 != date_sk_to_catalog_sales_row->end()) {
                iter2->second->emplace_back(row);
            } else {
                date_sk_to_catalog_sales_row->emplace(cs_sold_date_sk, new std::vector<CatalogSalesRow *>{row});
            }

            unsigned long wlcnt = qx_attribute_store_returns_catalog_sales->get_wlcnt(cs_bill_customer_sk);
            if (wlcnt > 0) {
                qx_attribute_catalog_sales_date_dim->update_from_left(row, 0, wlcnt);
            }

            unsigned long wrcnt = qx_attribute_catalog_sales_date_dim->get_wrcnt(cs_sold_date_sk);
            if (wrcnt > 0) {
                qx_attribute_store_returns_catalog_sales->update_from_right(row, 0, wrcnt);
            }

            batch->set_catalog_sales_row(row);
            batch->fill(reservoir);
            break;
        }
        case 2: {
            int d_date_sk = next_int(line, &begin, lineSize, ',');
            std::string d_date_id = next_string(line, &begin);
            std::string d_date = next_date(line, &begin, ',');
            int d_month_seq = next_int(line, &begin, lineSize, ',');
            int d_week_seq = next_int(line, &begin, lineSize, ',');
            int d_quarter_seq = next_int(line, &begin, lineSize, ',');
            int d_year = next_int(line, &begin, lineSize, ',');
            int d_dow = next_int(line, &begin, lineSize, ',');
            int d_moy = next_int(line, &begin, lineSize, ',');
            int d_dom = next_int(line, &begin, lineSize, ',');
            int d_qoy = next_int(line, &begin, lineSize, ',');

            auto row = new DateDimRow(d_date_sk, d_date_id, d_date, d_month_seq,
                                      d_week_seq, d_quarter_seq, d_year, d_dow,
                                      d_moy, d_dom, d_qoy);
            auto iter = date_sk_to_date_dim_row->find(d_date_sk);
            if (iter != date_sk_to_date_dim_row->end()) {
                iter->second->emplace_back(row);
            } else {
                date_sk_to_date_dim_row->emplace(d_date_sk, new std::vector<DateDimRow *>{row});
            }
            qx_attribute_date_dim_store_sales->update_from_left(row, 0, 1);

            batch->set_date_dim_row(row);
            batch->fill(reservoir);
            break;
        }
        case 3: {
            int d_date_sk = next_int(line, &begin, lineSize, ',');
            std::string d_date_id = next_string(line, &begin);
            std::string d_date = next_date(line, &begin, ',');
            int d_month_seq = next_int(line, &begin, lineSize, ',');
            int d_week_seq = next_int(line, &begin, lineSize, ',');
            int d_quarter_seq = next_int(line, &begin, lineSize, ',');
            int d_year = next_int(line, &begin, lineSize, ',');
            int d_dow = next_int(line, &begin, lineSize, ',');
            int d_moy = next_int(line, &begin, lineSize, ',');
            int d_dom = next_int(line, &begin, lineSize, ',');
            int d_qoy = next_int(line, &begin, lineSize, ',');

            auto row = new DateDimRow(d_date_sk, d_date_id, d_date, d_month_seq,
                                      d_week_seq, d_quarter_seq, d_year, d_dow,
                                      d_moy, d_dom, d_qoy);
            auto iter = date_sk_to_date_dim_row2->find(d_date_sk);
            if (iter != date_sk_to_date_dim_row2->end()) {
                iter->second->emplace_back(row);
            } else {
                date_sk_to_date_dim_row2->emplace(d_date_sk, new std::vector<DateDimRow *>{row});
            }
            qx_attribute_catalog_sales_date_dim->update_from_right(row, 0, 1);

            batch->set_date_dim_row2(row);
            batch->fill(reservoir);
            break;
        }
        case 4: {
            int ss_sold_date_sk = next_int(line, &begin, lineSize, ',');
            int ss_sold_time_sk = next_int(line, &begin, lineSize, ',');
            int ss_item_sk = next_int(line, &begin, lineSize, ',');
            int ss_customer_sk = next_int(line, &begin, lineSize, ',');
            int ss_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int ss_hdemo_sk = next_int(line, &begin, lineSize, ',');
            int ss_addr_sk = next_int(line, &begin, lineSize, ',');
            int ss_store_sk = next_int(line, &begin, lineSize, ',');
            int ss_promo_sk = next_int(line, &begin, lineSize, ',');
            int ss_ticket_number = next_int(line, &begin, lineSize, ',');
            int ss_quantity = next_int(line, &begin, lineSize, ',');
            long ss_item_sk_with_ticket_number = next_long(line, &begin, lineSize, ',');

            auto row = new StoreSalesRow(ss_sold_date_sk, ss_sold_time_sk,
                                           ss_item_sk, ss_customer_sk, ss_cdemo_sk,
                                           ss_hdemo_sk, ss_addr_sk, ss_store_sk,
                                           ss_promo_sk, ss_ticket_number, ss_quantity,
                                           ss_item_sk_with_ticket_number);

            auto iter1 = date_sk_to_store_sales_row->find(ss_sold_date_sk);
            if (iter1 != date_sk_to_store_sales_row->end()) {
                iter1->second->emplace_back(row);
            } else {
                date_sk_to_store_sales_row->emplace(ss_sold_date_sk, new std::vector<StoreSalesRow *>{row});
            }

            auto iter2 = item_sk_with_ticket_number_to_store_sales_row->find(ss_item_sk_with_ticket_number);
            if (iter2 != item_sk_with_ticket_number_to_store_sales_row->end()) {
                iter2->second->emplace_back(row);
            } else {
                item_sk_with_ticket_number_to_store_sales_row->emplace(ss_item_sk_with_ticket_number, new std::vector<StoreSalesRow *>{row});
            }

            unsigned long wlcnt = qx_attribute_date_dim_store_sales->get_wlcnt(ss_sold_date_sk);
            if (wlcnt > 0) {
                qx_attribute_store_sales_store_returns->update_from_left(row, 0, wlcnt);
            }

            unsigned long wrcnt = qx_attribute_store_sales_store_returns->get_wrcnt(ss_item_sk_with_ticket_number);
            if (wrcnt > 0) {
                qx_attribute_date_dim_store_sales->update_from_right(row, 0, wrcnt);
            }

            batch->set_store_sales_row(row);
            batch->fill(reservoir);
            break;
        }
    }
}