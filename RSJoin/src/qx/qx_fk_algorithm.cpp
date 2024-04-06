#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/qx/qx_fk_algorithm.h"
#include "../../include/qx/qx_rows.h"
#include "../../include/qx/qx_fk_left_batch.h"
#include "../../include/qx/qx_fk_right_batch.h"


QxFkAlgorithm::~QxFkAlgorithm() {
    delete reservoir;
    delete left_batch;
    delete right_batch;

    for (auto &iter : *date_sk_to_date_dim_row) {
        auto row = iter.second;
        delete row;
    }
    date_sk_to_date_dim_row->clear();
    delete date_sk_to_date_dim_row;

    for (auto &iter : *item_sk_with_ticket_number_to_store_sales_row) {
        auto row = iter.second;
        delete row;
    }
    item_sk_with_ticket_number_to_store_sales_row->clear();
    delete item_sk_with_ticket_number_to_store_sales_row;

    for (auto &iter : *item_sk_with_ticket_number_to_store_returns_row) {
        auto row = iter.second;
        delete row;
    }
    item_sk_with_ticket_number_to_store_returns_row->clear();
    delete item_sk_with_ticket_number_to_store_returns_row;

    for (auto &iter : *date_sk_to_date_dim_row2) {
        auto row = iter.second;
        delete row;
    }
    date_sk_to_date_dim_row2->clear();
    delete date_sk_to_date_dim_row2;

    for (auto &iter : *customer_sk_to_left_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    customer_sk_to_left_row->clear();
    delete customer_sk_to_left_row;

    for (auto &iter : *customer_sk_to_right_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row->catalog_sales_row;
            delete row;
        }
        vec->clear();
        delete vec;
    }
    customer_sk_to_right_row->clear();
    delete customer_sk_to_right_row;
}

void QxFkAlgorithm::init(const std::string &file, int k) {
    this->input = file;

    this->reservoir = new Reservoir<QxRow>(k);
    this->date_sk_to_date_dim_row = new std::unordered_map<int, DateDimRow*>;
    this->item_sk_with_ticket_number_to_store_sales_row = new std::unordered_map<long, StoreSalesRow*>;
    this->item_sk_with_ticket_number_to_store_returns_row = new std::unordered_map<long, StoreReturnsRow*>;
    this->date_sk_to_date_dim_row2 = new std::unordered_map<int, DateDimRow*>;

    this->customer_sk_to_left_row = new std::unordered_map<int, std::vector<QxFkLeftRow*>*>;
    this->customer_sk_to_right_row = new std::unordered_map<int, std::vector<QxFkRightRow*>*>;

    this->left_batch = new QxFkLeftBatch;
    this->right_batch = new QxFkRightBatch;
}

Reservoir<QxRow>* QxFkAlgorithm::run(bool print_info) {
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

void QxFkAlgorithm::process(const std::string &line) {
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

            auto store_returns_row = new StoreReturnsRow(sr_returned_date_sk, sr_return_time_sk,
                                           sr_item_sk, sr_customer_sk, sr_cdemo_sk,
                                           sr_hdemo_sk, sr_addr_sk, sr_store_sk,
                                           sr_reason_sk, sr_ticket_number,
                                           sr_item_sk_with_ticket_number);

            auto iter1 = item_sk_with_ticket_number_to_store_sales_row->find(sr_item_sk_with_ticket_number);
            if (iter1 != item_sk_with_ticket_number_to_store_sales_row->end()) {
                auto store_sales_row = iter1->second;
                auto date_dim_row = date_sk_to_date_dim_row->at(store_sales_row->ss_sold_date_sk);

                auto left_row = new QxFkLeftRow(date_dim_row, store_sales_row, store_returns_row);
                auto iter2 = customer_sk_to_left_row->find(sr_customer_sk);
                if (iter2 != customer_sk_to_left_row->end()) {
                    iter2->second->emplace_back(left_row);
                } else {
                    customer_sk_to_left_row->emplace(sr_customer_sk, new std::vector<QxFkLeftRow*>{left_row});
                }

                auto iter3 = customer_sk_to_right_row->find(sr_customer_sk);
                if (iter3 != customer_sk_to_right_row->end()) {
                    left_batch->init(left_row, iter3->second);
                    left_batch->fill(reservoir);
                }
            } else {
                item_sk_with_ticket_number_to_store_returns_row->emplace(sr_item_sk_with_ticket_number, store_returns_row);
            }
            break;
        }
        case 1: {
            int cs_sold_date_sk = next_int(line, &begin, lineSize, ',');

            auto iter1 = date_sk_to_date_dim_row2->find(cs_sold_date_sk);
            if (iter1 == date_sk_to_date_dim_row2->end()) {
                break;
            } else {
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

                auto date_dim_row2 = iter1->second;
                auto catalog_sales_row = new CatalogSalesRow(cs_sold_date_sk, cs_sold_time_sk,
                                                 cs_ship_date_sk,cs_bill_customer_sk,
                                                 cs_bill_cdemo_sk, cs_bill_hdemo_sk,
                                                 cs_bill_addr_sk,cs_ship_customer_sk,
                                                 cs_ship_cdemo_sk, cs_ship_hdemo_sk,
                                                 cs_ship_addr_sk,cs_call_center_sk,
                                                 cs_catalog_page_sk, cs_ship_mode_sk,
                                                 cs_warehouse_sk, cs_item_sk, cs_promo_sk,
                                                 cs_order_number);

                auto right_row = new QxFkRightRow(catalog_sales_row, date_dim_row2);
                auto iter2 = customer_sk_to_right_row->find(cs_bill_customer_sk);
                if (iter2 != customer_sk_to_right_row->end()) {
                    iter2->second->emplace_back(right_row);
                } else {
                    customer_sk_to_right_row->emplace(cs_bill_customer_sk, new std::vector<QxFkRightRow*>{right_row});
                }

                auto iter3 = customer_sk_to_left_row->find(cs_bill_customer_sk);
                if (iter3 != customer_sk_to_left_row->end()) {
                    right_batch->init(iter3->second, right_row);
                    right_batch->fill(reservoir);
                }
            }
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
            date_sk_to_date_dim_row->emplace(d_date_sk, row);
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
            date_sk_to_date_dim_row2->emplace(d_date_sk, row);
            break;
        }
        case 4: {
            int ss_sold_date_sk = next_int(line, &begin, lineSize, ',');

            auto iter1 = date_sk_to_date_dim_row->find(ss_sold_date_sk);
            if (iter1 == date_sk_to_date_dim_row->end()) {
                break;
            } else {
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

                auto store_sales_row = new StoreSalesRow(ss_sold_date_sk, ss_sold_time_sk,
                                             ss_item_sk, ss_customer_sk, ss_cdemo_sk,
                                             ss_hdemo_sk, ss_addr_sk, ss_store_sk,
                                             ss_promo_sk, ss_ticket_number, ss_quantity,
                                             ss_item_sk_with_ticket_number);

                auto iter2 = item_sk_with_ticket_number_to_store_returns_row->find(ss_item_sk_with_ticket_number);
                if (iter2 != item_sk_with_ticket_number_to_store_returns_row->end()) {
                    auto date_dim_row = iter1->second;
                    auto store_returns_row = iter2->second;

                    auto left_row = new QxFkLeftRow(date_dim_row, store_sales_row, store_returns_row);
                    auto iter3 = customer_sk_to_left_row->find(store_returns_row->sr_customer_sk);
                    if (iter3 != customer_sk_to_left_row->end()) {
                        iter3->second->emplace_back(left_row);
                    } else {
                        customer_sk_to_left_row->emplace(store_returns_row->sr_customer_sk, new std::vector<QxFkLeftRow*>{left_row});
                    }

                    auto iter4 = customer_sk_to_right_row->find(store_returns_row->sr_customer_sk);
                    if (iter4 != customer_sk_to_right_row->end()) {
                        left_batch->init(left_row, iter4->second);
                        left_batch->fill(reservoir);
                    }
                } else {
                    item_sk_with_ticket_number_to_store_sales_row->emplace(ss_item_sk_with_ticket_number, store_sales_row);
                }
            }
        }
    }
}