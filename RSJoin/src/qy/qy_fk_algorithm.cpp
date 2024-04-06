#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/qy/qy_fk_algorithm.h"
#include "../../include/qy/qy_rows.h"
#include "../../include/qy/qy_fk_left_batch.h"
#include "../../include/qy/qy_fk_right_batch.h"


QyFkAlgorithm::~QyFkAlgorithm() {
    delete reservoir;
    delete left_batch;
    delete right_batch;

    for (auto &iter : *hdemo_sk_to_household_demographics_row) {
        auto row = iter.second;
        delete row;
    }
    hdemo_sk_to_household_demographics_row->clear();
    delete hdemo_sk_to_household_demographics_row;

    for (auto &iter : *hdemo_sk_to_household_demographics_row2) {
        auto row = iter.second;
        delete row;
    }
    hdemo_sk_to_household_demographics_row2->clear();
    delete hdemo_sk_to_household_demographics_row2;

    for (auto &iter : *customer_sk_to_customer_row) {
        auto row = iter.second;
        delete row;
    }
    customer_sk_to_customer_row->clear();
    delete customer_sk_to_customer_row;

    for (auto &iter : *income_band_sk_to_left_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row->store_sales_row;
            delete row;
        }
        vec->clear();
        delete vec;
    }
    income_band_sk_to_left_row->clear();
    delete income_band_sk_to_left_row;

    for (auto &iter : *income_band_sk_to_right_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row->customer_row2;
            delete row;
        }
        vec->clear();
        delete vec;
    }
    income_band_sk_to_right_row->clear();
    delete income_band_sk_to_right_row;
}

void QyFkAlgorithm::init(const std::string &file, int k) {
    this->input = file;

    this->reservoir = new Reservoir<QyRow>(k);
    this->hdemo_sk_to_household_demographics_row = new std::unordered_map<int, HouseholdDemographicsRow*>;
    this->hdemo_sk_to_household_demographics_row2 = new std::unordered_map<int, HouseholdDemographicsRow*>;
    this->customer_sk_to_customer_row = new std::unordered_map<int, CustomerRow*>;

    this->income_band_sk_to_left_row = new std::unordered_map<int, std::vector<QyFkLeftRow*>*>;
    this->income_band_sk_to_right_row = new std::unordered_map<int, std::vector<QyFkRightRow*>*>;

    this->left_batch = new QyFkLeftBatch;
    this->right_batch = new QyFkRightBatch;
}

Reservoir<QyRow>* QyFkAlgorithm::run(bool print_info) {
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

void QyFkAlgorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    if (line.at(0) == '-')
        return;

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    switch (sid) {
        case 0: {
            int ss_sold_date_sk = next_int(line, &begin, lineSize, ',');
            int ss_sold_time_sk = next_int(line, &begin, lineSize, ',');
            int ss_item_sk = next_int(line, &begin, lineSize, ',');
            int ss_customer_sk = next_int(line, &begin, lineSize, ',');

            auto iter1 = customer_sk_to_customer_row->find(ss_customer_sk);
            if (iter1 != customer_sk_to_customer_row->end()) {
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

                auto customer_row = iter1->second;
                auto household_demographics_row = hdemo_sk_to_household_demographics_row->at(customer_row->c_current_hdemo_sk);

                auto left_row = new QyFkLeftRow(store_sales_row, customer_row, household_demographics_row);

                auto iter2 = income_band_sk_to_left_row->find(household_demographics_row->hd_income_band_sk);
                if (iter2 != income_band_sk_to_left_row->end()) {
                    iter2->second->emplace_back(left_row);
                } else {
                    income_band_sk_to_left_row->emplace(household_demographics_row->hd_income_band_sk, new std::vector<QyFkLeftRow*>{left_row});
                }

                auto iter3 = income_band_sk_to_right_row->find(household_demographics_row->hd_income_band_sk);
                if (iter3 != income_band_sk_to_right_row->end()) {
                    auto right_rows = iter3->second;
                    left_batch->init(left_row, right_rows);
                    left_batch->fill(reservoir);
                }
            }
            break;
        }
        case 1: {
            int c_customer_sk = next_int(line, &begin, lineSize, ',');
            std::string c_customer_id = next_string(line, &begin);
            int c_current_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int c_current_hdemo_sk = next_int(line, &begin, lineSize, ',');

            auto iter1 = hdemo_sk_to_household_demographics_row2->find(c_current_hdemo_sk);
            if (iter1 != hdemo_sk_to_household_demographics_row2->end()) {
                auto customer_row2 = new CustomerRow(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk);
                auto household_demographics_row2 = iter1->second;

                auto right_row = new QyFkRightRow(household_demographics_row2, customer_row2);

                auto iter2 = income_band_sk_to_right_row->find(household_demographics_row2->hd_income_band_sk);
                if (iter2 != income_band_sk_to_right_row->end()) {
                    iter2->second->emplace_back(right_row);
                } else {
                    income_band_sk_to_right_row->emplace(household_demographics_row2->hd_income_band_sk, new std::vector<QyFkRightRow*>{right_row});
                }

                auto iter3 = income_band_sk_to_left_row->find(household_demographics_row2->hd_income_band_sk);
                if (iter3 != income_band_sk_to_left_row->end()) {
                    auto left_rows = iter3->second;
                    right_batch->init(left_rows, right_row);
                    right_batch->fill(reservoir);
                }
            }
            break;
        }
        case 2: {
            int hd_demo_sk = next_int(line, &begin, lineSize, ',');
            int hd_income_band_sk = next_int(line, &begin, lineSize, ',');
            std::string hd_buy_potential = next_string(line, &begin);
            int hd_dep_count = next_int(line, &begin, lineSize, ',');
            int hd_vehicle_count = next_int(line, &begin, lineSize, ',');

            auto household_demographics_row = new HouseholdDemographicsRow(hd_demo_sk, hd_income_band_sk,
                                                                           hd_buy_potential, hd_dep_count, hd_vehicle_count);
            hdemo_sk_to_household_demographics_row->emplace(hd_demo_sk, household_demographics_row);
            break;
        }
        case 3: {
            int hd_demo_sk = next_int(line, &begin, lineSize, ',');
            int hd_income_band_sk = next_int(line, &begin, lineSize, ',');
            std::string hd_buy_potential = next_string(line, &begin);
            int hd_dep_count = next_int(line, &begin, lineSize, ',');
            int hd_vehicle_count = next_int(line, &begin, lineSize, ',');

            auto household_demographics_row2 = new HouseholdDemographicsRow(hd_demo_sk, hd_income_band_sk,
                                                                           hd_buy_potential, hd_dep_count, hd_vehicle_count);
            hdemo_sk_to_household_demographics_row2->emplace(hd_demo_sk, household_demographics_row2);
            break;
        }
        case 4: {
            int c_customer_sk = next_int(line, &begin, lineSize, ',');
            std::string c_customer_id = next_string(line, &begin);
            int c_current_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int c_current_hdemo_sk = next_int(line, &begin, lineSize, ',');

            auto iter1 = hdemo_sk_to_household_demographics_row->find(c_current_hdemo_sk);
            if (iter1 != hdemo_sk_to_household_demographics_row->end()) {
                auto customer_row = new CustomerRow(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk);
                customer_sk_to_customer_row->emplace(c_customer_sk, customer_row);
            }
            break;
        }
    }
}
