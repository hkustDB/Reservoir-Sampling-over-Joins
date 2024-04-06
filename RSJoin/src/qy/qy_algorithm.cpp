#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/qy/qy_algorithm.h"
#include "../../include/qy/qy_attribute_store_sales_customer.h"
#include "../../include/qy/qy_attribute_customer_household_demographics.h"
#include "../../include/qy/qy_attribute_household_demographics_household_demographics.h"
#include "../../include/qy/qy_attribute_household_demographics_customer.h"
#include "../../include/qy/qy_batch.h"
#include "../../include/qy/qy_rows.h"


QyAlgorithm::~QyAlgorithm() {
    delete reservoir;
    delete batch;

    for (auto &iter : *customer_sk_to_store_sales_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    customer_sk_to_store_sales_row->clear();
    delete customer_sk_to_store_sales_row;

    for (auto &iter : *customer_sk_to_customer_row) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    customer_sk_to_customer_row->clear();
    delete customer_sk_to_customer_row;

    for (auto &iter : *hdemo_sk_to_customer_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    hdemo_sk_to_customer_row->clear();
    delete hdemo_sk_to_customer_row;

    for (auto &iter : *hdemo_sk_to_household_demographics_row) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    hdemo_sk_to_household_demographics_row->clear();
    delete hdemo_sk_to_household_demographics_row;

    for (auto &iter : *income_band_sk_to_household_demographics_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    income_band_sk_to_household_demographics_row->clear();
    delete income_band_sk_to_household_demographics_row;

    for (auto &iter : *income_band_sk_to_household_demographics_row2) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    income_band_sk_to_household_demographics_row2->clear();
    delete income_band_sk_to_household_demographics_row2;

    for (auto &iter : *hdemo_sk_to_household_demographics_row2) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    hdemo_sk_to_household_demographics_row2->clear();
    delete hdemo_sk_to_household_demographics_row2;

    for (auto &iter : *hdemo_sk_to_customer_row2) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    hdemo_sk_to_customer_row2->clear();
    delete hdemo_sk_to_customer_row2;

    delete qy_attribute_store_sales_customer;
    delete qy_attribute_customer_household_demographics;
    delete qy_attribute_household_demographics_household_demographics;
    delete qy_attribute_household_demographics_customer;
}

void QyAlgorithm::init(const std::string &file, int k) {
    this->input = file;

    this->reservoir = new Reservoir<QyRow>(k);
    this->customer_sk_to_store_sales_row = new std::unordered_map<int, std::vector<StoreSalesRow*>*>;
    this->customer_sk_to_customer_row = new std::unordered_map<int, std::vector<CustomerRow*>*>;
    this->hdemo_sk_to_customer_row = new std::unordered_map<int, std::vector<CustomerRow*>*>;
    this->hdemo_sk_to_household_demographics_row = new std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>;
    this->income_band_sk_to_household_demographics_row = new std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>;
    this->income_band_sk_to_household_demographics_row2 = new std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>;
    this->hdemo_sk_to_household_demographics_row2 = new std::unordered_map<int, std::vector<HouseholdDemographicsRow*>*>;
    this->hdemo_sk_to_customer_row2 = new std::unordered_map<int, std::vector<CustomerRow*>*>;

    this->qy_attribute_store_sales_customer = new QyAttributeStoreSalesCustomer;
    this->qy_attribute_customer_household_demographics = new QyAttributeCustomerHouseholdDemographics;
    this->qy_attribute_household_demographics_household_demographics = new QyAttributeHouseholdDemographicsHouseholdDemographics;
    this->qy_attribute_household_demographics_customer = new QyAttributeHouseholdDemographicsCustomer;

    qy_attribute_store_sales_customer->set_left_relation_index(customer_sk_to_store_sales_row);
    qy_attribute_store_sales_customer->set_right_relation_index(customer_sk_to_customer_row);
    qy_attribute_store_sales_customer->set_right_attribute(qy_attribute_customer_household_demographics);

    qy_attribute_customer_household_demographics->set_left_relation_index(hdemo_sk_to_customer_row);
    qy_attribute_customer_household_demographics->set_right_relation_index(hdemo_sk_to_household_demographics_row);
    qy_attribute_customer_household_demographics->set_left_attribute(qy_attribute_store_sales_customer);
    qy_attribute_customer_household_demographics->set_right_attribute(qy_attribute_household_demographics_household_demographics);

    qy_attribute_household_demographics_household_demographics->set_left_relation_index(income_band_sk_to_household_demographics_row);
    qy_attribute_household_demographics_household_demographics->set_right_relation_index(income_band_sk_to_household_demographics_row2);
    qy_attribute_household_demographics_household_demographics->set_left_attribute(qy_attribute_customer_household_demographics);
    qy_attribute_household_demographics_household_demographics->set_right_attribute(qy_attribute_household_demographics_customer);

    qy_attribute_household_demographics_customer->set_left_relation_index(hdemo_sk_to_household_demographics_row2);
    qy_attribute_household_demographics_customer->set_right_relation_index(hdemo_sk_to_customer_row2);
    qy_attribute_household_demographics_customer->set_left_attribute(qy_attribute_household_demographics_household_demographics);

    this->batch = new QyBatch(qy_attribute_store_sales_customer, qy_attribute_customer_household_demographics,
                              qy_attribute_household_demographics_household_demographics, qy_attribute_household_demographics_customer);
}

Reservoir<QyRow>* QyAlgorithm::run(bool print_info) {
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

void QyAlgorithm::process(const std::string &line) {
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

            auto iter = customer_sk_to_store_sales_row->find(ss_customer_sk);
            if (iter != customer_sk_to_store_sales_row->end()) {
                iter->second->emplace_back(store_sales_row);
            } else {
                customer_sk_to_store_sales_row->emplace(ss_customer_sk, new std::vector<StoreSalesRow*>{store_sales_row});
            }
            qy_attribute_store_sales_customer->update_from_left(store_sales_row, 0, 1);

            batch->set_store_sales_row(store_sales_row);
            batch->fill(reservoir);
            break;
        }
        case 1: {
            int c_customer_sk = next_int(line, &begin, lineSize, ',');
            std::string c_customer_id = next_string(line, &begin);
            int c_current_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int c_current_hdemo_sk = next_int(line, &begin, lineSize, ',');

            auto customer_row2 = new CustomerRow(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk);

            auto iter = hdemo_sk_to_customer_row2->find(c_current_hdemo_sk);
            if (iter != hdemo_sk_to_customer_row2->end()) {
                iter->second->emplace_back(customer_row2);
            } else {
                hdemo_sk_to_customer_row2->emplace(c_current_hdemo_sk, new std::vector<CustomerRow*>{customer_row2});
            }
            qy_attribute_household_demographics_customer->update_from_right(customer_row2, 0, 1);

            batch->set_customer_row2(customer_row2);
            batch->fill(reservoir);
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

            auto iter1 = hdemo_sk_to_household_demographics_row->find(hd_demo_sk);
            if (iter1 != hdemo_sk_to_household_demographics_row->end()) {
                iter1->second->emplace_back(household_demographics_row);
            } else {
                hdemo_sk_to_household_demographics_row->emplace(hd_demo_sk, new std::vector<HouseholdDemographicsRow*>{household_demographics_row});
            }

            auto iter2 = income_band_sk_to_household_demographics_row->find(hd_income_band_sk);
            if (iter2 != income_band_sk_to_household_demographics_row->end()) {
                iter2->second->emplace_back(household_demographics_row);
            } else {
                income_band_sk_to_household_demographics_row->emplace(hd_income_band_sk, new std::vector<HouseholdDemographicsRow*>{household_demographics_row});
            }

            unsigned long wlcnt = qy_attribute_customer_household_demographics->get_wlcnt(hd_demo_sk);
            if (wlcnt > 0) {
                qy_attribute_household_demographics_household_demographics->update_from_left(household_demographics_row, 0, wlcnt);
            }

            unsigned long wrcnt = qy_attribute_household_demographics_household_demographics->get_wrcnt(hd_income_band_sk);
            if (wrcnt > 0) {
                qy_attribute_customer_household_demographics->update_from_right(household_demographics_row, 0, wrcnt);
            }

            batch->set_household_demographics_row(household_demographics_row);
            batch->fill(reservoir);
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

            auto iter1 = income_band_sk_to_household_demographics_row2->find(hd_income_band_sk);
            if (iter1 != income_band_sk_to_household_demographics_row2->end()) {
                iter1->second->emplace_back(household_demographics_row2);
            } else {
                income_band_sk_to_household_demographics_row2->emplace(hd_income_band_sk, new std::vector<HouseholdDemographicsRow*>{household_demographics_row2});
            }

            auto iter2 = hdemo_sk_to_household_demographics_row2->find(hd_demo_sk);
            if (iter2 != hdemo_sk_to_household_demographics_row2->end()) {
                iter2->second->emplace_back(household_demographics_row2);
            } else {
                hdemo_sk_to_household_demographics_row2->emplace(hd_demo_sk, new std::vector<HouseholdDemographicsRow*>{household_demographics_row2});
            }

            unsigned long wlcnt = qy_attribute_household_demographics_household_demographics->get_wlcnt(hd_income_band_sk);
            if (wlcnt > 0) {
                qy_attribute_household_demographics_customer->update_from_left(household_demographics_row2, 0, wlcnt);
            }

            unsigned long wrcnt = qy_attribute_household_demographics_customer->get_wrcnt(hd_demo_sk);
            if (wrcnt > 0) {
                qy_attribute_household_demographics_household_demographics->update_from_right(household_demographics_row2, 0, wrcnt);
            }

            batch->set_household_demographics_row2(household_demographics_row2);
            batch->fill(reservoir);
            break;
        }
        case 4: {
            int c_customer_sk = next_int(line, &begin, lineSize, ',');
            std::string c_customer_id = next_string(line, &begin);
            int c_current_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int c_current_hdemo_sk = next_int(line, &begin, lineSize, ',');

            auto customer_row = new CustomerRow(c_customer_sk, c_customer_id, c_current_cdemo_sk, c_current_hdemo_sk);

            auto iter1 = customer_sk_to_customer_row->find(c_customer_sk);
            if (iter1 != customer_sk_to_customer_row->end()) {
                iter1->second->emplace_back(customer_row);
            } else {
                customer_sk_to_customer_row->emplace(c_customer_sk, new std::vector<CustomerRow*>{customer_row});
            }

            auto iter2 = hdemo_sk_to_customer_row->find(c_current_hdemo_sk);
            if (iter2 != hdemo_sk_to_customer_row->end()) {
                iter2->second->emplace_back(customer_row);
            } else {
                hdemo_sk_to_customer_row->emplace(c_current_hdemo_sk, new std::vector<CustomerRow*>{customer_row});
            }

            unsigned long wlcnt = qy_attribute_store_sales_customer->get_wlcnt(c_customer_sk);
            if (wlcnt > 0) {
                qy_attribute_customer_household_demographics->update_from_left(customer_row, 0, wlcnt);
            }

            unsigned long wrcnt = qy_attribute_customer_household_demographics->get_wrcnt(c_current_hdemo_sk);
            if (wrcnt > 0) {
                qy_attribute_store_sales_customer->update_from_right(customer_row, 0, wrcnt);
            }

            batch->set_customer_row(customer_row);
            batch->fill(reservoir);
            break;
        }
    }
}
