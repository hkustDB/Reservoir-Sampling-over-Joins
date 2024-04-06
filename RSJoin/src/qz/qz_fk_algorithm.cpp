#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/qz/qz_fk_algorithm.h"
#include "../../include/qz/qz_fk_attribute_item_middle.h"
#include "../../include/qz/qz_fk_attribute_middle_right.h"
#include "../../include/qz/qz_fk_left_batch.h"
#include "../../include/qz/qz_fk_middle_batch.h"
#include "../../include/qz/qz_fk_right_batch.h"
#include "../../include/qz/qz_rows.h"


QzFkAlgorithm::~QzFkAlgorithm() {
    delete reservoir;
    delete left_batch;
    delete middle_batch;
    delete right_batch;

    for (auto &iter : *category_id_to_item_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    category_id_to_item_row->clear();
    delete category_id_to_item_row;

    for (auto &iter : *category_id_to_middle_row) {
        auto vec = iter.second;
        for (auto row: *vec) {
            delete row->store_sales_row;
            delete row;
        }
        vec->clear();
        delete vec;
    }
    category_id_to_middle_row->clear();
    delete category_id_to_middle_row;

    for (auto &iter : *income_band_sk_to_middle_row) {
        auto vec = iter.second;
        vec->clear();
        delete vec;
    }
    income_band_sk_to_middle_row->clear();
    delete income_band_sk_to_middle_row;

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

    for (auto &iter : *item_sk_to_item_row2) {
        auto row = iter.second;
        delete row;
    }
    item_sk_to_item_row2->clear();
    delete item_sk_to_item_row2;

    delete qz_fk_attribute_item_middle;
    delete qz_fk_attribute_middle_right;
}

void QzFkAlgorithm::init(const std::string &file, int k) {
    this->input = file;

    this->reservoir = new Reservoir<QzRow>(k);

    this->category_id_to_item_row = new std::unordered_map<int, std::vector<ItemRow*>*>;
    this->category_id_to_middle_row = new std::unordered_map<int, std::vector<QzFkMiddleRow*>*>;
    this->income_band_sk_to_middle_row = new std::unordered_map<int, std::vector<QzFkMiddleRow*>*>;
    this->income_band_sk_to_right_row = new std::unordered_map<int, std::vector<QzFkRightRow*>*>;

    this->hdemo_sk_to_household_demographics_row = new std::unordered_map<int, HouseholdDemographicsRow*>;
    this->hdemo_sk_to_household_demographics_row2 = new std::unordered_map<int, HouseholdDemographicsRow*>;
    this->customer_sk_to_customer_row = new std::unordered_map<int, CustomerRow*>;
    this->item_sk_to_item_row2 = new std::unordered_map<int, ItemRow*>;

    this->qz_fk_attribute_item_middle = new QzFkAttributeItemMiddle;
    this->qz_fk_attribute_middle_right = new QzFkAttributeMiddleRight;

    qz_fk_attribute_item_middle->set_left_relation_index(category_id_to_item_row);
    qz_fk_attribute_item_middle->set_right_relation_index(category_id_to_middle_row);
    qz_fk_attribute_item_middle->set_right_attribute(qz_fk_attribute_middle_right);

    qz_fk_attribute_middle_right->set_left_relation_index(income_band_sk_to_middle_row);
    qz_fk_attribute_middle_right->set_right_relation_index(income_band_sk_to_right_row);
    qz_fk_attribute_middle_right->set_left_attribute(qz_fk_attribute_item_middle);

    this->left_batch = new QzFkLeftBatch(qz_fk_attribute_item_middle, qz_fk_attribute_middle_right);
    this->middle_batch = new QzFkMiddleBatch;
    this->right_batch = new QzFkRightBatch(qz_fk_attribute_item_middle, qz_fk_attribute_middle_right);
}

Reservoir<QzRow>* QzFkAlgorithm::run(bool print_info) {
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

    if (print_info) {
        unsigned long propagation_cnt = qz_fk_attribute_item_middle->get_propagation_cnt()
                                            + qz_fk_attribute_middle_right->get_propagation_cnt();
        printf("propagations: %lu\n", propagation_cnt);
    }

    return reservoir;
}

void QzFkAlgorithm::process(const std::string &line) {
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

            if (ss_customer_sk != 0) {
                auto iter1 = customer_sk_to_customer_row->find(ss_customer_sk);
                if (iter1 != customer_sk_to_customer_row->end()) {
                    auto iter2 = item_sk_to_item_row2->find(ss_item_sk);
                    if (iter2 != item_sk_to_item_row2->end()) {
                        int ss_cdemo_sk = next_int(line, &begin, lineSize, ',');
                        int ss_hdemo_sk = next_int(line, &begin, lineSize, ',');
                        int ss_addr_sk = next_int(line, &begin, lineSize, ',');
                        int ss_store_sk = next_int(line, &begin, lineSize, ',');
                        int ss_promo_sk = next_int(line, &begin, lineSize, ',');
                        int ss_ticket_number = next_int(line, &begin, lineSize, ',');
                        int ss_quantity = next_int(line, &begin, lineSize, ',');
                        long ss_item_sk_with_ticket_number = next_long(line, &begin, lineSize, ',');

                        auto customer_row = iter1->second;
                        auto household_demographics_row = hdemo_sk_to_household_demographics_row->at(
                                customer_row->c_current_hdemo_sk);
                        auto item_row2 = iter2->second;

                        auto store_sales_row = new StoreSalesRow(ss_sold_date_sk, ss_sold_time_sk,
                                                                 ss_item_sk, ss_customer_sk, ss_cdemo_sk,
                                                                 ss_hdemo_sk, ss_addr_sk, ss_store_sk,
                                                                 ss_promo_sk, ss_ticket_number, ss_quantity,
                                                                 ss_item_sk_with_ticket_number);

                        auto middle_row = new QzFkMiddleRow(item_row2, store_sales_row, customer_row,
                                                            household_demographics_row);

                        auto iter3 = category_id_to_middle_row->find(item_row2->i_category_id);
                        if (iter3 != category_id_to_middle_row->end()) {
                            iter3->second->emplace_back(middle_row);
                        } else {
                            category_id_to_middle_row->emplace(item_row2->i_category_id,
                                                               new std::vector<QzFkMiddleRow *>{middle_row});
                        }

                        auto iter4 = income_band_sk_to_middle_row->find(household_demographics_row->hd_income_band_sk);
                        if (iter4 != income_band_sk_to_middle_row->end()) {
                            iter4->second->emplace_back(middle_row);
                        } else {
                            income_band_sk_to_middle_row->emplace(household_demographics_row->hd_income_band_sk,
                                                                  new std::vector<QzFkMiddleRow *>{middle_row});
                        }

                        unsigned long wlcnt = qz_fk_attribute_item_middle->get_wlcnt(item_row2->i_category_id);
                        if (wlcnt > 0) {
                            qz_fk_attribute_middle_right->update_from_left(middle_row, 0, wlcnt);
                        }

                        unsigned long wrcnt = qz_fk_attribute_middle_right->get_wrcnt(
                                household_demographics_row->hd_income_band_sk);
                        if (wrcnt > 0) {
                            qz_fk_attribute_item_middle->update_from_right(middle_row, 0, wrcnt);
                        }

                        if (wlcnt > 0 && wrcnt > 0) {
                            middle_batch->init(qz_fk_attribute_item_middle->get_left_layer(item_row2->i_category_id),
                                               middle_row,
                                               qz_fk_attribute_middle_right->get_right_layer(
                                                       household_demographics_row->hd_income_band_sk));
                            middle_batch->fill(reservoir);
                        }
                    }
                }
            }
            break;
        }
        case 1: {
            int c_customer_sk = next_int(line, &begin, lineSize, ',');
            std::string c_customer_id = next_string(line, &begin);
            int c_current_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int c_current_hdemo_sk = next_int(line, &begin, lineSize, ',');

            if (c_current_hdemo_sk != 0) {
                auto iter1 = hdemo_sk_to_household_demographics_row2->find(c_current_hdemo_sk);
                if (iter1 != hdemo_sk_to_household_demographics_row2->end()) {
                    auto household_demographics_row2 = iter1->second;
                    auto customer_row2 = new CustomerRow(c_customer_sk, c_customer_id, c_current_cdemo_sk,
                                                         c_current_hdemo_sk);

                    auto right_row = new QzFkRightRow(household_demographics_row2, customer_row2);

                    auto iter2 = income_band_sk_to_right_row->find(household_demographics_row2->hd_income_band_sk);
                    if (iter2 != income_band_sk_to_right_row->end()) {
                        iter2->second->emplace_back(right_row);
                    } else {
                        income_band_sk_to_right_row->emplace(household_demographics_row2->hd_income_band_sk,
                                                             new std::vector<QzFkRightRow *>{right_row});
                    }
                    qz_fk_attribute_middle_right->update_from_right(right_row);

                    right_batch->set_right_row(right_row);
                    right_batch->fill(reservoir);
                }
            }
            break;
        }
        case 2: {
            int i_item_sk = next_int(line, &begin, lineSize, ',');
            std::string i_item_id = next_string(line, &begin);
            std::string i_rec_start_date = next_date(line, &begin, ',');
            std::string i_rec_end_date = next_date(line, &begin, ',');
            std::string i_item_desc = next_string(line, &begin);
            double i_current_price = next_double(line, &begin, ',');
            double i_wholesale_cost = next_double(line, &begin, ',');
            int i_brand_id = next_int(line, &begin, lineSize, ',');
            std::string i_brand = next_string(line, &begin);
            int i_class_id = next_int(line, &begin, lineSize, ',');
            std::string i_class = next_string(line, &begin);
            int i_category_id = next_int(line, &begin, lineSize, ',');
            std::string i_category = next_string(line, &begin);
            int i_manufact_id = next_int(line, &begin, lineSize, ',');
            std::string i_manufact = next_string(line, &begin);

            auto item_row = new ItemRow(i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc,
                                        i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id,
                                        i_class, i_category_id, i_category, i_manufact_id, i_manufact);

            auto iter = category_id_to_item_row->find(i_category_id);
            if (iter != category_id_to_item_row->end()) {
                iter->second->emplace_back(item_row);
            } else {
                category_id_to_item_row->emplace(i_category_id, new std::vector<ItemRow*>{item_row});
            }
            qz_fk_attribute_item_middle->update_from_left(item_row);

            left_batch->set_item_row(item_row);
            left_batch->fill(reservoir);
            break;
        }
        case 3: {
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
        case 4: {
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
        case 5: {
            int c_customer_sk = next_int(line, &begin, lineSize, ',');
            std::string c_customer_id = next_string(line, &begin);
            int c_current_cdemo_sk = next_int(line, &begin, lineSize, ',');
            int c_current_hdemo_sk = next_int(line, &begin, lineSize, ',');

            if (c_current_hdemo_sk != 0) {
                auto iter1 = hdemo_sk_to_household_demographics_row->find(c_current_hdemo_sk);
                if (iter1 != hdemo_sk_to_household_demographics_row->end()) {
                    auto customer_row = new CustomerRow(c_customer_sk, c_customer_id, c_current_cdemo_sk,
                                                        c_current_hdemo_sk);

                    customer_sk_to_customer_row->emplace(c_customer_sk, customer_row);
                }
            }
            break;
        }
        case 6: {
            int i_item_sk = next_int(line, &begin, lineSize, ',');
            std::string i_item_id = next_string(line, &begin);
            std::string i_rec_start_date = next_date(line, &begin, ',');
            std::string i_rec_end_date = next_date(line, &begin, ',');
            std::string i_item_desc = next_string(line, &begin);
            double i_current_price = next_double(line, &begin, ',');
            double i_wholesale_cost = next_double(line, &begin, ',');
            int i_brand_id = next_int(line, &begin, lineSize, ',');
            std::string i_brand = next_string(line, &begin);
            int i_class_id = next_int(line, &begin, lineSize, ',');
            std::string i_class = next_string(line, &begin);
            int i_category_id = next_int(line, &begin, lineSize, ',');
            std::string i_category = next_string(line, &begin);
            int i_manufact_id = next_int(line, &begin, lineSize, ',');
            std::string i_manufact = next_string(line, &begin);

            auto item_row2 = new ItemRow(i_item_sk, i_item_id, i_rec_start_date, i_rec_end_date, i_item_desc,
                                         i_current_price, i_wholesale_cost, i_brand_id, i_brand, i_class_id,
                                         i_class, i_category_id, i_category, i_manufact_id, i_manufact);

            item_sk_to_item_row2->emplace(i_item_sk, item_row2);
            break;
        }
    }
}