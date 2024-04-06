#include <fstream>
#include <iostream>
#include <vector>
#include "reservoir.h"
#include "utils.h"
#include "q10/q10_fk_plan2_algorithm.h"
#include "q10/q10_fk_plan2_attribute_left_middle.h"
#include "q10/q10_fk_plan2_attribute_middle_right.h"
#include "q10/q10_fk_plan2_left_batch.h"
#include "q10/q10_fk_plan2_middle_batch.h"
#include "q10/q10_fk_plan2_right_batch.h"
#include "q10/q10_rows.h"


Q10FkPlan2Algorithm::~Q10FkPlan2Algorithm() {
    delete reservoir;

    for (auto kv : *person_id_to_left_row) {
        auto left_rows = kv.second;
        for (auto left_row : *left_rows) {
            delete left_row;
        }
        left_rows->clear();
        delete left_rows;
    }
    person_id_to_left_row->clear();
    delete person_id_to_left_row;

    for (auto kv : *person_id_to_middle_row) {
        auto middle_rows = kv.second;
        for (auto middle_row : *middle_rows) {
            delete middle_row;
        }
        middle_rows->clear();
        delete middle_rows;
    }
    person_id_to_middle_row->clear();
    delete person_id_to_middle_row;

    for (auto kv : *message_id_to_middle_row) {
        auto middle_rows = kv.second;
        middle_rows->clear();
        delete middle_rows;
    }
    message_id_to_middle_row->clear();
    delete message_id_to_middle_row;

    for (auto kv : *message_id_to_right_row) {
        auto right_rows = kv.second;
        for (auto right_row : *right_rows) {
            delete right_row->message_has_tag_row2;
            delete right_row;
        }
        right_rows->clear();
        delete right_rows;
    }
    message_id_to_right_row->clear();
    delete message_id_to_right_row;

    for (auto kv : *tag_class_id_to_tag_class_row) {
        delete kv.second;
    }
    tag_class_id_to_tag_class_row->clear();
    delete tag_class_id_to_tag_class_row;

    for (auto kv : *tag_id_to_tag_row) {
        delete kv.second;
    }
    tag_id_to_tag_row->clear();
    delete tag_id_to_tag_row;

    for (auto kv : *tag_id_to_tag_row2) {
        delete kv.second;
    }
    tag_id_to_tag_row2->clear();
    delete tag_id_to_tag_row2;

    for (auto kv : *country_id_to_country_row) {
        delete kv.second;
    }
    country_id_to_country_row->clear();
    delete country_id_to_country_row;

    for (auto kv : *city_id_to_city_row) {
        delete kv.second;
    }
    city_id_to_city_row->clear();
    delete city_id_to_city_row;

    for (auto kv : *person_id_to_person_row) {
        delete kv.second;
    }
    person_id_to_person_row->clear();
    delete person_id_to_person_row;

    for (auto kv : *person_id_to_person_row2) {
        delete kv.second;
    }
    person_id_to_person_row2->clear();
    delete person_id_to_person_row2;

    for (auto kv : *message_id_to_message_row) {
        delete kv.second;
    }
    message_id_to_message_row->clear();
    delete message_id_to_message_row;

    for (auto kv : *person1_id_to_person_knows_person_row) {
        auto person_knows_person_rows = kv.second;
        for (auto person_knows_person_row : *person_knows_person_rows) {
            delete person_knows_person_row;
        }
        person_knows_person_rows->clear();
        delete person_knows_person_rows;
    }
    person1_id_to_person_knows_person_row->clear();
    delete person1_id_to_person_knows_person_row;

    for (auto kv : *person2_id_to_person_knows_person_row) {
        auto person_knows_person_rows = kv.second;
        person_knows_person_rows->clear();
        delete person_knows_person_rows;
    }
    person2_id_to_person_knows_person_row->clear();
    delete person2_id_to_person_knows_person_row;

    for (auto kv : *message_id_to_message_has_tag_row) {
        auto message_has_tag_rows = kv.second;
        for (auto message_has_tag_row : *message_has_tag_rows) {
            delete message_has_tag_row;
        }
        message_has_tag_rows->clear();
        delete message_has_tag_rows;
    }
    message_id_to_message_has_tag_row->clear();
    delete message_id_to_message_has_tag_row;

    delete q10_fk_plan2_attribute_left_middle;
    delete q10_fk_plan2_attribute_middle_right;

    delete left_batch;
    delete middle_batch;
    delete right_batch;
}

void Q10FkPlan2Algorithm::init(const std::string &file, int k) {
    this->input = file;

    this->reservoir = new Reservoir<Q10FkPlan2Row>(k);

    this->person_id_to_left_row = new std::unordered_map<unsigned long, std::vector<Q10FkPlan2LeftRow*>*>;
    this->person_id_to_middle_row = new std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>;
    this->message_id_to_middle_row = new std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>;
    this->message_id_to_right_row = new std::unordered_map<unsigned long, std::vector<Q10FkPlan2RightRow*>*>;

    this->tag_class_id_to_tag_class_row = new std::unordered_map<unsigned long, TagClassRow*>;
    this->tag_id_to_tag_row = new std::unordered_map<unsigned long, TagRow*>;
    this->tag_id_to_tag_row2 = new std::unordered_map<unsigned long, TagRow*>;
    this->country_id_to_country_row = new std::unordered_map<unsigned long, CountryRow*>;
    this->city_id_to_city_row = new std::unordered_map<unsigned long, CityRow*>;
    this->person_id_to_person_row = new std::unordered_map<unsigned long, PersonRow*>;
    this->person_id_to_person_row2 = new std::unordered_map<unsigned long, PersonRow*>;
    this->message_id_to_message_row = new std::unordered_map<unsigned long, MessageRow*>;
    this->person1_id_to_person_knows_person_row = new std::unordered_map<unsigned long, std::vector<PersonKnowsPersonRow*>*>;
    this->person2_id_to_person_knows_person_row = new std::unordered_map<unsigned long, std::vector<PersonKnowsPersonRow*>*>;
    this->message_id_to_message_has_tag_row = new std::unordered_map<unsigned long, std::vector<MessageHasTagRow*>*>;

    this->q10_fk_plan2_attribute_left_middle = new Q10FkPlan2AttributeLeftMiddle;
    this->q10_fk_plan2_attribute_middle_right = new Q10FkPlan2AttributeMiddleRight;

    q10_fk_plan2_attribute_left_middle->set_left_relation_index(person_id_to_left_row);
    q10_fk_plan2_attribute_left_middle->set_right_relation_index(person_id_to_middle_row);
    q10_fk_plan2_attribute_left_middle->set_right_attribute(q10_fk_plan2_attribute_middle_right);

    q10_fk_plan2_attribute_middle_right->set_left_relation_index(message_id_to_middle_row);
    q10_fk_plan2_attribute_middle_right->set_right_relation_index(message_id_to_right_row);
    q10_fk_plan2_attribute_middle_right->set_left_attribute(q10_fk_plan2_attribute_left_middle);

    this->left_batch = new Q10FkPlan2LeftBatch(q10_fk_plan2_attribute_left_middle, q10_fk_plan2_attribute_middle_right);
    this->middle_batch = new Q10FkPlan2MiddleBatch;
    this->right_batch = new Q10FkPlan2RightBatch(q10_fk_plan2_attribute_left_middle, q10_fk_plan2_attribute_middle_right);
}

Reservoir<Q10FkPlan2Row>* Q10FkPlan2Algorithm::run(bool print_info) {
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

void Q10FkPlan2Algorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    if (line.at(0) == '-')
        return;

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    switch (sid) {
        case 0: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long subclass_of_tag_class_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto tag_class_row = new TagClassRow(id, name, url, subclass_of_tag_class_id);
            tag_class_id_to_tag_class_row->emplace(id, tag_class_row);
            break;
        }
        case 1: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long type_tag_class_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto tag_row = new TagRow(id, name, url, type_tag_class_id);
            tag_id_to_tag_row->emplace(id, tag_row);
            break;
        }
        case 2: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long type_tag_class_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto iter = tag_class_id_to_tag_class_row->find(type_tag_class_id);
            if (iter != tag_class_id_to_tag_class_row->end()) {
                auto tag_row2 = new TagRow(id, name, url, type_tag_class_id);
                tag_id_to_tag_row2->emplace(id, tag_row2);
            }
            break;
        }
        case 3: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long part_of_place_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto country_row = new CountryRow(id, name, url, part_of_place_id);
            country_id_to_country_row->emplace(id, country_row);
            break;
        }
        case 4: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long part_of_place_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto iter = country_id_to_country_row->find(part_of_place_id);
            if (iter != country_id_to_country_row->end()) {
                auto city_row = new CityRow(id, name, url, part_of_place_id);
                city_id_to_city_row->emplace(id, city_row);
            }
            break;
        }
        case 5: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string location_ip = next_string(line, &begin);
            std::string browser_used = next_string(line, &begin);
            std::string content = next_string(line, &begin);
            unsigned long length = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long creator_person_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto message_row = new MessageRow(id, location_ip, browser_used, content, length, creator_person_id);
            message_id_to_message_row->emplace(id, message_row);

            auto iter1 = message_id_to_message_has_tag_row->find(creator_person_id);
            if (iter1 != message_id_to_message_has_tag_row->end()) {
                auto message_has_tag_rows = iter1->second;
                for (auto message_has_tag_row : *message_has_tag_rows) {
                    auto tag_row = tag_id_to_tag_row->at(message_has_tag_row->tag_id);
                    auto middle_row = new Q10FkPlan2MiddleRow(message_row, message_has_tag_row, tag_row);

                    insert_middle_row(middle_row);
                }
            }
            break;
        }
        case 6: {
            unsigned long message_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long tag_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto iter1 = tag_id_to_tag_row->find(tag_id);
            if (iter1 != tag_id_to_tag_row->end()) {
                auto message_has_tag_row = new MessageHasTagRow(message_id, tag_id);
                auto iter2 = message_id_to_message_has_tag_row->find(message_id);
                if (iter2 != message_id_to_message_has_tag_row->end()) {
                    iter2->second->emplace_back(message_has_tag_row);
                } else {
                    message_id_to_message_has_tag_row->emplace(message_id, new std::vector<MessageHasTagRow*>{message_has_tag_row});
                }

                auto iter3 = message_id_to_message_row->find(message_id);
                if (iter3 != message_id_to_message_row->end()) {
                    auto message_row = iter3->second;
                    auto tag_row = iter1->second;
                    auto middle_row = new Q10FkPlan2MiddleRow(message_row, message_has_tag_row, tag_row);

                    insert_middle_row(middle_row);
                }
            }
            break;
        }
        case 7: {
            unsigned long message_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long tag_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto iter = tag_id_to_tag_row2->find(tag_id);
            if (iter != tag_id_to_tag_row2->end()) {
                auto tag_row2 = iter->second;
                auto tag_class_row = tag_class_id_to_tag_class_row->at(tag_row2->type_tag_class_id);
                auto message_has_tag_row2 = new MessageHasTagRow(message_id, tag_id);
                auto right_row = new Q10FkPlan2RightRow(message_has_tag_row2, tag_row2, tag_class_row);

                insert_right_row(right_row);
            }
            break;
        }
        case 8: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string first_name = next_string(line, &begin);
            std::string last_name = next_string(line, &begin);
            std::string gender = next_string(line, &begin);
            std::string birthday = next_string(line, &begin);
            std::string location_ip = next_string(line, &begin);
            std::string browser_used = next_string(line, &begin);
            unsigned long location_city_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto iter1 = city_id_to_city_row->find(location_city_id);
            if (iter1 != city_id_to_city_row->end()) {
                auto person_row = new PersonRow(id, first_name, last_name, gender, birthday, location_ip, browser_used, location_city_id);
                person_id_to_person_row->emplace(id, person_row);

                auto iter2 = person1_id_to_person_knows_person_row->find(id);
                if (iter2 != person1_id_to_person_knows_person_row->end()) {
                    auto city_row = iter1->second;
                    auto country_row = country_id_to_country_row->at(city_row->part_of_place_id);
                    auto person_knows_person_rows = iter2->second;
                    for (auto person_knows_person_row : *person_knows_person_rows) {
                        auto iter3 = person_id_to_person_row2->find(person_knows_person_row->person2_id);
                        if (iter3 != person_id_to_person_row2->end()) {
                            auto person_row2 = iter3->second;
                            auto left_row = new Q10FkPlan2LeftRow(person_row, person_knows_person_row, person_row2, city_row, country_row);

                            insert_left_row(left_row);
                        }
                    }
                }
            }
            break;
        }
        case 9: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string first_name = next_string(line, &begin);
            std::string last_name = next_string(line, &begin);
            std::string gender = next_string(line, &begin);
            std::string birthday = next_string(line, &begin);
            std::string location_ip = next_string(line, &begin);
            std::string browser_used = next_string(line, &begin);
            unsigned long location_city_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto person_row2 = new PersonRow(id, first_name, last_name, gender, birthday, location_ip, browser_used, location_city_id);
            person_id_to_person_row2->emplace(id, person_row2);

            auto iter1 = person2_id_to_person_knows_person_row->find(id);
            if (iter1 != person2_id_to_person_knows_person_row->end()) {
                auto person_knows_person_rows = iter1->second;
                for (auto person_knows_person_row : *person_knows_person_rows) {
                    auto iter2 = person_id_to_person_row->find(person_knows_person_row->person1_id);
                    if (iter2 != person_id_to_person_row->end()) {
                        auto person_row = iter2->second;
                        auto city_row = city_id_to_city_row->at(person_row->location_city_id);
                        auto country_row = country_id_to_country_row->at(city_row->part_of_place_id);
                        auto left_row = new Q10FkPlan2LeftRow(person_row, person_knows_person_row, person_row2, city_row, country_row);

                        insert_left_row(left_row);
                    }
                }
            }
            break;
        }
        case 10: {
            unsigned long person1_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long person2_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto person_knows_person_row = new PersonKnowsPersonRow(person1_id, person2_id);
            auto iter1 = person1_id_to_person_knows_person_row->find(person1_id);
            if (iter1 != person1_id_to_person_knows_person_row->end()) {
                iter1->second->emplace_back(person_knows_person_row);
            } else {
                person1_id_to_person_knows_person_row->emplace(person1_id, new std::vector<PersonKnowsPersonRow*>{person_knows_person_row});
            }

            auto iter2 = person2_id_to_person_knows_person_row->find(person2_id);
            if (iter2 != person2_id_to_person_knows_person_row->end()) {
                iter2->second->emplace_back(person_knows_person_row);
            } else {
                person2_id_to_person_knows_person_row->emplace(person2_id, new std::vector<PersonKnowsPersonRow*>{person_knows_person_row});
            }

            auto iter3 = person_id_to_person_row->find(person1_id);
            if (iter3 != person_id_to_person_row->end()) {
                auto iter4 = person_id_to_person_row2->find(person2_id);
                if (iter4 != person_id_to_person_row2->end()) {
                    auto person_row = iter3->second;
                    auto person_row2 = iter4->second;
                    auto city_row = city_id_to_city_row->at(person_row->location_city_id);
                    auto country_row = country_id_to_country_row->at(city_row->part_of_place_id);
                    auto left_row = new Q10FkPlan2LeftRow(person_row, person_knows_person_row, person_row2, city_row, country_row);

                    insert_left_row(left_row);
                }
            }

            break;
        }
    }
}

inline void Q10FkPlan2Algorithm::insert_left_row(Q10FkPlan2LeftRow* left_row) {
    auto iter1 = person_id_to_left_row->find(left_row->person_row->id);
    if (iter1 != person_id_to_left_row->end()) {
        iter1->second->emplace_back(left_row);
    } else {
        person_id_to_left_row->emplace(left_row->person_row->id, new std::vector<Q10FkPlan2LeftRow*>{left_row});
    }
    q10_fk_plan2_attribute_left_middle->update_from_left(left_row);

    left_batch->set_left_row(left_row);
    left_batch->fill(reservoir);
}

inline void Q10FkPlan2Algorithm::insert_middle_row(Q10FkPlan2MiddleRow* middle_row) {
    auto person_id = middle_row->message_row->creator_person_id;
    auto message_id = middle_row->message_row->id;

    auto iter1 = person_id_to_middle_row->find(person_id);
    if (iter1 != person_id_to_middle_row->end()) {
        iter1->second->emplace_back(middle_row);
    } else {
        person_id_to_middle_row->emplace(person_id, new std::vector<Q10FkPlan2MiddleRow*>{middle_row});
    }

    auto iter2 = message_id_to_middle_row->find(message_id);
    if (iter2 != message_id_to_middle_row->end()) {
        iter2->second->emplace_back(middle_row);
    } else {
        message_id_to_middle_row->emplace(message_id, new std::vector<Q10FkPlan2MiddleRow*>{middle_row});
    }

    unsigned long wlcnt = q10_fk_plan2_attribute_left_middle->get_wlcnt(person_id);
    if (wlcnt > 0) {
        q10_fk_plan2_attribute_middle_right->update_from_left(middle_row, 0, wlcnt);
    }

    unsigned long wrcnt = q10_fk_plan2_attribute_middle_right->get_wrcnt(message_id);
    if (wrcnt > 0) {
        q10_fk_plan2_attribute_left_middle->update_from_right(middle_row, 0, wrcnt);
    }

    if (wlcnt > 0 && wrcnt > 0) {
        middle_batch->init(q10_fk_plan2_attribute_left_middle->get_left_layer(person_id),
                           middle_row,
                           q10_fk_plan2_attribute_middle_right->get_right_layer(message_id));
        middle_batch->fill(reservoir);
    }
}

inline void Q10FkPlan2Algorithm::insert_right_row(Q10FkPlan2RightRow* right_row) {
    auto iter1 = message_id_to_right_row->find(right_row->message_has_tag_row2->message_id);
    if (iter1 != message_id_to_right_row->end()) {
        iter1->second->emplace_back(right_row);
    } else {
        message_id_to_right_row->emplace(right_row->message_has_tag_row2->message_id, new std::vector<Q10FkPlan2RightRow*>{right_row});
    }
    q10_fk_plan2_attribute_middle_right->update_from_right(right_row);

    right_batch->set_right_row(right_row);
    right_batch->fill(reservoir);
}