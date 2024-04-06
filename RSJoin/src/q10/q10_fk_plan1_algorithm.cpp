#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/acyclic/join_tree.h"
#include "../../include/q10/q10_fk_plan1_algorithm.h"
#include "../../include/q10/q10_rows.h"

Q10FkPlan1Algorithm::~Q10FkPlan1Algorithm() {
    for (auto p : *tag_class_id_to_tag_class_row) {
        delete p.second;
    }
    tag_class_id_to_tag_class_row->clear();
    delete tag_class_id_to_tag_class_row;

    for (auto p : *tag_id_to_tag_row) {
        delete p.second;
    }
    tag_id_to_tag_row->clear();
    delete tag_id_to_tag_row;

    for (auto p : *tag_id_to_tag_row2) {
        delete p.second;
    }
    tag_id_to_tag_row2->clear();
    delete tag_id_to_tag_row2;

    for (auto p : *country_id_to_country_row) {
        delete p.second;
    }
    country_id_to_country_row->clear();
    delete country_id_to_country_row;

    for (auto p : *city_id_to_city_row) {
        delete p.second;
    }
    city_id_to_city_row->clear();
    delete city_id_to_city_row;

    for (auto p : *person_id_to_person_row) {
        delete p.second;
    }
    person_id_to_person_row->clear();
    delete person_id_to_person_row;

    for (auto p : *person_id_to_person_row2) {
        delete p.second;
    }
    person_id_to_person_row2->clear();
    delete person_id_to_person_row2;

    for (auto p : *person2_id_to_person_knows_person_row) {
        auto vec = p.second;
        for (auto row : *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    person2_id_to_person_knows_person_row->clear();
    delete person2_id_to_person_knows_person_row;

    for (auto p : *creator_person_id_to_message_row) {
        auto vec = p.second;
        for (auto row : *vec) {
            delete row;
        }
        vec->clear();
        delete vec;
    }
    creator_person_id_to_message_row->clear();
    delete creator_person_id_to_message_row;

    for (auto p : message_person_city_country_rows) {
        delete p;
    }

    for (auto p : message_has_tag_tag_rows) {
        delete p->message_has_tag_row;
        delete p;
    }

    for (auto p : message_has_tag_tag_tag_class_rows) {
        delete p->message_has_tag_row2;
        delete p;
    }

    for (auto p : person_knows_person_person_rows) {
        delete p;
    }

    delete tree1_message_person_city_country;
    delete tree2_message_person_city_country;
    delete tree3_message_person_city_country;
    delete tree4_message_person_city_country;

    delete tree1_message_has_tag_tag;
    delete tree2_message_has_tag_tag;
    delete tree3_message_has_tag_tag;
    delete tree4_message_has_tag_tag;

    delete tree1_message_has_tag_tag_tag_class;
    delete tree2_message_has_tag_tag_tag_class;
    delete tree3_message_has_tag_tag_tag_class;
    delete tree4_message_has_tag_tag_tag_class;

    delete tree1_person_knows_person_person;
    delete tree2_person_knows_person_person;
    delete tree3_person_knows_person_person;
    delete tree4_person_knows_person_person;
}

void Q10FkPlan1Algorithm::init(const std::string &file, int k) {
    this->input = file;
    this->reservoir = new Reservoir<Q10FkPlan1Row>(k);

    this->tag_class_id_to_tag_class_row = new std::unordered_map<unsigned long, TagClassRow*>;
    this->tag_id_to_tag_row = new std::unordered_map<unsigned long, TagRow*>;
    this->tag_id_to_tag_row2 = new std::unordered_map<unsigned long, TagRow*>;
    this->country_id_to_country_row = new std::unordered_map<unsigned long, CountryRow*>;
    this->city_id_to_city_row = new std::unordered_map<unsigned long, CityRow*>;
    this->person_id_to_person_row = new std::unordered_map<unsigned long, PersonRow*>;
    this->person_id_to_person_row2 = new std::unordered_map<unsigned long, PersonRow*>;
    this->person2_id_to_person_knows_person_row = new std::unordered_map<unsigned long, std::vector<PersonKnowsPersonRow*>*>;
    this->creator_person_id_to_message_row = new std::unordered_map<unsigned long, std::vector<MessageRow*>*>;

    // build join tree rooted at MessagePersonCityCountry
    tree1_message_person_city_country = new Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>(
                           [](Q10FkPlan1Row* r, MessagePersonCityCountryRow* t){ r->message_person_city_country_row = t; });
    tree1_message_has_tag_tag = new Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagRow* t){ r->message_has_tag_tag_row = t; });
    tree1_message_has_tag_tag_tag_class = new Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagTagClassRow* t){ r->message_has_tag_tag_tag_class_row = t; });
    tree1_person_knows_person_person = new Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>(
                           [](Q10FkPlan1Row* r, PersonKnowsPersonPersonRow* t){ r->person_knows_person_person_row = t; });

    int tree1_message_has_tag_tag_id = tree1_message_person_city_country->register_child(tree1_message_has_tag_tag,
                                               [](MessagePersonCityCountryRow* t){ return t->message_row->id; });
    tree1_message_has_tag_tag->register_parent(tree1_message_person_city_country, tree1_message_has_tag_tag_id,
                                               [](MessageHasTagTagRow* t){ return t->message_has_tag_row->message_id; });
    int tree1_message_has_tag_tag_tag_class_id = tree1_message_person_city_country->register_child(tree1_message_has_tag_tag_tag_class,
                                               [](MessagePersonCityCountryRow* t){ return t->message_row->id; });
    tree1_message_has_tag_tag_tag_class->register_parent(tree1_message_person_city_country, tree1_message_has_tag_tag_tag_class_id,
                                               [](MessageHasTagTagTagClassRow* t){ return t->message_has_tag_row2->message_id; });
    int tree1_person_knows_person_person_id = tree1_message_person_city_country->register_child(tree1_person_knows_person_person,
                                               [](MessagePersonCityCountryRow* t){ return t->person_row->id; });
    tree1_person_knows_person_person->register_parent(tree1_message_person_city_country, tree1_person_knows_person_person_id,
                                               [](PersonKnowsPersonPersonRow* t){ return t->person_knows_person_row->person1_id; });

    // build join tree rooted at MessageHasTagTag
    tree2_message_has_tag_tag = new Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagRow* t){ r->message_has_tag_tag_row = t; });
    tree2_message_person_city_country = new Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>(
                           [](Q10FkPlan1Row* r, MessagePersonCityCountryRow* t){ r->message_person_city_country_row = t; });
    tree2_message_has_tag_tag_tag_class = new Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagTagClassRow* t){ r->message_has_tag_tag_tag_class_row = t; });
    tree2_person_knows_person_person = new Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>(
                           [](Q10FkPlan1Row* r, PersonKnowsPersonPersonRow* t){ r->person_knows_person_person_row = t; });

    int tree2_message_person_city_country_id = tree2_message_has_tag_tag->register_child(tree2_message_person_city_country,
                                               [](MessageHasTagTagRow* t){ return t->message_has_tag_row->message_id; });
    tree2_message_person_city_country->register_parent(tree2_message_has_tag_tag, tree2_message_person_city_country_id,
                                               [](MessagePersonCityCountryRow* t){ return t->message_row->id; });
    int tree2_message_has_tag_tag_tag_class_id = tree2_message_has_tag_tag->register_child(tree2_message_has_tag_tag_tag_class,
                                               [](MessageHasTagTagRow* t){ return t->message_has_tag_row->message_id; });
    tree2_message_has_tag_tag_tag_class->register_parent(tree2_message_has_tag_tag, tree2_message_has_tag_tag_tag_class_id,
                                               [](MessageHasTagTagTagClassRow* t){ return t->message_has_tag_row2->message_id; });
    int tree2_person_knows_person_person_id = tree2_message_person_city_country->register_child(tree2_person_knows_person_person,
                                               [](MessagePersonCityCountryRow* t){ return t->person_row->id; });
    tree2_person_knows_person_person->register_parent(tree2_message_person_city_country, tree2_person_knows_person_person_id,
                                               [](PersonKnowsPersonPersonRow* t){ return t->person_knows_person_row->person1_id; });

    // build join tree rooted at MessageHasTagTagTagClass
    tree3_message_has_tag_tag_tag_class = new Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagTagClassRow* t){ r->message_has_tag_tag_tag_class_row = t; });
    tree3_message_person_city_country = new Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>(
                           [](Q10FkPlan1Row* r, MessagePersonCityCountryRow* t){ r->message_person_city_country_row = t; });
    tree3_message_has_tag_tag = new Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagRow* t){ r->message_has_tag_tag_row = t; });
    tree3_person_knows_person_person = new Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>(
                           [](Q10FkPlan1Row* r, PersonKnowsPersonPersonRow* t){ r->person_knows_person_person_row = t; });

    int tree3_message_person_city_country_id = tree3_message_has_tag_tag_tag_class->register_child(tree3_message_person_city_country,
                                               [](MessageHasTagTagTagClassRow* t){ return t->message_has_tag_row2->message_id; });
    tree3_message_person_city_country->register_parent(tree3_message_has_tag_tag_tag_class, tree3_message_person_city_country_id,
                                               [](MessagePersonCityCountryRow* t){ return t->message_row->id; });
    int tree3_message_has_tag_tag_id = tree3_message_has_tag_tag_tag_class->register_child(tree3_message_has_tag_tag,
                                               [](MessageHasTagTagTagClassRow* t){ return t->message_has_tag_row2->message_id; });
    tree3_message_has_tag_tag->register_parent(tree3_message_has_tag_tag_tag_class, tree3_message_has_tag_tag_id,
                                               [](MessageHasTagTagRow* t){ return t->message_has_tag_row->message_id; });
    int tree3_person_knows_person_person_id = tree3_message_person_city_country->register_child(tree3_person_knows_person_person,
                                               [](MessagePersonCityCountryRow* t){ return t->person_row->id; });
    tree3_person_knows_person_person->register_parent(tree3_message_person_city_country, tree3_person_knows_person_person_id,
                                               [](PersonKnowsPersonPersonRow* t){ return t->person_knows_person_row->person1_id; });

    // build join tree rooted at PersonKnowsPersonPerson
    tree4_person_knows_person_person = new Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>(
                           [](Q10FkPlan1Row* r, PersonKnowsPersonPersonRow* t){ r->person_knows_person_person_row = t; });
    tree4_message_person_city_country = new Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>(
                           [](Q10FkPlan1Row* r, MessagePersonCityCountryRow* t){ r->message_person_city_country_row = t; });
    tree4_message_has_tag_tag = new Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagRow* t){ r->message_has_tag_tag_row = t; });
    tree4_message_has_tag_tag_tag_class = new Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>(
                           [](Q10FkPlan1Row* r, MessageHasTagTagTagClassRow* t){ r->message_has_tag_tag_tag_class_row = t; });

    int tree4_message_person_city_country_id = tree4_person_knows_person_person->register_child(tree4_message_person_city_country,
                                               [](PersonKnowsPersonPersonRow* t){ return t->person_knows_person_row->person1_id; });
    tree4_message_person_city_country->register_parent(tree4_person_knows_person_person, tree4_message_person_city_country_id,
                                               [](MessagePersonCityCountryRow* t){ return t->person_row->id; });
    int tree4_message_has_tag_tag_id = tree4_message_person_city_country->register_child(tree4_message_has_tag_tag,
                                               [](MessagePersonCityCountryRow* t){ return t->message_row->id; });
    tree4_message_has_tag_tag->register_parent(tree4_message_person_city_country, tree4_message_has_tag_tag_id,
                                               [](MessageHasTagTagRow* t){ return t->message_has_tag_row->message_id; });
    int tree4_message_has_tag_tag_tag_class_id = tree4_message_person_city_country->register_child(tree4_message_has_tag_tag_tag_class,
                                               [](MessagePersonCityCountryRow* t){ return t->message_row->id; });
    tree4_message_has_tag_tag_tag_class->register_parent(tree4_message_person_city_country, tree4_message_has_tag_tag_tag_class_id,
                                               [](MessageHasTagTagTagClassRow* t){ return t->message_has_tag_row2->message_id; });
}

Reservoir<Q10FkPlan1Row> *Q10FkPlan1Algorithm::run(bool print_info) {
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

void Q10FkPlan1Algorithm::process(const std::string &line) {
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
            auto iter1 = creator_person_id_to_message_row->find(creator_person_id);
            if (iter1 != creator_person_id_to_message_row->end()) {
                iter1->second->emplace_back(message_row);
            } else {
                creator_person_id_to_message_row->emplace(creator_person_id, new std::vector<MessageRow*>{message_row});
            }
            
            auto iter2 = person_id_to_person_row->find(creator_person_id);
            if (iter2 != person_id_to_person_row->end()) {
                auto person_row = iter2->second;
                auto city_row = city_id_to_city_row->at(person_row->location_city_id);
                auto country_row = country_id_to_country_row->at(city_row->part_of_place_id);
                auto message_person_city_country_row = new MessagePersonCityCountryRow(message_row, person_row, city_row, country_row);
                message_person_city_country_rows.emplace_back(message_person_city_country_row);
                auto batch = tree1_message_person_city_country->insert(message_person_city_country_row);
                if (batch != nullptr) {
                    batch->fill(reservoir);
                }
                tree2_message_person_city_country->insert(message_person_city_country_row);
                tree3_message_person_city_country->insert(message_person_city_country_row);
                tree4_message_person_city_country->insert(message_person_city_country_row);
            }
            break;
        }
        case 6: {
            unsigned long message_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long tag_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto iter = tag_id_to_tag_row->find(tag_id);
            if (iter != tag_id_to_tag_row->end()) {
                auto tag_row = iter->second;
                auto message_has_tag_row = new MessageHasTagRow(message_id, tag_id);

                auto message_has_tag_tag_row = new MessageHasTagTagRow(message_has_tag_row, tag_row);
                message_has_tag_tag_rows.emplace_back(message_has_tag_tag_row);
                tree1_message_has_tag_tag->insert(message_has_tag_tag_row);
                auto batch = tree2_message_has_tag_tag->insert(message_has_tag_tag_row);
                if (batch != nullptr) {
                    batch->fill(reservoir);
                }
                tree3_message_has_tag_tag->insert(message_has_tag_tag_row);
                tree4_message_has_tag_tag->insert(message_has_tag_tag_row);
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

                auto message_has_tag_tag_tag_class_row = new MessageHasTagTagTagClassRow(message_has_tag_row2, tag_row2, tag_class_row);
                message_has_tag_tag_tag_class_rows.emplace_back(message_has_tag_tag_tag_class_row);
                tree1_message_has_tag_tag_tag_class->insert(message_has_tag_tag_tag_class_row);
                tree2_message_has_tag_tag_tag_class->insert(message_has_tag_tag_tag_class_row);
                auto batch = tree3_message_has_tag_tag_tag_class->insert(message_has_tag_tag_tag_class_row);
                if (batch != nullptr) {
                    batch->fill(reservoir);
                }
                tree4_message_has_tag_tag_tag_class->insert(message_has_tag_tag_tag_class_row);
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
                auto iter2 = creator_person_id_to_message_row->find(id);
                if (iter2 != creator_person_id_to_message_row->end()) {
                    auto city_row = iter1->second;
                    auto country_row = country_id_to_country_row->at(city_row->part_of_place_id);
                    for (MessageRow* message_row : *iter2->second) {
                        auto message_person_city_country_row = new MessagePersonCityCountryRow(message_row, person_row, city_row, country_row);
                        message_person_city_country_rows.emplace_back(message_person_city_country_row);
                        auto batch = tree1_message_person_city_country->insert(message_person_city_country_row);
                        if (batch != nullptr) {
                            batch->fill(reservoir);
                        }
                        tree2_message_person_city_country->insert(message_person_city_country_row);
                        tree3_message_person_city_country->insert(message_person_city_country_row);
                        tree4_message_person_city_country->insert(message_person_city_country_row);
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
                for (PersonKnowsPersonRow* person_knows_person_row : *iter1->second) {
                    auto person_knows_person_person_row = new PersonKnowsPersonPersonRow(person_knows_person_row, person_row2);
                    person_knows_person_person_rows.emplace_back(person_knows_person_person_row);
                    tree1_person_knows_person_person->insert(person_knows_person_person_row);
                    tree2_person_knows_person_person->insert(person_knows_person_person_row);
                    tree3_person_knows_person_person->insert(person_knows_person_person_row);
                    auto batch = tree4_person_knows_person_person->insert(person_knows_person_person_row);
                    if (batch != nullptr) {
                        batch->fill(reservoir);
                    }
                }
            }
            break;
        }
        case 10: {
            unsigned long person1_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long person2_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto person_knows_person_row = new PersonKnowsPersonRow(person1_id, person2_id);
            auto iter1 = person2_id_to_person_knows_person_row->find(person2_id);
            if (iter1 != person2_id_to_person_knows_person_row->end()) {
                iter1->second->emplace_back(person_knows_person_row);
            } else {
                person2_id_to_person_knows_person_row->emplace(person2_id, new std::vector<PersonKnowsPersonRow*>{person_knows_person_row});
            }
            auto iter2 = person_id_to_person_row2->find(person2_id);
            if (iter2 != person_id_to_person_row2->end()) {
                auto person_row2 = person_id_to_person_row2->at(person2_id);
                auto person_knows_person_person_row = new PersonKnowsPersonPersonRow(person_knows_person_row, person_row2);
                person_knows_person_person_rows.emplace_back(person_knows_person_person_row);
                tree1_person_knows_person_person->insert(person_knows_person_person_row);
                tree2_person_knows_person_person->insert(person_knows_person_person_row);
                tree3_person_knows_person_person->insert(person_knows_person_person_row);
                auto batch = tree4_person_knows_person_person->insert(person_knows_person_person_row);
                if (batch != nullptr) {
                    batch->fill(reservoir);
                }
            }
            break;
        }
    }
}