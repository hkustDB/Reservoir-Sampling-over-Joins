#include <fstream>
#include <iostream>
#include <vector>
#include "reservoir.h"
#include "utils.h"
#include "acyclic/join_tree.h"
#include "q10/q10_algorithm.h"
#include "q10/q10_rows.h"

Q10Algorithm::~Q10Algorithm() {
    delete reservoir;

    delete message_root;
    delete message_has_tag_root;
    delete message_has_tag_root2;
    delete person_root;
    delete person_root2;
    delete person_knows_person_root;

    for (auto row: tag_class_rows) {
        delete row;
    }
    tag_class_rows.clear();

    for (auto row: tag_rows) {
        delete row;
    }
    tag_rows.clear();

    for (auto row: tag_rows2) {
        delete row;
    }
    tag_rows2.clear();

    for (auto row: country_rows) {
        delete row;
    }
    country_rows.clear();

    for (auto row: city_rows) {
        delete row;
    }
    city_rows.clear();

    for (auto row: person_rows) {
        delete row;
    }
    person_rows.clear();

    for (auto row: person_rows2) {
        delete row;
    }
    person_rows2.clear();

    for (auto row: person_knows_person_rows) {
        delete row;
    }
    person_knows_person_rows.clear();

    for (auto row: message_rows) {
        delete row;
    }
    message_rows.clear();

    for (auto row: message_has_tag_rows) {
        delete row;
    }
    message_has_tag_rows.clear();

    for (auto row: message_has_tag_rows2) {
        delete row;
    }
    message_has_tag_rows2.clear();

    for (auto node: message_nodes) {
        delete node;
    }
    message_nodes.clear();

    for (auto node: message_has_tag_nodes) {
        delete node;
    }
    message_has_tag_nodes.clear();

    for (auto node: message_has_tag_nodes2) {
        delete node;
    }
    message_has_tag_nodes2.clear();

    for (auto node: person_nodes) {
        delete node;
    }
    person_nodes.clear();

    for (auto node: person_nodes2) {
        delete node;
    }
    person_nodes2.clear();

    for (auto node: person_knows_person_nodes) {
        delete node;
    }
    person_knows_person_nodes.clear();

    for (auto node: tag_nodes) {
        delete node;
    }
    tag_nodes.clear();

    for (auto node: tag_nodes2) {
        delete node;
    }
    tag_nodes2.clear();

    for (auto node: tag_class_nodes) {
        delete node;
    }
    tag_class_nodes.clear();

    for (auto node: city_nodes) {
        delete node;
    }
    city_nodes.clear();

    for (auto node: country_nodes) {
        delete node;
    }
    country_nodes.clear();
}

void Q10Algorithm::init(const std::string &file, int k) {
    this->input = file;
    this->reservoir = new Reservoir<Q10Row>(k);

    // Tree1: join tree rooted at message
    message_root = create_message_node();
    auto tree1_message_has_tag = build_message_has_tag_sub_tree();
    int tree1_message_has_tag_id = message_root->register_child(tree1_message_has_tag, [](MessageRow* t){ return t->id; });
    tree1_message_has_tag->register_parent(message_root, tree1_message_has_tag_id, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree1_message_has_tag2 = build_message_has_tag_sub_tree2();
    int tree1_message_has_tag_id2 = message_root->register_child(tree1_message_has_tag2, [](MessageRow* t){ return t->id; });
    tree1_message_has_tag2->register_parent(message_root, tree1_message_has_tag_id2, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree1_person = build_person_sub_tree();
    int tree1_person_id = message_root->register_child(tree1_person, [](MessageRow* t){ return t->creator_person_id; });
    tree1_person->register_parent(message_root, tree1_person_id, [](PersonRow* t){ return t->id; });

    // Tree2: join tree rooted at message_has_tag
    message_has_tag_root = create_message_has_tag_node();
    auto tree2_tag = create_tag_node();
    int tree2_tag_id = message_has_tag_root->register_child(tree2_tag, [](MessageHasTagRow* t){ return t->tag_id; });
    tree2_tag->register_parent(message_has_tag_root, tree2_tag_id, [](TagRow* t){ return t->id; });
    tag_nodes.emplace_back(tree2_tag);

    auto tree2_message = create_message_node();
    int tree2_message_id = message_has_tag_root->register_child(tree2_message, [](MessageHasTagRow* t){ return t->message_id; });
    tree2_message->register_parent(message_has_tag_root, tree2_message_id, [](MessageRow* t){ return t->id; });
    message_nodes.emplace_back(tree2_message);

    auto tree2_person = build_person_sub_tree();
    int tree2_person_id = tree2_message->register_child(tree2_person, [](MessageRow* t){ return t->creator_person_id; });
    tree2_person->register_parent(tree2_message, tree2_person_id, [](PersonRow* t){ return t->id; });

    auto tree2_message_has_tag2 = build_message_has_tag_sub_tree2();
    int tree2_message_has_tag_id2 = tree2_message->register_child(tree2_message_has_tag2, [](MessageRow* t){ return t->id; });
    tree2_message_has_tag2->register_parent(tree2_message, tree2_message_has_tag_id2, [](MessageHasTagRow* t){ return t->message_id; });

    // Tree3: join tree rooted at person
    person_root = create_person_node();
    auto tree3_message = create_message_node();
    int tree3_message_id = person_root->register_child(tree3_message, [](PersonRow* t){ return t->id; });
    tree3_message->register_parent(person_root, tree3_message_id, [](MessageRow* t){ return t->creator_person_id; });
    message_nodes.emplace_back(tree3_message);

    auto tree3_message_has_tag = build_message_has_tag_sub_tree();
    int tree3_message_has_tag_id = tree3_message->register_child(tree3_message_has_tag, [](MessageRow* t){ return t->id; });
    tree3_message_has_tag->register_parent(tree3_message, tree3_message_has_tag_id, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree3_message_has_tag2 = build_message_has_tag_sub_tree2();
    int tree3_message_has_tag_id2 = tree3_message->register_child(tree3_message_has_tag2, [](MessageRow* t){ return t->id; });
    tree3_message_has_tag2->register_parent(tree3_message, tree3_message_has_tag_id2, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree3_person_knows_person = create_person_knows_person_node();
    int tree3_person_knows_person_id = person_root->register_child(tree3_person_knows_person, [](PersonRow* t){ return t->id; });
    tree3_person_knows_person->register_parent(person_root, tree3_person_knows_person_id, [](PersonKnowsPersonRow* t){ return t->person1_id; });
    person_knows_person_nodes.emplace_back(tree3_person_knows_person);

    auto tree3_person2 = create_person_node2();
    int tree3_person_id2 = tree3_person_knows_person->register_child(tree3_person2, [](PersonKnowsPersonRow* t){ return t->person2_id; });
    tree3_person2->register_parent(tree3_person_knows_person, tree3_person_id2, [](PersonRow* t){ return t->id; });
    person_nodes2.emplace_back(tree3_person2);

    auto tree3_city = build_city_sub_tree();
    int tree3_city_id = person_root->register_child(tree3_city, [](PersonRow* t){ return t->location_city_id; });
    tree3_city->register_parent(person_root, tree3_city_id, [](CityRow* t){ return t->id; });

    // Tree4: join tree rooted at message_has_tag2
    message_has_tag_root2 = create_message_has_tag_node2();
    auto tree4_tag2 = create_tag_node2();
    int tree4_tag_id2 = message_has_tag_root2->register_child(tree4_tag2, [](MessageHasTagRow* t){ return t->tag_id; });
    tree4_tag2->register_parent(message_has_tag_root2, tree4_tag_id2, [](TagRow* t){ return t->id; });
    tag_nodes2.emplace_back(tree4_tag2);

    auto tree4_tag_class = create_tag_class_node();
    int tree4_tag_class_id = tree4_tag2->register_child(tree4_tag_class, [](TagRow* t){ return t->type_tag_class_id; });
    tree4_tag_class->register_parent(tree4_tag2, tree4_tag_class_id, [](TagClassRow* t){ return t->id; });
    tag_class_nodes.emplace_back(tree4_tag_class);

    auto tree4_message = create_message_node();
    int tree4_message_id = message_has_tag_root2->register_child(tree4_message, [](MessageHasTagRow* t){ return t->message_id; });
    tree4_message->register_parent(message_has_tag_root2, tree4_message_id, [](MessageRow* t){ return t->id; });
    message_nodes.emplace_back(tree4_message);

    auto tree4_message_has_tag = build_message_has_tag_sub_tree();
    int tree4_message_has_tag_id = tree4_message->register_child(tree4_message_has_tag, [](MessageRow* t){ return t->id; });
    tree4_message_has_tag->register_parent(tree4_message, tree4_message_has_tag_id, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree4_person = build_person_sub_tree();
    int tree4_person_id = tree4_message->register_child(tree4_person, [](MessageRow* t){ return t->creator_person_id; });
    tree4_person->register_parent(tree4_message, tree4_person_id, [](PersonRow* t){ return t->id; });

    // Tree5: join tree rooted at person_knows_person
    person_knows_person_root = create_person_knows_person_node();
    auto tree5_person = create_person_node();
    int tree5_person_id = person_knows_person_root->register_child(tree5_person, [](PersonKnowsPersonRow* t){ return t->person1_id; });
    tree5_person->register_parent(person_knows_person_root, tree5_person_id, [](PersonRow* t){ return t->id; });
    person_nodes.emplace_back(tree5_person);

    auto tree5_city = build_city_sub_tree();
    int tree5_city_id = tree5_person->register_child(tree5_city, [](PersonRow* t){ return t->location_city_id; });
    tree5_city->register_parent(tree5_person, tree5_city_id, [](CityRow* t){ return t->id; });

    auto tree5_message = create_message_node();
    int tree5_message_id = tree5_person->register_child(tree5_message, [](PersonRow* t){ return t->id; });
    tree5_message->register_parent(tree5_person, tree5_message_id, [](MessageRow* t){ return t->creator_person_id; });
    message_nodes.emplace_back(tree5_message);

    auto tree5_message_has_tag = build_message_has_tag_sub_tree();
    int tree5_message_has_tag_id = tree5_message->register_child(tree5_message_has_tag, [](MessageRow* t){ return t->id; });
    tree5_message_has_tag->register_parent(tree5_message, tree5_message_has_tag_id, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree5_message_has_tag2 = build_message_has_tag_sub_tree2();
    int tree5_message_has_tag_id2 = tree5_message->register_child(tree5_message_has_tag2, [](MessageRow* t){ return t->id; });
    tree5_message_has_tag2->register_parent(tree5_message, tree5_message_has_tag_id2, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree5_person2 = create_person_node2();
    int tree5_person_id2 = person_knows_person_root->register_child(tree5_person2, [](PersonKnowsPersonRow* t){ return t->person2_id; });
    tree5_person2->register_parent(person_knows_person_root, tree5_person_id2, [](PersonRow* t){ return t->id; });
    person_nodes2.emplace_back(tree5_person2);

    // Tree6: join tree rooted at person2
    person_root2 = create_person_node2();
    auto tree6_person_knows_person = create_person_knows_person_node();
    int tree6_person_knows_person_id = person_root2->register_child(tree6_person_knows_person, [](PersonRow* t){ return t->id; });
    tree6_person_knows_person->register_parent(person_root2, tree6_person_knows_person_id, [](PersonKnowsPersonRow* t){ return t->person2_id; });
    person_knows_person_nodes.emplace_back(tree6_person_knows_person);

    auto tree6_person = create_person_node();
    int tree6_person_id = tree6_person_knows_person->register_child(tree6_person, [](PersonKnowsPersonRow* t){ return t->person1_id; });
    tree6_person->register_parent(tree6_person_knows_person, tree6_person_id, [](PersonRow* t){ return t->id; });
    person_nodes.emplace_back(tree6_person);

    auto tree6_city = build_city_sub_tree();
    int tree6_city_id = tree6_person->register_child(tree6_city, [](PersonRow* t){ return t->location_city_id; });
    tree6_city->register_parent(tree6_person, tree6_city_id, [](CityRow* t){ return t->id; });

    auto tree6_message = create_message_node();
    int tree6_message_id = tree6_person->register_child(tree6_message, [](PersonRow* t){ return t->id; });
    tree6_message->register_parent(tree6_person, tree6_message_id, [](MessageRow* t){ return t->creator_person_id; });
    message_nodes.emplace_back(tree6_message);

    auto tree6_message_has_tag = build_message_has_tag_sub_tree();
    int tree6_message_has_tag_id = tree6_message->register_child(tree6_message_has_tag, [](MessageRow* t){ return t->id; });
    tree6_message_has_tag->register_parent(tree6_message, tree6_message_has_tag_id, [](MessageHasTagRow* t){ return t->message_id; });

    auto tree6_message_has_tag2 = build_message_has_tag_sub_tree2();
    int tree6_message_has_tag_id2 = tree6_message->register_child(tree6_message_has_tag2, [](MessageRow* t){ return t->id; });
    tree6_message_has_tag2->register_parent(tree6_message, tree6_message_has_tag_id2, [](MessageHasTagRow* t){ return t->message_id; });
}

Reservoir<Q10Row> *Q10Algorithm::run(bool print_info) {
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

void Q10Algorithm::process(const std::string &line) {
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
            tag_class_rows.emplace_back(tag_class_row);
            for (auto node: tag_class_nodes) {
                node->insert(tag_class_row);
            }
            break;
        }
        case 1: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long type_tag_class_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto tag_row = new TagRow(id, name, url, type_tag_class_id);
            tag_rows.emplace_back(tag_row);
            for (auto node: tag_nodes) {
                node->insert(tag_row);
            }
            break;
        }
        case 2: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long type_tag_class_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto tag_row2 = new TagRow(id, name, url, type_tag_class_id);
            tag_rows2.emplace_back(tag_row2);
            for (auto node: tag_nodes2) {
                node->insert(tag_row2);
            }
            break;
        }
        case 3: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long part_of_place_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto country_row = new CountryRow(id, name, url, part_of_place_id);
            country_rows.emplace_back(country_row);
            for (auto node: country_nodes) {
                node->insert(country_row);
            }
            break;
        }
        case 4: {
            unsigned long id = next_unsigned_long(line, &begin, lineSize, ',');
            std::string name = next_string(line, &begin);
            std::string url = next_string(line, &begin);
            unsigned long part_of_place_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto city_row = new CityRow(id, name, url, part_of_place_id);
            city_rows.emplace_back(city_row);
            for (auto node: city_nodes) {
                node->insert(city_row);
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
            message_rows.emplace_back(message_row);
            for (auto node: message_nodes) {
                node->insert(message_row);
            }
            auto batch = message_root->insert(message_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            break;
        }
        case 6: {
            unsigned long message_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long tag_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto message_has_tag_row = new MessageHasTagRow(message_id, tag_id);
            message_has_tag_rows.emplace_back(message_has_tag_row);
            for (auto node: message_has_tag_nodes) {
                node->insert(message_has_tag_row);
            }
            auto batch = message_has_tag_root->insert(message_has_tag_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            break;
        }
        case 7: {
            unsigned long message_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long tag_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto message_has_tag_row2 = new MessageHasTagRow(message_id, tag_id);
            message_has_tag_rows2.emplace_back(message_has_tag_row2);
            for (auto node: message_has_tag_nodes2) {
                node->insert(message_has_tag_row2);
            }
            auto batch = message_has_tag_root2->insert(message_has_tag_row2);
            if (batch != nullptr) {
                batch->fill(reservoir);
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
            auto person_row = new PersonRow(id, first_name, last_name, gender, birthday, location_ip, browser_used, location_city_id);
            person_rows.emplace_back(person_row);
            for (auto node: person_nodes) {
                node->insert(person_row);
            }
            auto batch = person_root->insert(person_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
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
            person_rows2.emplace_back(person_row2);
            for (auto node: person_nodes2) {
                node->insert(person_row2);
            }
            auto batch = person_root2->insert(person_row2);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            break;
        }
        case 10: {
            unsigned long person1_id = next_unsigned_long(line, &begin, lineSize, ',');
            unsigned long person2_id = next_unsigned_long(line, &begin, lineSize, ',');
            auto person_knows_person_row = new PersonKnowsPersonRow(person1_id, person2_id);
            person_knows_person_rows.emplace_back(person_knows_person_row);
            for (auto node: person_knows_person_nodes) {
                node->insert(person_knows_person_row);
            }
            auto batch = person_knows_person_root->insert(person_knows_person_row);
            if (batch != nullptr) {
                batch->fill(reservoir);
            }
            break;
        }
    }
}

Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* Q10Algorithm::build_message_has_tag_sub_tree() {
    auto message_has_tag = create_message_has_tag_node();
    message_has_tag_nodes.emplace_back(message_has_tag);
    auto tag = create_tag_node();
    tag_nodes.emplace_back(tag);

    int id = message_has_tag->register_child(tag, [](MessageHasTagRow* t){ return t->tag_id; });
    tag->register_parent(message_has_tag, id, [](TagRow* t){ return t->id; });

    return message_has_tag;
}

Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* Q10Algorithm::build_message_has_tag_sub_tree2() {
    auto message_has_tag2 = create_message_has_tag_node2();
    message_has_tag_nodes2.emplace_back(message_has_tag2);
    auto tag2 = create_tag_node2();
    tag_nodes2.emplace_back(tag2);
    auto tag_class = create_tag_class_node();
    tag_class_nodes.emplace_back(tag_class);

    int tag_id2 = message_has_tag2->register_child(tag2, [](MessageHasTagRow* t){ return t->tag_id; });
    tag2->register_parent(message_has_tag2, tag_id2, [](TagRow* t){ return t->id; });

    int tag_class_id = tag2->register_child(tag_class, [](TagRow* t){ return t->type_tag_class_id; });
    tag_class->register_parent(tag2, tag_class_id, [](TagClassRow* t){ return t->id; });

    return message_has_tag2;
}

Relation<Q10Row, PersonRow, PersonRowHash>* Q10Algorithm::build_person_sub_tree() {
    auto person = create_person_node();
    person_nodes.emplace_back(person);
    auto person2 = create_person_node2();
    person_nodes2.emplace_back(person2);
    auto person_knows_person = create_person_knows_person_node();
    person_knows_person_nodes.emplace_back(person_knows_person);

    int person_knows_person_id = person->register_child(person_knows_person, [](PersonRow* t){ return t->id; });
    person_knows_person->register_parent(person, person_knows_person_id, [](PersonKnowsPersonRow* t){ return t->person1_id; });

    int person_id2 = person_knows_person->register_child(person2, [](PersonKnowsPersonRow* t){ return t->person2_id; });
    person2->register_parent(person_knows_person, person_id2, [](PersonRow* t){ return t->id; });

    auto city = build_city_sub_tree();
    int city_id = person->register_child(city, [](PersonRow* t){ return t->location_city_id; });
    city->register_parent(person, city_id, [](CityRow* t){ return t->id; });

    return person;
}

Relation<Q10Row, CityRow, CityRowHash> *Q10Algorithm::build_city_sub_tree() {
    auto city = create_city_node();
    city_nodes.emplace_back(city);
    auto country = create_country_node();
    country_nodes.emplace_back(country);

    int country_id = city->register_child(country, [](CityRow* t){ return t->part_of_place_id; });
    country->register_parent(city, country_id, [](CountryRow* t){ return t->id; });

    return city;
}

Relation<Q10Row, MessageRow, MessageRowHash> *Q10Algorithm::create_message_node() {
    return new Relation<Q10Row, MessageRow, MessageRowHash>([](Q10Row* r, MessageRow* t){ r->message_row = t; });
}

Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash> *Q10Algorithm::create_message_has_tag_node() {
    return new Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>([](Q10Row* r, MessageHasTagRow* t){ r->message_has_tag_row = t; });
}

Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash> *Q10Algorithm::create_message_has_tag_node2() {
    return new Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>([](Q10Row* r, MessageHasTagRow* t){ r->message_has_tag_row2 = t; });
}

Relation<Q10Row, TagRow, TagRowHash> *Q10Algorithm::create_tag_node() {
    return new Relation<Q10Row, TagRow, TagRowHash>([](Q10Row* r, TagRow* t){ r->tag_row = t; });
}

Relation<Q10Row, TagRow, TagRowHash> *Q10Algorithm::create_tag_node2() {
    return new Relation<Q10Row, TagRow, TagRowHash>([](Q10Row* r, TagRow* t){ r->tag_row2 = t; });
}

Relation<Q10Row, TagClassRow, TagClassRowHash> *Q10Algorithm::create_tag_class_node() {
    return new Relation<Q10Row, TagClassRow, TagClassRowHash>([](Q10Row* r, TagClassRow* t){ r->tag_class_row = t; });
}

Relation<Q10Row, PersonRow, PersonRowHash> *Q10Algorithm::create_person_node() {
    return new Relation<Q10Row, PersonRow, PersonRowHash>([](Q10Row* r, PersonRow* t){ r->person_row = t; });
}

Relation<Q10Row, PersonRow, PersonRowHash> *Q10Algorithm::create_person_node2() {
    return new Relation<Q10Row, PersonRow, PersonRowHash>([](Q10Row* r, PersonRow* t){ r->person_row2 = t; });
}

Relation<Q10Row, PersonKnowsPersonRow, PersonKnowsPersonRowHash> *Q10Algorithm::create_person_knows_person_node() {
    return new Relation<Q10Row, PersonKnowsPersonRow, PersonKnowsPersonRowHash>([](Q10Row* r, PersonKnowsPersonRow* t){ r->person_knows_person_row = t; });
}

Relation<Q10Row, CityRow, CityRowHash> *Q10Algorithm::create_city_node() {
    return new Relation<Q10Row, CityRow, CityRowHash>([](Q10Row* r, CityRow* t){ r->city_row = t; });
}

Relation<Q10Row, CountryRow, CountryRowHash> *Q10Algorithm::create_country_node() {
    return new Relation<Q10Row, CountryRow, CountryRowHash>([](Q10Row* r, CountryRow* t){ r->country_row = t; });
}
