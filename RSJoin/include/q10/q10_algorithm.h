#ifndef RESERVOIR_Q10_ALGORITHM_H
#define RESERVOIR_Q10_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include "q10_rows.h"

template<typename T>
class Reservoir;
template<typename R, typename T, typename H>
class Relation;

class Q10Algorithm {
public:
    ~Q10Algorithm();
    void init(const std::string& file, int k);
    Reservoir<Q10Row>* run(bool print_info);

private:
    std::string input;
    Reservoir<Q10Row>* reservoir = nullptr;

    // static
    std::vector<TagClassRow*> tag_class_rows;
    std::vector<TagRow*> tag_rows;
    std::vector<TagRow*> tag_rows2;
    std::vector<CountryRow*> country_rows;
    std::vector<CityRow*> city_rows;

    // dynamic
    std::vector<PersonRow*> person_rows;
    std::vector<PersonRow*> person_rows2;
    std::vector<PersonKnowsPersonRow*> person_knows_person_rows;
    std::vector<MessageRow*> message_rows;
    std::vector<MessageHasTagRow*> message_has_tag_rows;
    std::vector<MessageHasTagRow*> message_has_tag_rows2;

    // join tree roots
    Relation<Q10Row, MessageRow, MessageRowHash>* message_root = nullptr;
    Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* message_has_tag_root = nullptr;
    Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* message_has_tag_root2 = nullptr;
    Relation<Q10Row, PersonRow, PersonRowHash>* person_root = nullptr;
    Relation<Q10Row, PersonRow, PersonRowHash>* person_root2 = nullptr;
    Relation<Q10Row, PersonKnowsPersonRow, PersonKnowsPersonRowHash>* person_knows_person_root = nullptr;

    // tree nodes (roots are not included)
    std::vector<Relation<Q10Row, MessageRow, MessageRowHash>*> message_nodes;
    std::vector<Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>*> message_has_tag_nodes;
    std::vector<Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>*> message_has_tag_nodes2;
    std::vector<Relation<Q10Row, PersonRow, PersonRowHash>*> person_nodes;
    std::vector<Relation<Q10Row, PersonRow, PersonRowHash>*> person_nodes2;
    std::vector<Relation<Q10Row, PersonKnowsPersonRow, PersonKnowsPersonRowHash>*> person_knows_person_nodes;
    std::vector<Relation<Q10Row, TagRow, TagRowHash>*> tag_nodes;
    std::vector<Relation<Q10Row, TagRow, TagRowHash>*> tag_nodes2;
    std::vector<Relation<Q10Row, TagClassRow, TagClassRowHash>*> tag_class_nodes;
    std::vector<Relation<Q10Row, CityRow, CityRowHash>*> city_nodes;
    std::vector<Relation<Q10Row, CountryRow, CountryRowHash>*> country_nodes;

    void process(const std::string& line);

    // helper function for creating join trees
    // build message_has_tag x tag
    Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* build_message_has_tag_sub_tree();
    // build message_has_tag2 x tag2 x tag_class
    Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* build_message_has_tag_sub_tree2();
    // build person x person_knows_person x person2 x city x country
    Relation<Q10Row, PersonRow, PersonRowHash>* build_person_sub_tree();
    // build city x country
    Relation<Q10Row, CityRow, CityRowHash>* build_city_sub_tree();

    // helper function for creating tree nodes
    Relation<Q10Row, MessageRow, MessageRowHash>* create_message_node();
    Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* create_message_has_tag_node();
    Relation<Q10Row, MessageHasTagRow, MessageHasTagRowHash>* create_message_has_tag_node2();
    Relation<Q10Row, PersonRow, PersonRowHash>* create_person_node();
    Relation<Q10Row, PersonRow, PersonRowHash>* create_person_node2();
    Relation<Q10Row, PersonKnowsPersonRow, PersonKnowsPersonRowHash>* create_person_knows_person_node();
    Relation<Q10Row, TagRow, TagRowHash>* create_tag_node();
    Relation<Q10Row, TagRow, TagRowHash>* create_tag_node2();
    Relation<Q10Row, TagClassRow, TagClassRowHash>* create_tag_class_node();
    Relation<Q10Row, CityRow, CityRowHash>* create_city_node();
    Relation<Q10Row, CountryRow, CountryRowHash>* create_country_node();
};


#endif
