#ifndef RESERVOIR_Q10_FK_PLAN1_ALGORITHM_H
#define RESERVOIR_Q10_FK_PLAN1_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include "q10_rows.h"

template<typename T>
class Reservoir;

class Q10FkPlan1Algorithm {
public:
    ~Q10FkPlan1Algorithm();
    void init(const std::string& file, int k);
    Reservoir<Q10FkPlan1Row>* run(bool print_info);

private:
    std::string input;
    Reservoir<Q10FkPlan1Row>* reservoir = nullptr;

    std::unordered_map<unsigned long, TagClassRow*>* tag_class_id_to_tag_class_row = nullptr;
    std::unordered_map<unsigned long, TagRow*>* tag_id_to_tag_row = nullptr;
    std::unordered_map<unsigned long, TagRow*>* tag_id_to_tag_row2 = nullptr;
    std::unordered_map<unsigned long, CountryRow*>* country_id_to_country_row = nullptr;
    std::unordered_map<unsigned long, CityRow*>* city_id_to_city_row = nullptr;
    std::unordered_map<unsigned long, PersonRow*>* person_id_to_person_row = nullptr;
    std::unordered_map<unsigned long, PersonRow*>* person_id_to_person_row2 = nullptr;
    std::unordered_map<unsigned long, std::vector<PersonKnowsPersonRow*>*>* person2_id_to_person_knows_person_row = nullptr;
    std::unordered_map<unsigned long, std::vector<MessageRow*>*>* creator_person_id_to_message_row = nullptr;

    // join tree rooted at MessagePersonCityCountry
    Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>* tree1_message_person_city_country;
    Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>* tree1_message_has_tag_tag;
    Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>* tree1_message_has_tag_tag_tag_class;
    Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>* tree1_person_knows_person_person;

    // join tree rooted at MessageHasTagTag
    Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>* tree2_message_person_city_country;
    Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>* tree2_message_has_tag_tag;
    Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>* tree2_message_has_tag_tag_tag_class;
    Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>* tree2_person_knows_person_person;

    // join tree rooted at MessageHasTagTagTagClass
    Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>* tree3_message_person_city_country;
    Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>* tree3_message_has_tag_tag;
    Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>* tree3_message_has_tag_tag_tag_class;
    Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>* tree3_person_knows_person_person;

    // join tree rooted at PersonKnowsPersonPerson
    Relation<Q10FkPlan1Row, MessagePersonCityCountryRow, MessagePersonCityCountryRowHash>* tree4_message_person_city_country;
    Relation<Q10FkPlan1Row, MessageHasTagTagRow, MessageHasTagTagRowHash>* tree4_message_has_tag_tag;
    Relation<Q10FkPlan1Row, MessageHasTagTagTagClassRow, MessageHasTagTagTagClassRowHash>* tree4_message_has_tag_tag_tag_class;
    Relation<Q10FkPlan1Row, PersonKnowsPersonPersonRow, PersonKnowsPersonPersonRowHash>* tree4_person_knows_person_person;

    // for destruction
    std::vector<MessagePersonCityCountryRow*> message_person_city_country_rows;
    std::vector<MessageHasTagTagRow*> message_has_tag_tag_rows;
    std::vector<MessageHasTagTagTagClassRow*> message_has_tag_tag_tag_class_rows;
    std::vector<PersonKnowsPersonPersonRow*> person_knows_person_person_rows;

    void process(const std::string& line);
};


#endif
