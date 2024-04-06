#ifndef RESERVOIR_Q10_FK_PLAN2_ALGORITHM_H
#define RESERVOIR_Q10_FK_PLAN2_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include "q10_rows.h"

template<typename T>
class Reservoir;
struct Q10FkPlan2Row;
struct Q10FkPlan2LeftRow;
struct Q10FkPlan2MiddleRow;
struct Q10FkPlan2RightRow;
class Q10FkPlan2LeftBatch;
class Q10FkPlan2MiddleBatch;
class Q10FkPlan2RightBatch;
class Q10FkPlan2AttributeLeftMiddle;
class Q10FkPlan2AttributeMiddleRight;

class Q10FkPlan2Algorithm {
public:
    ~Q10FkPlan2Algorithm();
    void init(const std::string& file, int k);
    Reservoir<Q10FkPlan2Row>* run(bool print_info);

private:
    std::string input;
    Q10FkPlan2LeftBatch* left_batch = nullptr;
    Q10FkPlan2MiddleBatch* middle_batch = nullptr;
    Q10FkPlan2RightBatch* right_batch = nullptr;
    Reservoir<Q10FkPlan2Row>* reservoir = nullptr;

    std::unordered_map<unsigned long, std::vector<Q10FkPlan2LeftRow*>*>* person_id_to_left_row = nullptr;
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* person_id_to_middle_row = nullptr;
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* message_id_to_middle_row = nullptr;
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2RightRow*>*>* message_id_to_right_row = nullptr;

    std::unordered_map<unsigned long, TagClassRow*>* tag_class_id_to_tag_class_row = nullptr;
    std::unordered_map<unsigned long, TagRow*>* tag_id_to_tag_row = nullptr;
    std::unordered_map<unsigned long, TagRow*>* tag_id_to_tag_row2 = nullptr;
    std::unordered_map<unsigned long, CountryRow*>* country_id_to_country_row = nullptr;
    std::unordered_map<unsigned long, CityRow*>* city_id_to_city_row = nullptr;
    std::unordered_map<unsigned long, PersonRow*>* person_id_to_person_row = nullptr;
    std::unordered_map<unsigned long, PersonRow*>* person_id_to_person_row2 = nullptr;
    std::unordered_map<unsigned long, MessageRow*>* message_id_to_message_row = nullptr;
    std::unordered_map<unsigned long, std::vector<PersonKnowsPersonRow*>*>* person1_id_to_person_knows_person_row = nullptr;
    std::unordered_map<unsigned long, std::vector<PersonKnowsPersonRow*>*>* person2_id_to_person_knows_person_row = nullptr;
    std::unordered_map<unsigned long, std::vector<MessageHasTagRow*>*>* message_id_to_message_has_tag_row = nullptr;

    Q10FkPlan2AttributeLeftMiddle* q10_fk_plan2_attribute_left_middle;
    Q10FkPlan2AttributeMiddleRight* q10_fk_plan2_attribute_middle_right;

    void process(const std::string& line);

    void insert_left_row(Q10FkPlan2LeftRow* left_row);
    void insert_middle_row(Q10FkPlan2MiddleRow* middle_row);
    void insert_right_row(Q10FkPlan2RightRow* right_row);
};


#endif //RESERVOIR_Q10_FK_PLAN2_ALGORITHM_H
