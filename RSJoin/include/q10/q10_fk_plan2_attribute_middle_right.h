#ifndef RESERVOIR_Q10_FK_PLAN2_ATTRIBUTE_MIDDLE_RIGHT_H
#define RESERVOIR_Q10_FK_PLAN2_ATTRIBUTE_MIDDLE_RIGHT_H

#include <unordered_map>
#include <vector>
#include "q10_rows.h"

class Q10FkPlan2AttributeLeftMiddle;

class Q10FkPlan2AttributeMiddleRight {
public:
    ~Q10FkPlan2AttributeMiddleRight();

    void set_left_attribute(Q10FkPlan2AttributeLeftMiddle* attribute);

    void set_left_relation_index(std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<unsigned long, std::vector<Q10FkPlan2RightRow*>*>* relation_index);

    void update_from_left(Q10FkPlan2MiddleRow* row, unsigned long old_count, unsigned long new_count);
    void update_from_right(Q10FkPlan2RightRow* row);

    unsigned long get_lcnt(unsigned long key);
    unsigned long get_rcnt(unsigned long key);
    unsigned long get_wrcnt(unsigned long key);

    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* get_left_layer(unsigned long key);
    std::vector<Q10FkPlan2RightRow*>* get_right_layer(unsigned long key);

    unsigned long get_left_layer_max_unit_size(unsigned long key);

private:
    Q10FkPlan2AttributeLeftMiddle* left_attribute;

    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* left_relation_index;
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2RightRow*>*>* right_relation_index;

    std::unordered_map<unsigned long, unsigned long> lcnt;
    std::unordered_map<unsigned long, unsigned long> rcnt;
    std::unordered_map<unsigned long, unsigned long> wrcnt;

    std::unordered_map<unsigned long, std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>*> left_layers;
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2RightRow*>*> right_layers;
    std::unordered_map<Q10FkPlan2MiddleRow, unsigned long, Q10FkPlan2MiddleRowHash> left_layer_indices;
    std::unordered_map<unsigned long, unsigned long> left_layer_max_unit_sizes;

    void propagate_to_left(unsigned long key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(unsigned long key, Q10FkPlan2MiddleRow* row, unsigned long old_count, unsigned long new_count);
    void update_right_layer(unsigned long key, Q10FkPlan2RightRow* row);
};


#endif
