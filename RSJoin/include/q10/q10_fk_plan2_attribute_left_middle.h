#ifndef RESERVOIR_Q10_FK_PLAN2_ATTRIBUTE_LEFT_MIDDLE_H
#define RESERVOIR_Q10_FK_PLAN2_ATTRIBUTE_LEFT_MIDDLE_H


#include <unordered_map>
#include <vector>
#include "q10_rows.h"

class Q10FkPlan2AttributeMiddleRight;

class Q10FkPlan2AttributeLeftMiddle {
public:
    ~Q10FkPlan2AttributeLeftMiddle();

    void set_right_attribute(Q10FkPlan2AttributeMiddleRight* attribute);

    void set_left_relation_index(std::unordered_map<unsigned long, std::vector<Q10FkPlan2LeftRow*>*>* relation_index);
    void set_right_relation_index(std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* relation_index);

    void update_from_left(Q10FkPlan2LeftRow* row);
    void update_from_right(Q10FkPlan2MiddleRow* row, unsigned long old_count, unsigned long new_count);

    unsigned long get_lcnt(unsigned long key);
    unsigned long get_wlcnt(unsigned long key);
    unsigned long get_rcnt(unsigned long key);

    std::vector<Q10FkPlan2LeftRow*>* get_left_layer(unsigned long key);
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* get_right_layer(unsigned long key);

    unsigned long get_right_layer_max_unit_size(unsigned long key);

private:
    Q10FkPlan2AttributeMiddleRight* right_attribute;

    std::unordered_map<unsigned long, std::vector<Q10FkPlan2LeftRow*>*>* left_relation_index;
    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* right_relation_index;

    std::unordered_map<unsigned long, unsigned long> lcnt;
    std::unordered_map<unsigned long, unsigned long> wlcnt;
    std::unordered_map<unsigned long, unsigned long> rcnt;

    std::unordered_map<unsigned long, std::vector<Q10FkPlan2LeftRow*>*> left_layers;
    std::unordered_map<unsigned long, std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>*> right_layers;
    std::unordered_map<Q10FkPlan2MiddleRow, unsigned long, Q10FkPlan2MiddleRowHash> right_layer_indices;
    std::unordered_map<unsigned long, unsigned long> right_layer_max_unit_sizes;

    void propagate_to_right(unsigned long key, unsigned long old_count, unsigned long new_count);
    void update_left_layer(unsigned long key, Q10FkPlan2LeftRow* row);
    void update_right_layer(unsigned long key, Q10FkPlan2MiddleRow* row, unsigned long old_count, unsigned long new_count);
};

#endif
