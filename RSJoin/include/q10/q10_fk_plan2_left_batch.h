#ifndef RESERVOIR_Q10_FK_PLAN2_LEFT_BATCH_H
#define RESERVOIR_Q10_FK_PLAN2_LEFT_BATCH_H


#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct Q10FkPlan2Row;
struct Q10FkPlan2LeftRow;
struct Q10FkPlan2MiddleRow;
struct Q10FkPlan2RightRow;
class Q10FkPlan2AttributeLeftMiddle;
class Q10FkPlan2AttributeMiddleRight;

class Q10FkPlan2LeftBatch {
public:
    Q10FkPlan2LeftBatch(Q10FkPlan2AttributeLeftMiddle* q10_fk_plan2_attribute_left_middle,
                        Q10FkPlan2AttributeMiddleRight* q10_fk_plan2_attribute_middle_right);

    void clear();
    void set_left_row(Q10FkPlan2LeftRow* row);
    void fill(Reservoir<Q10FkPlan2Row>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;

    Q10FkPlan2AttributeLeftMiddle* q10_fk_plan2_attribute_left_middle = nullptr;
    Q10FkPlan2AttributeMiddleRight* q10_fk_plan2_attribute_middle_right = nullptr;
    Q10FkPlan2Row* next_row = nullptr;

    Q10FkPlan2LeftRow* left_row = nullptr;
    Q10FkPlan2MiddleRow* middle_row = nullptr;
    Q10FkPlan2RightRow* right_row = nullptr;

    unsigned long middle_layer_unit_size;

    unsigned long middle_layer_index;
    unsigned long right_layer_index;

    std::unordered_map<unsigned long, std::vector<Q10FkPlan2MiddleRow*>*>* middle_layer = nullptr;
    std::vector<Q10FkPlan2RightRow*>* right_layer = nullptr;

    void init_real_iterator();
    bool has_next_real();
    Q10FkPlan2Row* next_real();
    void compute_next_real();

    void fill_with_skip(Reservoir<Q10FkPlan2Row>* reservoir);
};


#endif //RESERVOIR_Q10_FK_PLAN2_LEFT_BATCH_H
