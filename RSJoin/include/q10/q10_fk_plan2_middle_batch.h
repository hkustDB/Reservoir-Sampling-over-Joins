#ifndef RESERVOIR_Q10_FK_PLAN2_MIDDLE_BATCH_H
#define RESERVOIR_Q10_FK_PLAN2_MIDDLE_BATCH_H

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

class Q10FkPlan2MiddleBatch {
public:
    void init(std::vector<Q10FkPlan2LeftRow*>* left, Q10FkPlan2MiddleRow* middle, std::vector<Q10FkPlan2RightRow*>* right);
    void fill(Reservoir<Q10FkPlan2Row>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long right_size = 1;

    std::vector<Q10FkPlan2LeftRow*>* left_rows = nullptr;
    Q10FkPlan2MiddleRow* middle_row = nullptr;
    std::vector<Q10FkPlan2RightRow*>* right_rows = nullptr;

    void fill_with_skip(Reservoir<Q10FkPlan2Row>* reservoir);
};


#endif //RESERVOIR_Q10_FK_PLAN2_MIDDLE_BATCH_H
