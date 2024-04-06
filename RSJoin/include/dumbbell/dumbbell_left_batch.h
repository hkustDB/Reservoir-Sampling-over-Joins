#ifndef RESERVOIR_DUMBBELL_LEFT_BATCH_H
#define RESERVOIR_DUMBBELL_LEFT_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct DumbbellRow;
struct GraphRow;
struct TriangleRow;
class DumbbellAttributeLeftMiddle;
class DumbbellAttributeMiddleRight;

class DumbbellLeftBatch {
public:
    DumbbellLeftBatch(DumbbellAttributeLeftMiddle* dumbbell_attribute_left_middle, DumbbellAttributeMiddleRight* dumbbell_attribute_middle_right);

    void clear();
    void set_left_row(TriangleRow* row);
    void fill(Reservoir<DumbbellRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;

    DumbbellAttributeLeftMiddle* dumbbell_attribute_left_middle = nullptr;
    DumbbellAttributeMiddleRight* dumbbell_attribute_middle_right = nullptr;
    DumbbellRow* next_row = nullptr;

    TriangleRow* left_row = nullptr;
    GraphRow* middle_row = nullptr;
    TriangleRow* right_row = nullptr;

    unsigned long middle_layer_unit_size = 1;

    unsigned long middle_layer_index = 0;
    unsigned long right_layer_index = 0;

    std::unordered_map<unsigned long, std::vector<GraphRow*>*>* middle_layer = nullptr;
    std::vector<TriangleRow*>* right_layer = nullptr;

    void init_real_iterator();
    bool has_next_real();
    DumbbellRow* next_real();
    void compute_next_real();

    void fill_with_skip(Reservoir<DumbbellRow>* reservoir);
};


#endif 
