#ifndef RESERVOIR_DUMBBELL_MIDDLE_BATCH_H
#define RESERVOIR_DUMBBELL_MIDDLE_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct DumbbellRow;
struct GraphRow;
struct TriangleRow;
class DumbbellAttributeLeftMiddle;
class DumbbellAttributeMiddleRight;

class DumbbellMiddleBatch {
public:
    void init(std::vector<TriangleRow*>* left, std::vector<TriangleRow*>* right);
    void fill(Reservoir<DumbbellRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long right_size = 1;

    std::vector<TriangleRow*>* left_rows = nullptr;
    std::vector<TriangleRow*>* right_rows = nullptr;

    void fill_with_skip(Reservoir<DumbbellRow>* reservoir);
};


#endif
