#ifndef RESERVOIR_QZ_FK_MIDDLE_BATCH_H
#define RESERVOIR_QZ_FK_MIDDLE_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QzRow;
struct ItemRow;
struct QzFkMiddleRow;
struct QzFkRightRow;
class QzFkGroupAttributeItemMiddle;
class QzFkGroupAttributeMiddleRight;

class QzFkMiddleBatch {
public:
    void init(std::vector<ItemRow*>* left, QzFkMiddleRow* middle, std::vector<QzFkRightRow*>* right);
    void fill(Reservoir<QzRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long right_size = 1;

    std::vector<ItemRow*>* item_rows = nullptr;
    QzFkMiddleRow* middle_row = nullptr;
    std::vector<QzFkRightRow*>* right_rows = nullptr;

    void fill_with_skip(Reservoir<QzRow>* reservoir);
};


#endif
