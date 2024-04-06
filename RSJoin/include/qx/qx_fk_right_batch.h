#ifndef RESERVOIR_QX_FK_RIGHT_BATCH_H
#define RESERVOIR_QX_FK_RIGHT_BATCH_H

#include <vector>

template<typename T>
class Reservoir;
struct QxRow;
struct QxFkLeftRow;
struct QxFkRightRow;

class QxFkRightBatch {
public:
    void init(std::vector<QxFkLeftRow*>* left_rows, QxFkRightRow* right_row);
    void fill(Reservoir<QxRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;

    std::vector<QxFkLeftRow*>* left_rows = nullptr;
    QxFkRightRow* right_row = nullptr;

    void fill_with_skip(Reservoir<QxRow>* reservoir);
};


#endif
