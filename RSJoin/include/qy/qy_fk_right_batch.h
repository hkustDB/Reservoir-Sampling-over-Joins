#ifndef RESERVOIR_QY_FK_RIGHT_BATCH_H
#define RESERVOIR_QY_FK_RIGHT_BATCH_H

#include <vector>

template<typename T>
class Reservoir;
struct QyRow;
struct QyFkLeftRow;
struct QyFkRightRow;

class QyFkRightBatch {
public:
    void init(std::vector<QyFkLeftRow*>* left_rows, QyFkRightRow* right_row);
    void fill(Reservoir<QyRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;

    std::vector<QyFkLeftRow*>* left_rows = nullptr;
    QyFkRightRow* right_row = nullptr;

    void fill_with_skip(Reservoir<QyRow>* reservoir);
};


#endif
