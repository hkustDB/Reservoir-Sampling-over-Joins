#ifndef RESERVOIR_QY_FK_LEFT_BATCH_H
#define RESERVOIR_QY_FK_LEFT_BATCH_H

#include <vector>

template<typename T>
class Reservoir;
struct QyRow;
struct QyFkLeftRow;
struct QyFkRightRow;

class QyFkLeftBatch {
public:
    void init(QyFkLeftRow* left_row, std::vector<QyFkRightRow*>* right_rows);
    void fill(Reservoir<QyRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;

    QyFkLeftRow* left_row = nullptr;
    std::vector<QyFkRightRow*>* right_rows = nullptr;

    void fill_with_skip(Reservoir<QyRow>* reservoir);
};


#endif
