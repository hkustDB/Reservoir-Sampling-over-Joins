#ifndef RESERVOIR_QX_FK_LEFT_BATCH_H
#define RESERVOIR_QX_FK_LEFT_BATCH_H

#include <vector>

template<typename T>
class Reservoir;
struct QxRow;
struct QxFkLeftRow;
struct QxFkRightRow;

class QxFkLeftBatch {
public:
    void init(QxFkLeftRow* left_row, std::vector<QxFkRightRow*>* right_rows);
    void fill(Reservoir<QxRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;

    QxFkLeftRow* left_row = nullptr;
    std::vector<QxFkRightRow*>* right_rows = nullptr;

    void fill_with_skip(Reservoir<QxRow>* reservoir);
};


#endif
