#ifndef RESERVOIR_QZ_FK_RIGHT_BATCH_H
#define RESERVOIR_QZ_FK_RIGHT_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QzRow;
struct ItemRow;
struct QzFkMiddleRow;
struct QzFkRightRow;
class QzFkAttributeItemMiddle;
class QzFkAttributeMiddleRight;

class QzFkRightBatch {
public:
    QzFkRightBatch(QzFkAttributeItemMiddle* qz_fk_attribute_item_middle, QzFkAttributeMiddleRight* qz_fk_attribute_middle_right);

    void clear();
    void set_right_row(QzFkRightRow* row);
    void fill(Reservoir<QzRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;

    QzFkAttributeItemMiddle* qz_fk_attribute_item_middle = nullptr;
    QzFkAttributeMiddleRight* qz_fk_attribute_middle_right = nullptr;
    QzRow* next_row = nullptr;

    ItemRow* item_row = nullptr;
    QzFkMiddleRow* middle_row = nullptr;
    QzFkRightRow* right_row = nullptr;

    unsigned long middle_layer_unit_size;

    unsigned long item_layer_index;
    unsigned long middle_layer_index;

    std::vector<ItemRow*>* item_layer = nullptr;
    std::unordered_map<unsigned long, std::vector<QzFkMiddleRow*>*>* middle_layer = nullptr;

    void init_real_iterator();
    bool has_next_real();
    QzRow* next_real();
    void compute_next_real();

    void fill_with_skip(Reservoir<QzRow>* reservoir);
};


#endif