#ifndef RESERVOIR_QZ_FK_GROUP_LEFT_BATCH_H
#define RESERVOIR_QZ_FK_GROUP_LEFT_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct QzRow;
struct ItemRow;
struct QzFkMiddleRow;
struct QzFkGroupMiddleRow;
struct QzFkRightRow;
class QzFkGroupAttributeItemMiddle;
class QzFkGroupAttributeMiddleRight;

class QzFkGroupLeftBatch {
public:
    QzFkGroupLeftBatch(QzFkGroupAttributeItemMiddle* qz_fk_group_attribute_item_middle,
                       QzFkGroupAttributeMiddleRight* qz_fk_group_attribute_middle_right);

    void clear();
    void set_item_row(ItemRow* row);
    void fill(Reservoir<QzRow>* reservoir);

private:
    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;

    QzFkGroupAttributeItemMiddle* qz_fk_group_attribute_item_middle = nullptr;
    QzFkGroupAttributeMiddleRight* qz_fk_group_attribute_middle_right = nullptr;
    QzRow* next_row = nullptr;

    ItemRow* item_row = nullptr;
    QzFkMiddleRow* middle_row = nullptr;
    QzFkGroupMiddleRow* group_middle_row = nullptr;
    QzFkRightRow* right_row = nullptr;

    unsigned long middle_layer_unit_size;
    unsigned long middle_layer_index;
    unsigned long middle_group_index;
    unsigned long right_layer_index;

    std::unordered_map<unsigned long, std::vector<QzFkGroupMiddleRow*>*>* middle_layer = nullptr;
    std::vector<QzFkRightRow*>* right_layer = nullptr;

    void init_real_iterator();
    bool has_next_real();
    QzRow* next_real();
    void compute_next_real();

    void fill_with_skip(Reservoir<QzRow>* reservoir);
};


#endif
