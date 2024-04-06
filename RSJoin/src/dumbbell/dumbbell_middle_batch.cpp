#include "../../include/reservoir.h"
#include "../../include/dumbbell/dumbbell_middle_batch.h"
#include "../../include/dumbbell/dumbbell_rows.h"

void DumbbellMiddleBatch::init(std::vector<TriangleRow*>* left, std::vector<TriangleRow*>* right) {
    this->left_rows = left;
    this->right_rows = right;
    this->size = left->size() * right->size();
    this->position = 0;
    this->right_size = right->size();
}

void DumbbellMiddleBatch::fill(Reservoir<DumbbellRow> *reservoir) {
    if (!reservoir->is_full()) {
        while (!reservoir->is_full() && position < size) {
            unsigned long left_index = position / right_size;
            unsigned long right_index = position % right_size;
            auto left_row = left_rows->at(left_index);
            auto right_row = right_rows->at(right_index);
            reservoir->insert(new DumbbellRow(left_row->a, left_row->b, left_row->c,
                                              right_row->a, right_row->b, right_row->c));
            position += 1;
        }

        if (reservoir->is_full()) {
            reservoir->update_w();
            reservoir->update_t();
        }
    }

    if (reservoir->is_full()) {
        fill_with_skip(reservoir);
    }
}

void DumbbellMiddleBatch::fill_with_skip(Reservoir<DumbbellRow> *reservoir) {
    while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += reservoir->get_t();
            unsigned long left_index = position / right_size;
            unsigned long right_index = position % right_size;
            auto left_row = left_rows->at(left_index);
            auto right_row = right_rows->at(right_index);
            reservoir->replace(new DumbbellRow(left_row->a, left_row->b, left_row->c,
                                               right_row->a, right_row->b, right_row->c));
            position += 1;
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}