#include "reservoir.h"
#include "q10/q10_fk_plan2_middle_batch.h"
#include "q10/q10_rows.h"

void Q10FkPlan2MiddleBatch::init(std::vector<Q10FkPlan2LeftRow*>* left, Q10FkPlan2MiddleRow* middle, std::vector<Q10FkPlan2RightRow*>* right) {
    this->left_rows = left;
    this->middle_row = middle;
    this->right_rows = right;
    this->size = left->size() * right->size();
    this->position = 0;
    this->right_size = right->size();
}

void Q10FkPlan2MiddleBatch::fill(Reservoir<Q10FkPlan2Row> *reservoir) {
    if (!reservoir->is_full()) {
        while (!reservoir->is_full() && position < size) {
            unsigned long left_index = position / right_size;
            unsigned long right_index = position % right_size;
            auto left_row = left_rows->at(left_index);
            auto right_row = right_rows->at(right_index);
            reservoir->insert(new Q10FkPlan2Row(left_row, middle_row, right_row));
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

void Q10FkPlan2MiddleBatch::fill_with_skip(Reservoir<Q10FkPlan2Row> *reservoir) {
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
            reservoir->replace(new Q10FkPlan2Row(left_row, middle_row, right_row));
            position += 1;
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}