#include "../../include/reservoir.h"
#include "../../include/qz/qz_fk_middle_batch.h"
#include "../../include/qz/qz_rows.h"

void QzFkMiddleBatch::init(std::vector<ItemRow*>* left, QzFkMiddleRow* middle, std::vector<QzFkRightRow*>* right) {
    this->item_rows = left;
    this->middle_row = middle;
    this->right_rows = right;
    this->size = left->size() * right->size();
    this->position = 0;
    this->right_size = right->size();
}

void QzFkMiddleBatch::fill(Reservoir<QzRow> *reservoir) {
    if (!reservoir->is_full()) {
        while (!reservoir->is_full() && position < size) {
            unsigned long left_index = position / right_size;
            unsigned long right_index = position % right_size;
            auto item_row = item_rows->at(left_index);
            auto right_row = right_rows->at(right_index);
            reservoir->insert(new QzRow(item_row,
                                        middle_row->item_row2, middle_row->store_sales_row,
                                        middle_row->customer_row, middle_row->household_demographics_row,
                                        right_row->household_demographics_row2, right_row->customer_row2));
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

void QzFkMiddleBatch::fill_with_skip(Reservoir<QzRow> *reservoir) {
    while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += reservoir->get_t();
            unsigned long left_index = position / right_size;
            unsigned long right_index = position % right_size;
            auto item_row = item_rows->at(left_index);
            auto right_row = right_rows->at(right_index);
            reservoir->replace(new QzRow(item_row,
                                        middle_row->item_row2, middle_row->store_sales_row,
                                        middle_row->customer_row, middle_row->household_demographics_row,
                                        right_row->household_demographics_row2, right_row->customer_row2));
            position += 1;
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}