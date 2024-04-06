#include "../../include/reservoir.h"
#include "../../include/qy/qy_fk_left_batch.h"
#include "../../include/qy/qy_rows.h"

void QyFkLeftBatch::init(QyFkLeftRow *row, std::vector<QyFkRightRow*>* rows) {
    this->left_row = row;
    this->right_rows = rows;
    this->position = 0;
    this->size = rows->size();
}

void QyFkLeftBatch::fill(Reservoir<QyRow> *reservoir) {
    if (!reservoir->is_full()) {
        while (!reservoir->is_full() && position < size) {
            auto right_row = right_rows->at(position);
            reservoir->insert(new QyRow(left_row->store_sales_row, left_row->customer_row, left_row->household_demographics_row,
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

void QyFkLeftBatch::fill_with_skip(Reservoir<QyRow> *reservoir) {
    while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += reservoir->get_t();
            auto right_row = right_rows->at(position);
            reservoir->replace(new QyRow(left_row->store_sales_row, left_row->customer_row, left_row->household_demographics_row,
                                         right_row->household_demographics_row2, right_row->customer_row2));
            position += 1;
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}