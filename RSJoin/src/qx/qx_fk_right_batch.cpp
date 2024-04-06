#include "../../include/reservoir.h"
#include "../../include/qx/qx_fk_right_batch.h"
#include "../../include/qx/qx_rows.h"

void QxFkRightBatch::init(std::vector<QxFkLeftRow*>* rows, QxFkRightRow* row) {
    this->left_rows = rows;
    this->right_row = row;
    this->position = 0;
    this->size = rows->size();
}

void QxFkRightBatch::fill(Reservoir<QxRow> *reservoir) {
    if (!reservoir->is_full()) {
        while (!reservoir->is_full() && position < size) {
            auto left_row = left_rows->at(position);
            reservoir->insert(new QxRow(left_row->date_dim_row, left_row->store_sales_row, left_row->store_returns_row,
                                        right_row->catalog_sales_row, right_row->date_dim_row2));
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

void QxFkRightBatch::fill_with_skip(Reservoir<QxRow> *reservoir) {
    while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += reservoir->get_t();
            auto left_row = left_rows->at(position);
            reservoir->replace(new QxRow(left_row->date_dim_row, left_row->store_sales_row, left_row->store_returns_row,
                                         right_row->catalog_sales_row, right_row->date_dim_row2));
            position += 1;
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}