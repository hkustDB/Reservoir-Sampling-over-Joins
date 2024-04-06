#include "../../include/reservoir.h"
#include "../../include/qz/qz_fk_right_batch.h"
#include "../../include/qz/qz_fk_attribute_item_middle.h"
#include "../../include/qz/qz_fk_attribute_middle_right.h"
#include "../../include/qz/qz_rows.h"

QzFkRightBatch::QzFkRightBatch(QzFkAttributeItemMiddle *qz_fk_attribute_item_middle,
                               QzFkAttributeMiddleRight *qz_fk_attribute_middle_right):
        qz_fk_attribute_item_middle(qz_fk_attribute_item_middle),
        qz_fk_attribute_middle_right(qz_fk_attribute_middle_right) {
    clear();
}

void QzFkRightBatch::clear() {
    position = 0;
    position_after_next_real = 0;
    middle_layer_unit_size = 1;
    item_layer_index = 0;
    middle_layer_index = 0;
    next_row = nullptr;
}

void QzFkRightBatch::set_right_row(QzFkRightRow *row) {
    clear();
    right_row = row;
    size = qz_fk_attribute_middle_right->get_lcnt(row->household_demographics_row2->hd_income_band_sk);
    if (size > 0) {
        middle_layer = qz_fk_attribute_middle_right->get_left_layer(row->household_demographics_row2->hd_income_band_sk);
    }
}

void QzFkRightBatch::fill(Reservoir<QzRow> *reservoir) {
    if (size > 0) {
        if (!reservoir->is_full()) {
            init_real_iterator();

            while (!reservoir->is_full() && has_next_real()) {
                reservoir->insert(next_real());
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
}

void QzFkRightBatch::init_real_iterator() {
    while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
        middle_layer_unit_size *= 2;
    }
    middle_layer_index = 0;
    middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);

    item_layer = qz_fk_attribute_item_middle->get_left_layer(middle_row->item_row2->i_category_id);
    item_layer_index = 0;
    item_row = item_layer->at(item_layer_index);

    next_row = new QzRow(item_row,
                         middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                         right_row->household_demographics_row2, right_row->customer_row2);
    position_after_next_real = 1;
}

bool QzFkRightBatch::has_next_real() {
    return next_row != nullptr;
}

QzRow* QzFkRightBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void QzFkRightBatch::compute_next_real() {
    position_after_next_real = position + 1;
    if (item_layer_index < item_layer->size() - 1) {
        item_layer_index += 1;
        item_row = item_layer->at(item_layer_index);
        next_row = new QzRow(item_row,
                             middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                             right_row->household_demographics_row2, right_row->customer_row2);
        return;
    } else {
        position_after_next_real += (qz_fk_attribute_item_middle->get_wlcnt(middle_row->item_row2->i_category_id)
                                     - qz_fk_attribute_item_middle->get_lcnt(middle_row->item_row2->i_category_id));
    }

    if (middle_layer_index < middle_layer->at(middle_layer_unit_size)->size() - 1) {
        middle_layer_index += 1;
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        item_layer = qz_fk_attribute_item_middle->get_left_layer(middle_row->item_row2->i_category_id);
        item_layer_index = 0;
        item_row = item_layer->at(item_layer_index);
        next_row = new QzRow(item_row,
                             middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                             right_row->household_demographics_row2, right_row->customer_row2);
        return;
    } else if (middle_layer_unit_size < qz_fk_attribute_middle_right->get_left_layer_max_unit_size(right_row->household_demographics_row2->hd_income_band_sk)) {
        middle_layer_unit_size *= 2;
        while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
            middle_layer_unit_size *= 2;
        }
        middle_layer_index = 0;
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        item_layer = qz_fk_attribute_item_middle->get_left_layer(middle_row->item_row2->i_category_id);
        item_layer_index = 0;
        item_row = item_layer->at(item_layer_index);
        next_row = new QzRow(item_row,
                             middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                             right_row->household_demographics_row2, right_row->customer_row2);
        return;
    } else {
        position_after_next_real = size;
        next_row = nullptr;
        return;
    }
}

void QzFkRightBatch::fill_with_skip(Reservoir<QzRow> *reservoir) {
    LOOP:while (true) {
    if (size - position <= reservoir->get_t()) {
        reservoir->t_subtract(size - position);
        position = size;
        break;
    } else {
        position += (reservoir->get_t() + 1);
        unsigned long offset = position - 1;

        middle_layer_unit_size = 1;
        while (middle_layer_unit_size <= qz_fk_attribute_middle_right->get_left_layer_max_unit_size(right_row->household_demographics_row2->hd_income_band_sk)) {
            if (middle_layer->find(middle_layer_unit_size) != middle_layer->end()) {
                if (offset < middle_layer->at(middle_layer_unit_size)->size() * middle_layer_unit_size) {
                    middle_layer_index = offset / (middle_layer_unit_size);
                    offset -= middle_layer_index * middle_layer_unit_size;
                    break;
                } else {
                    offset -= middle_layer->at(middle_layer_unit_size)->size() * middle_layer_unit_size;
                }
            }
            middle_layer_unit_size *= 2;
        }
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);

        if (offset >= qz_fk_attribute_item_middle->get_lcnt(middle_row->item_row2->i_category_id)) {
            reservoir->update_t();
            // we get a dummy tuple, update reservoir.t and try next pick
            goto LOOP;
        }

        item_layer = qz_fk_attribute_item_middle->get_left_layer(middle_row->item_row2->i_category_id);
        item_layer_index = offset;
        offset = 0;
        item_row = item_layer->at(item_layer_index);

        reservoir->replace(new QzRow(item_row,
                                     middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                                     right_row->household_demographics_row2, right_row->customer_row2));
        reservoir->update_w();
        reservoir->update_t();
    }
}
}