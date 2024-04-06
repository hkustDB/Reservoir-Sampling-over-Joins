#include "../../include/reservoir.h"
#include "../../include/qz/qz_fk_group_left_batch.h"
#include "../../include/qz/qz_fk_group_attribute_item_middle.h"
#include "../../include/qz/qz_fk_group_attribute_middle_right.h"
#include "../../include/qz/qz_rows.h"

QzFkGroupLeftBatch::QzFkGroupLeftBatch(QzFkGroupAttributeItemMiddle *qz_fk_group_attribute_item_middle,
                                       QzFkGroupAttributeMiddleRight *qz_fk_group_attribute_middle_right):
                                       qz_fk_group_attribute_item_middle(qz_fk_group_attribute_item_middle),
                                       qz_fk_group_attribute_middle_right(qz_fk_group_attribute_middle_right) {
    clear();
}

void QzFkGroupLeftBatch::clear() {
    position = 0;
    position_after_next_real = 0;
    middle_layer_unit_size = 1;
    middle_layer_index = 0;
    middle_group_index = 0;
    right_layer_index = 0;
    next_row = nullptr;
}

void QzFkGroupLeftBatch::set_item_row(ItemRow *row) {
    clear();
    item_row = row;
    size = qz_fk_group_attribute_item_middle->get_rcnt(row->i_category_id);
    if (size > 0) {
        middle_layer = qz_fk_group_attribute_item_middle->get_right_layer(item_row->i_category_id);
    }
}

void QzFkGroupLeftBatch::fill(Reservoir<QzRow> *reservoir) {
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

void QzFkGroupLeftBatch::init_real_iterator() {
    while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
        middle_layer_unit_size *= 2;
    }
    middle_layer_index = 0;
    middle_group_index = 0;
    group_middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
    middle_row = group_middle_row->group.at(middle_group_index);

    right_layer = qz_fk_group_attribute_middle_right->get_right_layer(middle_row->household_demographics_row->hd_income_band_sk);
    right_layer_index = 0;
    right_row = right_layer->at(right_layer_index);

    next_row = new QzRow(item_row,
                         middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                         right_row->household_demographics_row2, right_row->customer_row2);
    position_after_next_real = 1;
}

bool QzFkGroupLeftBatch::has_next_real() {
    return next_row != nullptr;
}

QzRow* QzFkGroupLeftBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void QzFkGroupLeftBatch::compute_next_real() {
    position_after_next_real = position + 1;
    if (right_layer_index < right_layer->size() - 1) {
        right_layer_index += 1;
        right_row = right_layer->at(right_layer_index);
        next_row = new QzRow(item_row,
                             middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                             right_row->household_demographics_row2, right_row->customer_row2);
        return;
    } else {
        position_after_next_real += (qz_fk_group_attribute_middle_right->get_wrcnt(middle_row->household_demographics_row->hd_income_band_sk)
                                - qz_fk_group_attribute_middle_right->get_rcnt(middle_row->household_demographics_row->hd_income_band_sk));
    }

    if (middle_group_index < group_middle_row->group.size() - 1) {
        middle_group_index += 1;
        middle_row = group_middle_row->group.at(middle_group_index);
        right_layer = qz_fk_group_attribute_middle_right->get_right_layer(middle_row->household_demographics_row->hd_income_band_sk);
        right_layer_index = 0;
        right_row = right_layer->at(right_layer_index);
        next_row = new QzRow(item_row,
                             middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                             right_row->household_demographics_row2, right_row->customer_row2);
        return;
    } else {
        position_after_next_real += (group_middle_row->wcnt - group_middle_row->group.size())
                * (qz_fk_group_attribute_middle_right->get_wrcnt(middle_row->household_demographics_row->hd_income_band_sk));
    }

    if (middle_layer_index < middle_layer->at(middle_layer_unit_size)->size() - 1) {
        middle_layer_index += 1;
        group_middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        middle_group_index = 0;
        middle_row = group_middle_row->group.at(middle_group_index);
        right_layer = qz_fk_group_attribute_middle_right->get_right_layer(middle_row->household_demographics_row->hd_income_band_sk);
        right_layer_index = 0;
        right_row = right_layer->at(right_layer_index);
        next_row = new QzRow(item_row,
                             middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                             right_row->household_demographics_row2, right_row->customer_row2);
        return;
    } else if (middle_layer_unit_size < qz_fk_group_attribute_item_middle->get_right_layer_max_unit_size(item_row->i_category_id)) {
        middle_layer_unit_size *= 2;
        while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
            middle_layer_unit_size *= 2;
        }
        middle_layer_index = 0;
        group_middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        middle_group_index = 0;
        middle_row = group_middle_row->group.at(middle_group_index);
        right_layer = qz_fk_group_attribute_middle_right->get_right_layer(middle_row->household_demographics_row->hd_income_band_sk);
        right_layer_index = 0;
        right_row = right_layer->at(right_layer_index);
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

void QzFkGroupLeftBatch::fill_with_skip(Reservoir<QzRow> *reservoir) {
    LOOP:while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += (reservoir->get_t() + 1);
            unsigned long offset = position - 1;

            middle_layer_unit_size = 1;
            while (middle_layer_unit_size <= qz_fk_group_attribute_item_middle->get_right_layer_max_unit_size(item_row->i_category_id)) {
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
            group_middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
            unsigned wrcnt = qz_fk_group_attribute_middle_right->get_wrcnt(group_middle_row->hd_income_band_sk);
            middle_group_index = offset / wrcnt;
            if (middle_group_index >= group_middle_row->group.size()) {
                reservoir->update_t();
                // we get a dummy tuple, update reservoir.t and try next pick
                goto LOOP;
            }

            offset = offset - middle_group_index * wrcnt;
            middle_row = group_middle_row->group.at(middle_group_index);

            if (offset >= qz_fk_group_attribute_middle_right->get_rcnt(middle_row->household_demographics_row->hd_income_band_sk)) {
                reservoir->update_t();
                // we get a dummy tuple, update reservoir.t and try next pick
                goto LOOP;
            }

            right_layer = qz_fk_group_attribute_middle_right->get_right_layer(middle_row->household_demographics_row->hd_income_band_sk);
            right_layer_index = offset;
            offset = 0;
            right_row = right_layer->at(right_layer_index);

            reservoir->replace(new QzRow(item_row,
                                         middle_row->item_row2, middle_row->store_sales_row, middle_row->customer_row, middle_row->household_demographics_row,
                                         right_row->household_demographics_row2, right_row->customer_row2));
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}