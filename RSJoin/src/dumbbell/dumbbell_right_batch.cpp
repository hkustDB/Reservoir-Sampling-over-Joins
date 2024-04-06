#include "../../include/reservoir.h"
#include "../../include/dumbbell/dumbbell_right_batch.h"
#include "../../include/dumbbell/dumbbell_attribute_left_middle.h"
#include "../../include/dumbbell/dumbbell_attribute_middle_right.h"
#include "../../include/dumbbell/dumbbell_rows.h"

DumbbellRightBatch::DumbbellRightBatch(DumbbellAttributeLeftMiddle *dumbbell_attribute_left_middle,
                                       DumbbellAttributeMiddleRight *dumbbell_attribute_middle_right):
            dumbbell_attribute_left_middle(dumbbell_attribute_left_middle),
            dumbbell_attribute_middle_right(dumbbell_attribute_middle_right) {}

void DumbbellRightBatch::clear() {
    position = 0;
    position_after_next_real = 0;
    middle_layer_unit_size = 1;
    left_layer_index = 0;
    middle_layer_index = 0;
    next_row = nullptr;
}

void DumbbellRightBatch::set_right_row(TriangleRow *row) {
    clear();
    right_row = row;
    size = dumbbell_attribute_middle_right->get_lcnt(row->a);
    if (size > 0) {
        middle_layer = dumbbell_attribute_middle_right->get_left_layer(row->a);
    }
}

void DumbbellRightBatch::fill(Reservoir<DumbbellRow> *reservoir) {
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

void DumbbellRightBatch::init_real_iterator() {
    while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
        middle_layer_unit_size *= 2;
    }
    middle_layer_index = 0;
    middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);

    left_layer = dumbbell_attribute_left_middle->get_left_layer(middle_row->src);
    left_layer_index = 0;
    left_row = left_layer->at(left_layer_index);

    next_row = new DumbbellRow(left_row->a, left_row->b, left_row->c,
                               right_row->a, right_row->b, right_row->c);
    position_after_next_real = 1;
}

bool DumbbellRightBatch::has_next_real() {
    return next_row != nullptr;
}

DumbbellRow* DumbbellRightBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void DumbbellRightBatch::compute_next_real() {
    position_after_next_real = position + 1;
    if (left_layer_index < left_layer->size() - 1) {
        left_layer_index += 1;
        left_row = left_layer->at(left_layer_index);
        next_row = new DumbbellRow(left_row->a, left_row->b, left_row->c,
                                   right_row->a, right_row->b, right_row->c);
        return;
    } else {
        position_after_next_real += (dumbbell_attribute_left_middle->get_wlcnt(middle_row->src)
                                     - dumbbell_attribute_left_middle->get_lcnt(middle_row->src));
    }

    if (middle_layer_index < middle_layer->at(middle_layer_unit_size)->size() - 1) {
        middle_layer_index += 1;
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        left_layer = dumbbell_attribute_left_middle->get_left_layer(middle_row->src);
        left_layer_index = 0;
        left_row = left_layer->at(left_layer_index);
        next_row = new DumbbellRow(left_row->a, left_row->b, left_row->c,
                                   right_row->a, right_row->b, right_row->c);
        return;
    } else if (middle_layer_unit_size < dumbbell_attribute_middle_right->get_left_layer_max_unit_size(right_row->a)) {
        middle_layer_unit_size *= 2;
        while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
            middle_layer_unit_size *= 2;
        }
        middle_layer_index = 0;
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        left_layer = dumbbell_attribute_left_middle->get_left_layer(middle_row->src);
        left_layer_index = 0;
        left_row = left_layer->at(left_layer_index);
        next_row = new DumbbellRow(left_row->a, left_row->b, left_row->c,
                                   right_row->a, right_row->b, right_row->c);
        return;
    } else {
        position_after_next_real = size;
        next_row = nullptr;
        return;
    }
}

void DumbbellRightBatch::fill_with_skip(Reservoir<DumbbellRow> *reservoir) {
    LOOP:while (true) {
    if (size - position <= reservoir->get_t()) {
        reservoir->t_subtract(size - position);
        position = size;
        break;
    } else {
        position += (reservoir->get_t() + 1);
        unsigned long offset = position - 1;

        middle_layer_unit_size = 1;
        while (middle_layer_unit_size <= dumbbell_attribute_middle_right->get_left_layer_max_unit_size(right_row->a)) {
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

        if (offset >= dumbbell_attribute_left_middle->get_lcnt(middle_row->src)) {
            reservoir->update_t();
            // we get a dummy tuple, update reservoir.t and try next pick
            goto LOOP;
        }

        left_layer = dumbbell_attribute_left_middle->get_left_layer(middle_row->src);
        left_layer_index = offset;
        offset = 0;
        left_row = left_layer->at(left_layer_index);

        reservoir->replace(new DumbbellRow(left_row->a, left_row->b, left_row->c,
                                           right_row->a, right_row->b, right_row->c));
        reservoir->update_w();
        reservoir->update_t();
    }
}
}