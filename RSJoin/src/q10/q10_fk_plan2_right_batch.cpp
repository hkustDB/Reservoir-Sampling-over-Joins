#include "reservoir.h"
#include "q10/q10_fk_plan2_right_batch.h"
#include "q10/q10_fk_plan2_attribute_left_middle.h"
#include "q10/q10_fk_plan2_attribute_middle_right.h"
#include "q10/q10_rows.h"

Q10FkPlan2RightBatch::Q10FkPlan2RightBatch(Q10FkPlan2AttributeLeftMiddle* q10_fk_plan2_attribute_left_middle,
                                           Q10FkPlan2AttributeMiddleRight* q10_fk_plan2_attribute_middle_right):
        q10_fk_plan2_attribute_left_middle(q10_fk_plan2_attribute_left_middle),
        q10_fk_plan2_attribute_middle_right(q10_fk_plan2_attribute_middle_right) {
    clear();
}

void Q10FkPlan2RightBatch::clear() {
    position = 0;
    position_after_next_real = 0;
    middle_layer_unit_size = 1;
    left_layer_index = 0;
    middle_layer_index = 0;
    next_row = nullptr;
}

void Q10FkPlan2RightBatch::set_right_row(Q10FkPlan2RightRow* row) {
    clear();
    right_row = row;
    size = q10_fk_plan2_attribute_middle_right->get_lcnt(row->message_has_tag_row2->message_id);
    if (size > 0) {
        middle_layer = q10_fk_plan2_attribute_middle_right->get_left_layer(row->message_has_tag_row2->message_id);
    }
}

void Q10FkPlan2RightBatch::fill(Reservoir<Q10FkPlan2Row> *reservoir) {
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

void Q10FkPlan2RightBatch::init_real_iterator() {
    while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
        middle_layer_unit_size *= 2;
    }
    middle_layer_index = 0;
    middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);

    left_layer = q10_fk_plan2_attribute_left_middle->get_left_layer(middle_row->message_row->creator_person_id);
    left_layer_index = 0;
    left_row = left_layer->at(left_layer_index);

    next_row = new Q10FkPlan2Row(left_row, middle_row, right_row);
    position_after_next_real = 1;
}

bool Q10FkPlan2RightBatch::has_next_real() {
    return next_row != nullptr;
}

Q10FkPlan2Row* Q10FkPlan2RightBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void Q10FkPlan2RightBatch::compute_next_real() {
    position_after_next_real = position + 1;
    if (left_layer_index < left_layer->size() - 1) {
        left_layer_index += 1;
        left_row = left_layer->at(left_layer_index);
        next_row = new Q10FkPlan2Row(left_row, middle_row, right_row);
        return;
    } else {
        position_after_next_real += (q10_fk_plan2_attribute_left_middle->get_wlcnt(middle_row->message_row->creator_person_id)
                                     - q10_fk_plan2_attribute_left_middle->get_lcnt(middle_row->message_row->creator_person_id));
    }

    if (middle_layer_index < middle_layer->at(middle_layer_unit_size)->size() - 1) {
        middle_layer_index += 1;
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        left_layer = q10_fk_plan2_attribute_left_middle->get_left_layer(middle_row->message_row->creator_person_id);
        left_layer_index = 0;
        left_row = left_layer->at(left_layer_index);
        next_row = new Q10FkPlan2Row(left_row, middle_row, right_row);
        return;
    } else if (middle_layer_unit_size < q10_fk_plan2_attribute_middle_right->get_left_layer_max_unit_size(right_row->message_has_tag_row2->message_id)) {
        middle_layer_unit_size *= 2;
        while (middle_layer->find(middle_layer_unit_size) == middle_layer->end()) {
            middle_layer_unit_size *= 2;
        }
        middle_layer_index = 0;
        middle_row = middle_layer->at(middle_layer_unit_size)->at(middle_layer_index);
        left_layer = q10_fk_plan2_attribute_left_middle->get_left_layer(middle_row->message_row->creator_person_id);
        left_layer_index = 0;
        left_row = left_layer->at(left_layer_index);
        next_row = new Q10FkPlan2Row(left_row, middle_row, right_row);
        return;
    } else {
        position_after_next_real = size;
        next_row = nullptr;
        return;
    }
}

void Q10FkPlan2RightBatch::fill_with_skip(Reservoir<Q10FkPlan2Row> *reservoir) {
    LOOP:while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += (reservoir->get_t() + 1);
            unsigned long offset = position - 1;

            middle_layer_unit_size = 1;
            while (middle_layer_unit_size <= q10_fk_plan2_attribute_middle_right->get_left_layer_max_unit_size(right_row->message_has_tag_row2->message_id)) {
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

            if (offset >= q10_fk_plan2_attribute_left_middle->get_lcnt(middle_row->message_row->creator_person_id)) {
                reservoir->update_t();
                // we get a dummy tuple, update reservoir.t and try next pick
                goto LOOP;
            }

            left_layer = q10_fk_plan2_attribute_left_middle->get_left_layer(middle_row->message_row->creator_person_id);
            left_layer_index = offset;
            offset = 0;
            left_row = left_layer->at(left_layer_index);

            reservoir->replace(new Q10FkPlan2Row(left_row, middle_row, right_row));
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}