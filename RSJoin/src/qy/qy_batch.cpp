#include "../../include/reservoir.h"
#include "../../include/qy/qy_attribute_store_sales_customer.h"
#include "../../include/qy/qy_attribute_customer_household_demographics.h"
#include "../../include/qy/qy_attribute_household_demographics_household_demographics.h"
#include "../../include/qy/qy_attribute_household_demographics_customer.h"
#include "../../include/qy/qy_batch.h"
#include "../../include/qy/qy_rows.h"

QyBatch::QyBatch(QyAttributeStoreSalesCustomer *qy_attribute_store_sales_customer,
                 QyAttributeCustomerHouseholdDemographics *qy_attribute_customer_household_demographics,
                 QyAttributeHouseholdDemographicsHouseholdDemographics *qy_attribute_household_demographics_household_demographics,
                 QyAttributeHouseholdDemographicsCustomer *qy_attribute_household_demographics_customer):
                 qy_attribute_store_sales_customer(qy_attribute_store_sales_customer),
                 qy_attribute_customer_household_demographics(qy_attribute_customer_household_demographics),
                 qy_attribute_household_demographics_household_demographics(qy_attribute_household_demographics_household_demographics),
                 qy_attribute_household_demographics_customer(qy_attribute_household_demographics_customer) {
    layer_unit_sizes = new unsigned long[5]{};
    layer_indices = new unsigned long[5]{};
}

QyBatch::~QyBatch() {
    delete[] layer_unit_sizes;
    delete[] layer_indices;
}

void QyBatch::clear() {
    position = 0;
    position_after_next_real = 0;
    for (int i=0; i<5; i++) {
        layer_unit_sizes[i] = 1;
        layer_indices[i] = 0;
    }
}

void QyBatch::set_store_sales_row(StoreSalesRow *row) {
    clear();
    store_sales_row = row;
    rid = 0;
    size = get_rcnt(0);
}

void QyBatch::set_customer_row(CustomerRow *row) {
    clear();
    customer_row = row;
    rid = 1;
    size = get_lcnt(0) * get_rcnt(1);
}

void QyBatch::set_household_demographics_row(HouseholdDemographicsRow *row) {
    clear();
    household_demographics_row = row;
    rid = 2;
    size = get_lcnt(1) * get_rcnt(2);
}

void QyBatch::set_household_demographics_row2(HouseholdDemographicsRow *row) {
    clear();
    household_demographics_row2 = row;
    rid = 3;
    size = get_lcnt(2) * get_rcnt(3);
}

void QyBatch::set_customer_row2(CustomerRow *row) {
    clear();
    customer_row2 = row;
    rid = 4;
    size = get_lcnt(3);
}

void QyBatch::fill(Reservoir<QyRow> *reservoir) {
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

void QyBatch::init_real_iterator() {
    for (int i=rid-1; i>=0; i--) {
        update_layer(i, false);
        search_layer_unit_size(i);
        reset_layer_index(i);
        update_row(i);
    }

    for (int i=rid+1; i<5; i++) {
        update_layer(i, true);
        search_layer_unit_size(i);
        reset_layer_index(i);
        update_row(i);
    }

    next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
    position_after_next_real = 1;
}

bool QyBatch::has_next_real() {
    return next_row != nullptr;
}

QyRow *QyBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void QyBatch::compute_next_real() {
    position_after_next_real = position + 1;
    for (int i=4; i>rid; i--) {
        if (i == 4) {
            if (layer_indices[i] < get_layer_size(i) - 1) {
                layer_indices[i] += 1;
                update_row(i);
                next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
                return;
            } else {
                if (i-1 > rid) {
                    position_after_next_real += (get_wrcnt(i-1) - get_rcnt(i-1));
                }
            }
        } else {
            if (layer_indices[i] < get_layer_size(i) - 1) {
                layer_indices[i] += 1;
                update_row(i);
                for (int j=i+1; j<5; j++) {
                    update_layer(j, true);
                    if (j < 4) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
                return;
            } else if (layer_unit_sizes[i] < get_right_layer_max_unit_size(i-1)) {
                next_layer_unit_size(i);
                reset_layer_index(i);
                update_row(i);
                for (int j=i+1; j<5; j++) {
                    update_layer(j, true);
                    if (j < 4) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
                return;
            } else {
                if (i-1 > rid) {
                    position_after_next_real += (get_wrcnt(i-1) - get_rcnt(i-1));
                }
            }
        }
    }

    // run to here means the right hand side has exhausted

    for (int i=0; i<rid; i++) {
        if (i == 0) {
            if (layer_indices[i] < get_layer_size(i) - 1) {
                layer_indices[i] += 1;
                update_row(i);
                for (int j=rid+1; j<5; j++) {
                    update_layer(j, true);
                    if (j < 4) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
                return;
            } else if (i+1 < rid) {
                if (rid < 4) {
                    position_after_next_real += ((get_wlcnt(i) - get_lcnt(i)) * get_rcnt(rid));
                } else {
                    position_after_next_real += (get_wlcnt(i) - get_lcnt(i));
                }
            }
        } else {
            if (layer_indices[i] < get_layer_size(i) - 1) {
                layer_indices[i] += 1;
                update_row(i);
                for (int j=i-1; j>=0; j--) {
                    update_layer(j, false);
                    if (j > 0) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                for (int j=rid+1; j<5; j++) {
                    update_layer(j, true);
                    if (j > 0) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
                return;
            } else if (layer_unit_sizes[i] < get_left_layer_max_unit_size(i)) {
                next_layer_unit_size(i);
                reset_layer_index(i);
                update_row(i);
                for (int j=i-1; j>=0; j--) {
                    update_layer(j, false);
                    if (j > 0) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                for (int j=rid+1; j<5; j++) {
                    update_layer(j, true);
                    if (j > 0) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2);
                return;
            } else if (i+1 < rid) {
                if (rid < 4) {
                    position_after_next_real += ((get_wlcnt(i) - get_lcnt(i)) * get_rcnt(rid));
                } else {
                    position_after_next_real += (get_wlcnt(i) - get_lcnt(i));
                }
            }
        }
    }

    // run to here means the left hand side has exhausted
    // no more real tuples.
    position_after_next_real = size;
    next_row = nullptr;
}

void QyBatch::search_layer_unit_size(int i) {
    if (i == 1) {
        while (customer_layer->find(layer_unit_sizes[i]) == customer_layer->end()) {
            layer_unit_sizes[i] *= 2;
        }
    } else if (i == 2) {
        while (household_demographics_layer->find(layer_unit_sizes[i]) == household_demographics_layer->end()) {
            layer_unit_sizes[i] *= 2;
        }
    } else if (i == 3) {
        while (household_demographics_layer2->find(layer_unit_sizes[i]) == household_demographics_layer2->end()) {
            layer_unit_sizes[i] *= 2;
        }
    }
}

void QyBatch::reset_layer_unit_size(int i) {
    layer_unit_sizes[i] = 1;
}

void QyBatch::next_layer_unit_size(int i) {
    layer_unit_sizes[i] *= 2;
    search_layer_unit_size(i);
}

void QyBatch::reset_layer_index(int i) {
    layer_indices[i] = 0;
}

void QyBatch::update_row(int i) {
    switch (i) {
        case 0: {
            store_sales_row = store_sales_layer->at(1)->at(layer_indices[i]);
            break;
        }
        case 1: {
            customer_row = customer_layer->at(layer_unit_sizes[i])->at(layer_indices[i]);
            break;
        }
        case 2: {
            household_demographics_row = household_demographics_layer->at(layer_unit_sizes[i])->at(layer_indices[i]);
            break;
        }
        case 3: {
            household_demographics_row2 = household_demographics_layer2->at(layer_unit_sizes[i])->at(layer_indices[i]);
            break;
        }
        case 4: {
            customer_row2 = customer_layer2->at(1)->at(layer_indices[i]);
            break;
        }
    }
}

void QyBatch::update_layer(int i, bool from_left) {
    switch (i) {
        case 0: {
            if (!from_left)
                store_sales_layer = qy_attribute_store_sales_customer->get_left_layer(customer_row->c_customer_sk);
            break;
        }
        case 1: {
            if (from_left)
                customer_layer = qy_attribute_store_sales_customer->get_right_layer(store_sales_row->ss_customer_sk);
            else
                customer_layer = qy_attribute_customer_household_demographics->get_left_layer(household_demographics_row->hd_demo_sk);
            break;
        }
        case 2: {
            if (from_left)
                household_demographics_layer = qy_attribute_customer_household_demographics->get_right_layer(customer_row->c_current_hdemo_sk);
            else
                household_demographics_layer = qy_attribute_household_demographics_household_demographics->get_left_layer(household_demographics_row2->hd_income_band_sk);
            break;
        }
        case 3: {
            if (from_left)
                household_demographics_layer2 = qy_attribute_household_demographics_household_demographics->get_right_layer(household_demographics_row->hd_income_band_sk);
            else
                household_demographics_layer2 = qy_attribute_household_demographics_customer->get_left_layer(customer_row2->c_current_hdemo_sk);
            break;
        }
        case 4: {
            if (from_left)
                customer_layer2 = qy_attribute_household_demographics_customer->get_right_layer(household_demographics_row2->hd_demo_sk);
            break;
        }
    }
}

void QyBatch::fill_with_skip(Reservoir<QyRow> *reservoir) {
    LOOP:while (true) {
    if (size - position <= reservoir->get_t()) {
        reservoir->t_subtract(size - position);
        position = size;
        break;
    } else {
        // e.g., current position = 0, reservoir.t = 0,
        // we want the element at position 0, then position should be set to 1
        position += (reservoir->get_t() + 1);
        // the element we want is at (position - 1)
        unsigned long offset = position - 1;

        unsigned long rcnt;
        if (rid == 0 || rid == 4) {
            rcnt = 1;
        } else {
            rcnt = get_rcnt(rid);
        }

        for (int i=rid-1; i>=0; i--) {
            if (i == 0) {
                update_layer(i, false);
                layer_indices[i] = offset / rcnt;
                offset -= layer_indices[i] * rcnt;
                update_row(i);
            } else {
                update_layer(i, false);
                reset_layer_unit_size(i);
                while (layer_unit_sizes[i] <= get_left_layer_max_unit_size(i)) {
                    if (layer_nonempty(i)) {
                        if (offset < get_layer_size(i) * layer_unit_sizes[i] * rcnt) {
                            layer_indices[i] = offset / (layer_unit_sizes[i] * rcnt);
                            offset -= layer_indices[i] * layer_unit_sizes[i] * rcnt;
                            break;
                        } else {
                            offset -= get_layer_size(i) * layer_unit_sizes[i] * rcnt;
                        }
                    }
                    layer_unit_sizes[i] *= 2;
                }
                update_row(i);

                if (offset >= get_lcnt(i-1) * rcnt) {
                    reservoir->update_t();
                    // we get a dummy tuple, update reservoir.t and try next pick
                    goto LOOP;
                }
            }
        }

        // run to here means we have determined the rows on the left hand side of sid

        for (int i=rid+1; i<5; i++) {
            if (i == 4) {
                update_layer(i, true);
                layer_indices[i] = offset;
                offset = 0;
                update_row(i);
            } else {
                update_layer(i, true);
                reset_layer_unit_size(i);
                while (layer_unit_sizes[i] <= get_right_layer_max_unit_size(i-1)) {
                    if (layer_nonempty(i)) {
                        if (offset < get_layer_size(i) * layer_unit_sizes[i]) {
                            layer_indices[i] = offset / (layer_unit_sizes[i]);
                            offset -= layer_indices[i] * layer_unit_sizes[i];
                            break;
                        } else {
                            offset -= get_layer_size(i) * layer_unit_sizes[i];
                        }
                    }
                    layer_unit_sizes[i] *= 2;
                }
                update_row(i);

                if (offset >= get_rcnt(i)) {
                    reservoir->update_t();
                    // we get a dummy tuple, update reservoir.t and try next pick
                    goto LOOP;
                }
            }
        }

        // now we have all the rows, return a join result
        reservoir->replace(new QyRow(store_sales_row, customer_row, household_demographics_row, household_demographics_row2, customer_row2));
        reservoir->update_w();
        reservoir->update_t();
    }
}
}

unsigned long QyBatch::get_layer_size(int i) {
    switch (i) {
        case 0: {
            return store_sales_layer->at(1)->size();
        }
        case 1: {
            return customer_layer->at(layer_unit_sizes[i])->size();
        }
        case 2: {
            return household_demographics_layer->at(layer_unit_sizes[i])->size();
        }
        case 3: {
            return household_demographics_layer2->at(layer_unit_sizes[i])->size();
        }
        case 4: {
            return customer_layer2->at(1)->size();
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QyBatch::get_lcnt(int i) {
    switch (i) {
        case 0: {
            return qy_attribute_store_sales_customer->get_lcnt(customer_row->c_customer_sk);
        }
        case 1: {
            return qy_attribute_customer_household_demographics->get_lcnt(household_demographics_row->hd_demo_sk);
        }
        case 2: {
            return qy_attribute_household_demographics_household_demographics->get_lcnt(household_demographics_row2->hd_income_band_sk);
        }
        case 3: {
            return qy_attribute_household_demographics_customer->get_lcnt(customer_row2->c_current_hdemo_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QyBatch::get_wlcnt(int i) {
    switch (i) {
        case 0: {
            return qy_attribute_store_sales_customer->get_wlcnt(customer_row->c_customer_sk);
        }
        case 1: {
            return qy_attribute_customer_household_demographics->get_wlcnt(household_demographics_row->hd_demo_sk);
        }
        case 2: {
            return qy_attribute_household_demographics_household_demographics->get_wlcnt(household_demographics_row2->hd_income_band_sk);
        }
        case 3: {
            return qy_attribute_household_demographics_customer->get_wlcnt(customer_row2->c_current_hdemo_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QyBatch::get_rcnt(int i) {
    switch (i) {
        case 0: {
            return qy_attribute_store_sales_customer->get_rcnt(store_sales_row->ss_customer_sk);
        }
        case 1: {
            return qy_attribute_customer_household_demographics->get_rcnt(customer_row->c_current_hdemo_sk);
        }
        case 2: {
            return qy_attribute_household_demographics_household_demographics->get_rcnt(household_demographics_row->hd_income_band_sk);
        }
        case 3: {
            return qy_attribute_household_demographics_customer->get_rcnt(household_demographics_row2->hd_demo_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QyBatch::get_wrcnt(int i) {
    switch (i) {
        case 0: {
            return qy_attribute_store_sales_customer->get_wrcnt(store_sales_row->ss_customer_sk);
        }
        case 1: {
            return qy_attribute_customer_household_demographics->get_wrcnt(customer_row->c_current_hdemo_sk);
        }
        case 2: {
            return qy_attribute_household_demographics_household_demographics->get_wrcnt(household_demographics_row->hd_income_band_sk);
        }
        case 3: {
            return qy_attribute_household_demographics_customer->get_wrcnt(household_demographics_row2->hd_demo_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QyBatch::get_left_layer_max_unit_size(int i) {
    switch (i) {
        case 0: {
            return qy_attribute_store_sales_customer->get_left_layer_max_unit_size(customer_row->c_customer_sk);
        }
        case 1: {
            return qy_attribute_customer_household_demographics->get_left_layer_max_unit_size(household_demographics_row->hd_demo_sk);
        }
        case 2: {
            return qy_attribute_household_demographics_household_demographics->get_left_layer_max_unit_size(household_demographics_row2->hd_income_band_sk);
        }
        case 3: {
            return qy_attribute_household_demographics_customer->get_left_layer_max_unit_size(customer_row2->c_current_hdemo_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QyBatch::get_right_layer_max_unit_size(int i) {
    switch (i) {
        case 0: {
            return qy_attribute_store_sales_customer->get_right_layer_max_unit_size(store_sales_row->ss_customer_sk);
        }
        case 1: {
            return qy_attribute_customer_household_demographics->get_right_layer_max_unit_size(customer_row->c_current_hdemo_sk);
        }
        case 2: {
            return qy_attribute_household_demographics_household_demographics->get_right_layer_max_unit_size(household_demographics_row->hd_income_band_sk);
        }
        case 3: {
            return qy_attribute_household_demographics_customer->get_right_layer_max_unit_size(household_demographics_row2->hd_demo_sk);
        }
        default: {
            exit(1);
        }
    }
}

bool QyBatch::layer_nonempty(int i) {
    switch (i) {
        case 0: {
            return true;
        }
        case 1: {
            return customer_layer->find(layer_unit_sizes[i]) != customer_layer->end();
        }
        case 2: {
            return household_demographics_layer->find(layer_unit_sizes[i]) != household_demographics_layer->end();
        }
        case 3: {
            return household_demographics_layer2->find(layer_unit_sizes[i]) != household_demographics_layer2->end();
        }
        case 4: {
            return true;
        }
        default: {
            exit(1);
        }
    }
}