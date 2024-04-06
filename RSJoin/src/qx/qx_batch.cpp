#include "../../include/reservoir.h"
#include "../../include/qx/qx_attribute_date_dim_store_sales.h"
#include "../../include/qx/qx_attribute_store_sales_store_returns.h"
#include "../../include/qx/qx_attribute_store_returns_catalog_sales.h"
#include "../../include/qx/qx_attribute_catalog_sales_date_dim.h"
#include "../../include/qx/qx_batch.h"
#include "../../include/qx/qx_rows.h"

QxBatch::QxBatch(QxAttributeDateDimStoreSales* qx_attribute_date_dim_store_sales,
                   QxAttributeStoreSalesStoreReturns* qx_attribute_store_sales_store_returns,
                   QxAttributeStoreReturnsCatalogSales* qx_attribute_store_returns_catalog_sales,
                   QxAttributeCatalogSalesDateDim* qx_attribute_catalog_sales_date_dim):
                   qx_attribute_date_dim_store_sales(qx_attribute_date_dim_store_sales),
                   qx_attribute_store_sales_store_returns(qx_attribute_store_sales_store_returns),
                   qx_attribute_store_returns_catalog_sales(qx_attribute_store_returns_catalog_sales),
                   qx_attribute_catalog_sales_date_dim(qx_attribute_catalog_sales_date_dim) {
    layer_unit_sizes = new unsigned long[5]{};
    layer_indices = new unsigned long[5]{};
}

QxBatch::~QxBatch() {
    delete[] layer_unit_sizes;
    delete[] layer_indices;
}

void QxBatch::clear() {
    position = 0;
    position_after_next_real = 0;
    for (int i=0; i<5; i++) {
        layer_unit_sizes[i] = 1;
        layer_indices[i] = 0;
    }
}

void QxBatch::set_date_dim_row(DateDimRow *row) {
    clear();
    date_dim_row = row;
    rid = 0;
    size = get_rcnt(0);
}

void QxBatch::set_store_sales_row(StoreSalesRow *row) {
    clear();
    store_sales_row = row;
    rid = 1;
    size = get_lcnt(0) * get_rcnt(1);
}

void QxBatch::set_store_returns_row(StoreReturnsRow *row) {
    clear();
    store_returns_row = row;
    rid = 2;
    size = get_lcnt(1) * get_rcnt(2);
}

void QxBatch::set_catalog_sales_row(CatalogSalesRow *row) {
    clear();
    catalog_sales_row = row;
    rid = 3;
    size = get_lcnt(2) * get_rcnt(3);
}

void QxBatch::set_date_dim_row2(DateDimRow *row) {
    clear();
    date_dim_row2 = row;
    rid = 4;
    size = get_lcnt(3);
}

void QxBatch::fill(Reservoir<QxRow> *reservoir) {
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

void QxBatch::init_real_iterator() {
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

    next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
    position_after_next_real = 1;
}

bool QxBatch::has_next_real() {
    return next_row != nullptr;
}

QxRow *QxBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void QxBatch::compute_next_real() {
    position_after_next_real = position + 1;
    for (int i=4; i>rid; i--) {
        if (i == 4) {
            if (layer_indices[i] < get_layer_size(i) - 1) {
                layer_indices[i] += 1;
                update_row(i);
                next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
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
                next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
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
                next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
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
                next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
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
                next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
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
                next_row = new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2);
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

void QxBatch::search_layer_unit_size(int i) {
    if (i == 1) {
        while (store_sales_layer->find(layer_unit_sizes[i]) == store_sales_layer->end()) {
            layer_unit_sizes[i] *= 2;
        }
    } else if (i == 2) {
        while (store_returns_layer->find(layer_unit_sizes[i]) == store_returns_layer->end()) {
            layer_unit_sizes[i] *= 2;
        }
    } else if (i == 3) {
        while (catalog_sales_layer->find(layer_unit_sizes[i]) == catalog_sales_layer->end()) {
            layer_unit_sizes[i] *= 2;
        }
    }
}

void QxBatch::reset_layer_unit_size(int i) {
    layer_unit_sizes[i] = 1;
}

void QxBatch::next_layer_unit_size(int i) {
    layer_unit_sizes[i] *= 2;
    search_layer_unit_size(i);
}

void QxBatch::reset_layer_index(int i) {
    layer_indices[i] = 0;
}

void QxBatch::update_row(int i) {
    switch (i) {
        case 0: {
            date_dim_row = date_dim_layer->at(1)->at(layer_indices[i]);
            break;
        }
        case 1: {
            store_sales_row = store_sales_layer->at(layer_unit_sizes[i])->at(layer_indices[i]);
            break;
        }
        case 2: {
            store_returns_row = store_returns_layer->at(layer_unit_sizes[i])->at(layer_indices[i]);
            break;
        }
        case 3: {
            catalog_sales_row = catalog_sales_layer->at(layer_unit_sizes[i])->at(layer_indices[i]);
            break;
        }
        case 4: {
            date_dim_row2 = date_dim_layer2->at(1)->at(layer_indices[i]);
            break;
        }
    }
}

void QxBatch::update_layer(int i, bool from_left) {
    switch (i) {
        case 0: {
            if (!from_left)
                date_dim_layer = qx_attribute_date_dim_store_sales->get_left_layer(store_sales_row->ss_sold_date_sk);
            break;
        }
        case 1: {
            if (from_left)
                store_sales_layer = qx_attribute_date_dim_store_sales->get_right_layer(date_dim_row->d_date_sk);
            else
                store_sales_layer = qx_attribute_store_sales_store_returns->get_left_layer(store_returns_row->sr_item_sk_with_ticket_number);
            break;
        }
        case 2: {
            if (from_left)
                store_returns_layer = qx_attribute_store_sales_store_returns->get_right_layer(store_sales_row->ss_item_sk_with_ticket_number);
            else
                store_returns_layer = qx_attribute_store_returns_catalog_sales->get_left_layer(catalog_sales_row->cs_bill_customer_sk);
            break;
        }
        case 3: {
            if (from_left)
                catalog_sales_layer = qx_attribute_store_returns_catalog_sales->get_right_layer(store_returns_row->sr_customer_sk);
            else
                catalog_sales_layer = qx_attribute_catalog_sales_date_dim->get_left_layer(date_dim_row2->d_date_sk);
            break;
        }
        case 4: {
            if (from_left)
                date_dim_layer2 = qx_attribute_catalog_sales_date_dim->get_right_layer(catalog_sales_row->cs_sold_date_sk);
            break;
        }
    }
}

void QxBatch::fill_with_skip(Reservoir<QxRow> *reservoir) {
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
            reservoir->replace(new QxRow(date_dim_row, store_sales_row, store_returns_row, catalog_sales_row, date_dim_row2));
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}

unsigned long QxBatch::get_layer_size(int i) {
    switch (i) {
        case 0: {
            return date_dim_layer->at(1)->size();
        }
        case 1: {
            return store_sales_layer->at(layer_unit_sizes[i])->size();
        }
        case 2: {
            return store_returns_layer->at(layer_unit_sizes[i])->size();
        }
        case 3: {
            return catalog_sales_layer->at(layer_unit_sizes[i])->size();
        }
        case 4: {
            return date_dim_layer2->at(1)->size();
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QxBatch::get_lcnt(int i) {
    switch (i) {
        case 0: {
            return qx_attribute_date_dim_store_sales->get_lcnt(store_sales_row->ss_sold_date_sk);
        }
        case 1: {
            return qx_attribute_store_sales_store_returns->get_lcnt(store_returns_row->sr_item_sk_with_ticket_number);
        }
        case 2: {
            return qx_attribute_store_returns_catalog_sales->get_lcnt(catalog_sales_row->cs_bill_customer_sk);
        }
        case 3: {
            return qx_attribute_catalog_sales_date_dim->get_lcnt(date_dim_row2->d_date_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QxBatch::get_wlcnt(int i) {
    switch (i) {
        case 0: {
            return qx_attribute_date_dim_store_sales->get_wlcnt(store_sales_row->ss_sold_date_sk);
        }
        case 1: {
            return qx_attribute_store_sales_store_returns->get_wlcnt(store_returns_row->sr_item_sk_with_ticket_number);
        }
        case 2: {
            return qx_attribute_store_returns_catalog_sales->get_wlcnt(catalog_sales_row->cs_bill_customer_sk);
        }
        case 3: {
            return qx_attribute_catalog_sales_date_dim->get_wlcnt(date_dim_row2->d_date_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QxBatch::get_rcnt(int i) {
    switch (i) {
        case 0: {
            return qx_attribute_date_dim_store_sales->get_rcnt(date_dim_row->d_date_sk);
        }
        case 1: {
            return qx_attribute_store_sales_store_returns->get_rcnt(store_sales_row->ss_item_sk_with_ticket_number);
        }
        case 2: {
            return qx_attribute_store_returns_catalog_sales->get_rcnt(store_returns_row->sr_customer_sk);
        }
        case 3: {
            return qx_attribute_catalog_sales_date_dim->get_rcnt(catalog_sales_row->cs_sold_date_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QxBatch::get_wrcnt(int i) {
    switch (i) {
        case 0: {
            return qx_attribute_date_dim_store_sales->get_wrcnt(date_dim_row->d_date_sk);
        }
        case 1: {
            return qx_attribute_store_sales_store_returns->get_wrcnt(store_sales_row->ss_item_sk_with_ticket_number);
        }
        case 2: {
            return qx_attribute_store_returns_catalog_sales->get_wrcnt(store_returns_row->sr_customer_sk);
        }
        case 3: {
            return qx_attribute_catalog_sales_date_dim->get_wrcnt(catalog_sales_row->cs_sold_date_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QxBatch::get_left_layer_max_unit_size(int i) {
    switch (i) {
        case 0: {
            return qx_attribute_date_dim_store_sales->get_left_layer_max_unit_size(store_sales_row->ss_sold_date_sk);
        }
        case 1: {
            return qx_attribute_store_sales_store_returns->get_left_layer_max_unit_size(store_returns_row->sr_item_sk_with_ticket_number);
        }
        case 2: {
            return qx_attribute_store_returns_catalog_sales->get_left_layer_max_unit_size(catalog_sales_row->cs_bill_customer_sk);
        }
        case 3: {
            return qx_attribute_catalog_sales_date_dim->get_left_layer_max_unit_size(date_dim_row2->d_date_sk);
        }
        default: {
            exit(1);
        }
    }
}

unsigned long QxBatch::get_right_layer_max_unit_size(int i) {
    switch (i) {
        case 0: {
            return qx_attribute_date_dim_store_sales->get_right_layer_max_unit_size(date_dim_row->d_date_sk);
        }
        case 1: {
            return qx_attribute_store_sales_store_returns->get_right_layer_max_unit_size(store_sales_row->ss_item_sk_with_ticket_number);
        }
        case 2: {
            return qx_attribute_store_returns_catalog_sales->get_right_layer_max_unit_size(store_returns_row->sr_customer_sk);
        }
        case 3: {
            return qx_attribute_catalog_sales_date_dim->get_right_layer_max_unit_size(catalog_sales_row->cs_sold_date_sk);
        }
        default: {
            exit(1);
        }
    }
}

bool QxBatch::layer_nonempty(int i) {
    switch (i) {
        case 0: {
            return true;
        }
        case 1: {
            return store_sales_layer->find(layer_unit_sizes[i]) != store_sales_layer->end();
        }
        case 2: {
            return store_returns_layer->find(layer_unit_sizes[i]) != store_returns_layer->end();
        }
        case 3: {
            return catalog_sales_layer->find(layer_unit_sizes[i]) != catalog_sales_layer->end();
        }
        case 4: {
            return true;
        }
        default: {
            exit(1);
        }
    }
}