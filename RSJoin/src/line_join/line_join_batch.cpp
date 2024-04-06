#include "../include/reservoir.h"
#include "../../include/line_join/line_join_batch.h"
#include "../../include/line_join/line_join_row.h"
#include "../../include/line_join/line_join_attribute.h"

LineJoinBatch::LineJoinBatch(std::vector<LineJoinAttribute*>* line_join_attributes, int n, GraphRow* row, int sid)
        : line_join_attributes(line_join_attributes), n(n), sid(sid) {
    rows = std::vector<GraphRow*>();
    for (int i=0; i<n; i++) {
        rows.emplace_back(nullptr);
    }

    layer_unit_sizes = new unsigned long[n]{};
    for (int i=0; i<n; i++) {
        layer_unit_sizes[i] = 1;
    }

    layer_indices = new unsigned long[n]{};
    layers = std::vector<std::unordered_map<unsigned long, std::vector<GraphRow*>*>*>();
    for (int i=0; i<n; i++) {
        layers.emplace_back(nullptr);
    }

    rows.at(sid) = row;
    if (sid == 0) {
        size = line_join_attributes->at(0)->get_rcnt(row->dst);
    } else if (sid == n-1) {
        size = line_join_attributes->at(n-2)->get_lcnt(row->src);
    } else {
        size = line_join_attributes->at(sid-1)->get_lcnt(row->src) * line_join_attributes->at(sid)->get_rcnt(row->dst);
    }

    position = 0;
}

LineJoinBatch::~LineJoinBatch() {
    delete[] layer_unit_sizes;
    delete[] layer_indices;
}

void LineJoinBatch::fill(Reservoir<LineJoinRow>* reservoir) {
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

void LineJoinBatch::init_real_iterator() {
    for (int i=sid-1; i>=0; i--) {
        update_layer(i, false);
        search_layer_unit_size(i);
        reset_layer_index(i);
        update_row(i);
    }

    for (int i=sid+1; i<n; i++) {
        update_layer(i, true);
        search_layer_unit_size(i);
        reset_layer_index(i);
        update_row(i);
    }

    next_row = new LineJoinRow(rows, n);
    position_after_next_real = 1;
}

bool LineJoinBatch::has_next_real() {
    return next_row != nullptr;
}

LineJoinRow *LineJoinBatch::next_real() {
    position = position_after_next_real;

    auto result = next_row;
    compute_next_real();

    return result;
}

void LineJoinBatch::compute_next_real() {
    position_after_next_real = position + 1;
    for (int i=n-1; i>sid; i--) {
        if (i == n-1) {
            if (layer_indices[i] < layers.at(i)->at(1)->size() - 1) {
                layer_indices[i] += 1;
                update_row(i);
                next_row = new LineJoinRow(rows, n);
                return;
            } else {
                if (i-1 > sid) {
                    position_after_next_real += (line_join_attributes->at(i-1)->get_wrcnt(rows.at(i-1)->dst) -
                            line_join_attributes->at(i-1)->get_rcnt(rows.at(i-1)->dst));
                }
            }
        } else {
            if (layer_indices[i] < layers.at(i)->at(layer_unit_sizes[i])->size() - 1) {
                layer_indices[i] += 1;
                update_row(i);
                for (int j=i+1; j<n; j++) {
                    update_layer(j, true);
                    if (j < n-1) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new LineJoinRow(rows, n);
                return;
            } else if (layer_unit_sizes[i] < line_join_attributes->at(i-1)->get_right_layer_max_unit_size(rows.at(i-1)->dst)) {
                next_layer_unit_size(i);
                reset_layer_index(i);
                update_row(i);
                for (int j=i+1; j<n; j++) {
                    update_layer(j, true);
                    if (j < n-1) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new LineJoinRow(rows, n);
                return;
            } else {
                if (i-1 > sid) {
                    position_after_next_real += (line_join_attributes->at(i-1)->get_wrcnt(rows.at(i-1)->dst) -
                            line_join_attributes->at(i-1)->get_rcnt(rows.at(i-1)->dst));
                }
            }
        }
    }

    // run to here means the right hand side has exhausted

    for (int i=0; i<sid; i++) {
        if (i == 0) {
            if (layer_indices[i] < layers.at(i)->at(1)->size() - 1) {
                layer_indices[i] += 1;
                update_row(i);
                for (int j=sid+1; j<n; j++) {
                    update_layer(j, true);
                    if (j < n-1) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new LineJoinRow(rows, n);
                return;
            } else if (i+1 < sid) {
                if (sid < n-1) {
                    position_after_next_real +=
                            ((line_join_attributes->at(i)->get_wlcnt(rows.at(i+1)->src) - line_join_attributes->at(i)->get_lcnt(rows.at(i+1)->src))
                                * line_join_attributes->at(sid)->get_rcnt(rows.at(sid)->dst));
                } else {
                    position_after_next_real +=
                            (line_join_attributes->at(i)->get_wlcnt(rows.at(i+1)->src) - line_join_attributes->at(i)->get_lcnt(rows.at(i+1)->src));
                }
            }
        } else {
            if (layer_indices[i] < layers.at(i)->at(layer_unit_sizes[i])->size() - 1) {
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
                for (int j=sid+1; j<n; j++) {
                    update_layer(j, true);
                    if (j > 0) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new LineJoinRow(rows, n);
                return;
            } else if (layer_unit_sizes[i] < line_join_attributes->at(i)->get_left_layer_max_unit_size(rows.at(i+1)->src)) {
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
                for (int j=sid+1; j<n; j++) {
                    update_layer(j, true);
                    if (j > 0) {
                        reset_layer_unit_size(j);
                        search_layer_unit_size(j);
                    }
                    reset_layer_index(j);
                    update_row(j);
                }
                next_row = new LineJoinRow(rows, n);
                return;
            } else if (i+1 < sid) {
                if (sid < n-1) {
                    position_after_next_real +=
                            ((line_join_attributes->at(i)->get_wlcnt(rows.at(i+1)->src) - line_join_attributes->at(i)->get_lcnt(rows.at(i+1)->src))
                             * line_join_attributes->at(sid)->get_rcnt(rows.at(sid)->dst));
                } else {
                    position_after_next_real +=
                            (line_join_attributes->at(i)->get_wlcnt(rows.at(i+1)->src) - line_join_attributes->at(i)->get_lcnt(rows.at(i+1)->src));
                }
            }
        }
    }

    // run to here means the left hand side has exhausted
    // no more real tuples.
    position_after_next_real = size;
    next_row = nullptr;
}

void LineJoinBatch::search_layer_unit_size(int i) {
    while (layers.at(i)->find(layer_unit_sizes[i]) == layers.at(i)->end()) {
        layer_unit_sizes[i] *= 2;
    }
}

void LineJoinBatch::reset_layer_unit_size(int i) {
    layer_unit_sizes[i] = 1;
}

void LineJoinBatch::next_layer_unit_size(int i) {
    layer_unit_sizes[i] *= 2;
    search_layer_unit_size(i);
}

void LineJoinBatch::reset_layer_index(int i) {
    layer_indices[i] = 0;
}

void LineJoinBatch::update_row(int i) {
    rows.at(i) = layers.at(i)->at(layer_unit_sizes[i])->at(layer_indices[i]);
}

void LineJoinBatch::update_layer(int i, bool from_left) {
    if (from_left) {
        layers[i] = line_join_attributes->at(i-1)->get_right_layer(rows.at(i-1)->dst);
    } else {
        layers[i] = line_join_attributes->at(i)->get_left_layer(rows.at(i+1)->src);
    }
}

void LineJoinBatch::fill_with_skip(Reservoir<LineJoinRow>* reservoir) {
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
            if (sid == 0 || sid == n-1) {
                rcnt = 1;
            } else {
                rcnt = line_join_attributes->at(sid)->get_rcnt(rows.at(sid)->dst);
            }

            for (int i=sid-1; i>=0; i--) {
                if (i == 0) {
                    update_layer(i, false);
                    layer_indices[i] = offset / rcnt;
                    offset -= layer_indices[i] * rcnt;
                    update_row(i);
                } else {
                    update_layer(i, false);
                    reset_layer_unit_size(i);
                    while (layer_unit_sizes[i] <= line_join_attributes->at(i)->get_left_layer_max_unit_size(rows.at(i + 1)->src)) {
                        if (layers.at(i)->find(layer_unit_sizes[i]) != layers.at(i)->end()) {
                            if (offset < layers.at(i)->at(layer_unit_sizes[i])->size() * layer_unit_sizes[i] * rcnt) {
                                layer_indices[i] = offset / (layer_unit_sizes[i] * rcnt);
                                offset -= layer_indices[i] * layer_unit_sizes[i] * rcnt;
                                break;
                            } else {
                                offset -= layers.at(i)->at(layer_unit_sizes[i])->size() * layer_unit_sizes[i] * rcnt;
                            }
                        }
                        layer_unit_sizes[i] *= 2;
                    }
                    update_row(i);

                    if (offset >= line_join_attributes->at(i - 1)->get_lcnt(rows.at(i)->src) * rcnt) {
                        reservoir->update_t();
                        // we get a dummy tuple, update reservoir.t and try next pick
                        goto LOOP;
                    }
                }
            }

            // run to here means we have determined the rows on the left hand side of sid

            for (int i=sid+1; i<n; i++) {
                if (i == n-1) {
                    update_layer(i, true);
                    layer_indices[i] = offset;
                    offset = 0;
                    update_row(i);
                } else {
                    update_layer(i, true);
                    reset_layer_unit_size(i);
                    while (layer_unit_sizes[i] <= line_join_attributes->at(i-1)->get_right_layer_max_unit_size(rows.at(i - 1)->dst)) {
                        if (layers.at(i)->find(layer_unit_sizes[i]) != layers.at(i)->end()) {
                            if (offset < layers.at(i)->at(layer_unit_sizes[i])->size() * layer_unit_sizes[i]) {
                                layer_indices[i] = offset / (layer_unit_sizes[i]);
                                offset -= layer_indices[i] * layer_unit_sizes[i];
                                break;
                            } else {
                                offset -= layers.at(i)->at(layer_unit_sizes[i])->size() * layer_unit_sizes[i];
                            }
                        }
                        layer_unit_sizes[i] *= 2;
                    }
                    update_row(i);

                    if (offset >= line_join_attributes->at(i)->get_rcnt(rows.at(i)->dst)) {
                        reservoir->update_t();
                        // we get a dummy tuple, update reservoir.t and try next pick
                        goto LOOP;
                    }
                }
            }

            // now we have all the rows, return a join result
            reservoir->replace(new LineJoinRow(rows, n));
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}