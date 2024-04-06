#include "reservoir.h"
#include "graph_row.h"
#include "star_join/star_join_batch.h"
#include "star_join/star_join_row.h"

StarJoinBatch::StarJoinBatch(std::unordered_map<int, std::vector<std::vector<GraphRow*>*>*>* relation_indices, int n): relation_indices(relation_indices), n(n) {
    rows = std::vector<GraphRow*>();
    for (int i=0; i<n; i++) {
        rows.emplace_back(nullptr);
    }

    indices = new unsigned long[n]{};
    unit_sizes = new unsigned long[n]{};
}

StarJoinBatch::~StarJoinBatch() {
    delete[] indices;
    delete[] unit_sizes;
}

void StarJoinBatch::set_row(GraphRow* row, int id) {
    relations = relation_indices->at(row->src);
    rows[id] = row;
    sid = id;

    unsigned long s = 1;
    for (int i=n-1; i>=0; i--) {
        if (i != sid) {
            unit_sizes[i] = s;
            s *= relations->at(i)->size();
        }
    }
    size = s;
    position = 0;
}

void StarJoinBatch::fill(Reservoir<StarJoinRow> *reservoir) {
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

void StarJoinBatch::init_real_iterator() {
    for (int i=0; i<n; i++) {
        if (i != sid) {
            indices[i] = 0;
            rows[i] = relations->at(i)->at(0);
        }
    }

    next_row = new StarJoinRow(rows, n);
}

bool StarJoinBatch::has_next_real() {
    return next_row != nullptr;
}

StarJoinRow *StarJoinBatch::next_real() {
    position += 1;

    auto result = next_row;
    compute_next_real();

    return result;
}

void StarJoinBatch::compute_next_real() {
    if (position >= size) {
        next_row = nullptr;
    } else {
        // position < size, we can find the next real
        for (int i = n - 1; i >= 0; i--) {
            if (i != sid) {
                if (indices[i] < relations->at(i)->size() - 1) {
                    indices[i] += 1;
                    rows[i] = relations->at(i)->at(indices[i]);

                    // relations to the right has been reset to index = 0
                    // it is safe to create a new StarJoinRow
                    next_row = new StarJoinRow(rows, n);
                    return;
                } else {
                    indices[i] = 0;
                    rows[i] = relations->at(i)->at(0);
                }
            }
        }
    }
}

void StarJoinBatch::fill_with_skip(Reservoir<StarJoinRow> *reservoir) {
    while (true) {
        if (size - position <= reservoir->get_t()) {
            reservoir->t_subtract(size - position);
            position = size;
            break;
        } else {
            position += (reservoir->get_t() + 1);
            unsigned long offset = position - 1;

            for (int i=0; i<n; i++) {
                if (i != sid) {
                    indices[i] = offset / unit_sizes[i];
                    offset = offset - indices[i] * unit_sizes[i];
                    rows[i] = relations->at(i)->at(indices[i]);
                }
            }

            reservoir->replace(new StarJoinRow(rows, n));
            reservoir->update_w();
            reservoir->update_t();
        }
    }
}