#include <fstream>
#include <iostream>
#include <vector>
#include "../../include/reservoir.h"
#include "../../include/utils.h"
#include "../../include/dumbbell/dumbbell_algorithm.h"
#include "../../include/dumbbell/dumbbell_attribute_left_middle.h"
#include "../../include/dumbbell/dumbbell_attribute_middle_right.h"
#include "../../include/dumbbell/dumbbell_left_batch.h"
#include "../../include/dumbbell/dumbbell_middle_batch.h"
#include "../../include/dumbbell/dumbbell_right_batch.h"
#include "../../include/dumbbell/dumbbell_rows.h"

DumbbellAlgorithm::~DumbbellAlgorithm() {
    delete reservoir;
    delete left_batch;
    delete middle_batch;
    delete right_batch;

    for (auto p : *a_to_b) {
        delete p.second;
    }

    for (auto p : *b_to_a) {
        delete p.second;
    }

    for (auto p : *b_to_c) {
        delete p.second;
    }

    for (auto p : *c_to_b) {
        delete p.second;
    }

    for (auto p : *c_to_a) {
        delete p.second;
    }

    for (auto p : *a_to_c) {
        delete p.second;
    }

    for (auto p : *x_to_y) {
        delete p.second;
    }

    for (auto p : *y_to_x) {
        delete p.second;
    }

    for (auto p : *y_to_z) {
        delete p.second;
    }

    for (auto p : *z_to_y) {
        delete p.second;
    }

    for (auto p : *z_to_x) {
        delete p.second;
    }

    for (auto p : *x_to_z) {
        delete p.second;
    }

    for (auto p : *src_to_row7) {
        for (auto row : *p.second) {
            delete row;
        }
        p.second->clear();
        delete p.second;
    }
    src_to_row7->clear();
    delete src_to_row7;

    for (auto p : *dst_to_row7) {
        p.second->clear();
        delete p.second;
    }
    dst_to_row7->clear();
    delete dst_to_row7;

    delete dumbbell_attribute_left_middle;
    delete dumbbell_attribute_middle_right;
}

void DumbbellAlgorithm::init(const std::string &file, int k) {
    this->input = file;
    this->reservoir = new Reservoir<DumbbellRow>(k);

    // left triangle
    this->a_to_b = new std::unordered_map<int, std::unordered_set<int>*>;
    this->b_to_a = new std::unordered_map<int, std::unordered_set<int>*>;
    this->b_to_c = new std::unordered_map<int, std::unordered_set<int>*>;
    this->c_to_b = new std::unordered_map<int, std::unordered_set<int>*>;
    this->c_to_a = new std::unordered_map<int, std::unordered_set<int>*>;
    this->a_to_c = new std::unordered_map<int, std::unordered_set<int>*>;

    // right triangle
    this->x_to_y = new std::unordered_map<int, std::unordered_set<int>*>;
    this->y_to_x = new std::unordered_map<int, std::unordered_set<int>*>;
    this->y_to_z = new std::unordered_map<int, std::unordered_set<int>*>;
    this->z_to_y = new std::unordered_map<int, std::unordered_set<int>*>;
    this->z_to_x = new std::unordered_map<int, std::unordered_set<int>*>;
    this->x_to_z = new std::unordered_map<int, std::unordered_set<int>*>;

    // middle
    this->src_to_row7 = new std::unordered_map<int, std::vector<GraphRow*>*>;
    this->dst_to_row7 = new std::unordered_map<int, std::vector<GraphRow*>*>;

    this->dumbbell_attribute_left_middle = new DumbbellAttributeLeftMiddle;
    this->dumbbell_attribute_middle_right = new DumbbellAttributeMiddleRight;

    dumbbell_attribute_left_middle->set_right_relation_index(src_to_row7);
    dumbbell_attribute_left_middle->set_right_attribute(dumbbell_attribute_middle_right);

    dumbbell_attribute_middle_right->set_left_relation_index(dst_to_row7);
    dumbbell_attribute_middle_right->set_left_attribute(dumbbell_attribute_left_middle);

    this->left_batch = new DumbbellLeftBatch(dumbbell_attribute_left_middle, dumbbell_attribute_middle_right);
    this->middle_batch = new DumbbellMiddleBatch;
    this->right_batch = new DumbbellRightBatch(dumbbell_attribute_left_middle, dumbbell_attribute_middle_right);
}

Reservoir<DumbbellRow> *DumbbellAlgorithm::run(bool print_info) {
    std::ifstream in(input);
    std::string line;

    auto start_time = std::chrono::system_clock::now();
    while (getline(in, line), !line.empty()) {
        if (line.at(0) == '-') {
            if (print_info) {
                auto end_time = std::chrono::system_clock::now();
                print_execution_info(start_time, end_time);
            }
        } else {
            process(line);
        }
    }

    return reservoir;
}

void DumbbellAlgorithm::process(const std::string &line) {
    unsigned long begin = 0;
    unsigned long lineSize = line.size();

    if (line.at(0) == '-')
        return;

    int sid = next_int(line, &begin, lineSize, '|');
    begin += 2;     // skip dummy const timestamp

    // sid = 0 -> R1(a,b), sid = 1 -> R2(b,c), sid = 2 -> R3(c,a)
    // sid = 3 -> R4(x,y), sid = 4 -> R5(y,z), sid = 5 -> R6(z,x)
    // sid = 6 -> R7(a,x)
    switch (sid) {
        case 0: {
            int a = next_int(line, &begin, lineSize, ',');
            int b = next_int(line, &begin, lineSize, ',');
            auto iter1 = a_to_b->find(a);
            if (iter1 == a_to_b->end()) {
                a_to_b->emplace(a, new std::unordered_set<int>{b});
            } else {
                iter1->second->emplace(b);
            }

            auto iter2 = b_to_a->find(b);
            if (iter2 == b_to_a->end()) {
                b_to_a->emplace(b, new std::unordered_set<int>{a});
            } else {
                iter2->second->emplace(a);
            }

            auto iter3 = a_to_c->find(a);
            if (iter3 != a_to_c->end()) {
                auto iter4 = b_to_c->find(b);
                if (iter4 != b_to_c->end()) {
                    auto set1 = iter3->second;
                    auto set2 = iter4->second;
                    if (set1->size() < set2->size()) {
                        for (int c: *set1) {
                            if (set2->find(c) != set2->end()) {
                                insert_left_triangle_row(a, b, c);
                            }
                        }
                    } else {
                        for (int c: *set2) {
                            if (set1->find(c) != set1->end()) {
                                insert_left_triangle_row(a, b, c);
                            }
                        }
                    }
                }
            }
            break;
        }
        case 1: {
            int b = next_int(line, &begin, lineSize, ',');
            int c = next_int(line, &begin, lineSize, ',');
            auto iter1 = b_to_c->find(b);
            if (iter1 == b_to_c->end()) {
                b_to_c->emplace(b, new std::unordered_set<int>{c});
            } else {
                iter1->second->emplace(c);
            }

            auto iter2 = c_to_b->find(c);
            if (iter2 == c_to_b->end()) {
                c_to_b->emplace(c, new std::unordered_set<int>{b});
            } else {
                iter2->second->emplace(b);
            }

            auto iter3 = b_to_a->find(b);
            if (iter3 != b_to_a->end()) {
                auto iter4 = c_to_a->find(c);
                if (iter4 != c_to_a->end()) {
                    auto set1 = iter3->second;
                    auto set2 = iter4->second;
                    if (set1->size() < set2->size()) {
                        for (int a: *set1) {
                            if (set2->find(a) != set2->end()) {
                                insert_left_triangle_row(a, b, c);
                            }
                        }
                    } else {
                        for (int a: *set2) {
                            if (set1->find(a) != set1->end()) {
                                insert_left_triangle_row(a, b, c);
                            }
                        }
                    }
                }
            }
            break;
        }
        case 2: {
            int c = next_int(line, &begin, lineSize, ',');
            int a = next_int(line, &begin, lineSize, ',');
            auto iter1 = c_to_a->find(c);
            if (iter1 == c_to_a->end()) {
                c_to_a->emplace(c, new std::unordered_set<int>{a});
            } else {
                iter1->second->emplace(a);
            }

            auto iter2 = a_to_c->find(a);
            if (iter2 == a_to_c->end()) {
                a_to_c->emplace(a, new std::unordered_set<int>{c});
            } else {
                iter2->second->emplace(c);
            }

            auto iter3 = c_to_b->find(c);
            if (iter3 != c_to_b->end()) {
                auto iter4 = a_to_b->find(a);
                if (iter4 != a_to_b->end()) {
                    auto set1 = iter3->second;
                    auto set2 = iter4->second;
                    if (set1->size() < set2->size()) {
                        for (int b: *set1) {
                            if (set2->find(b) != set2->end()) {
                                insert_left_triangle_row(a, b, c);
                            }
                        }
                    } else {
                        for (int b: *set2) {
                            if (set1->find(b) != set1->end()) {
                                insert_left_triangle_row(a, b, c);
                            }
                        }
                    }
                }
            }
            break;
        }
        case 3: {
            int x = next_int(line, &begin, lineSize, ',');
            int y = next_int(line, &begin, lineSize, ',');
            auto iter1 = x_to_y->find(x);
            if (iter1 == x_to_y->end()) {
                x_to_y->emplace(x, new std::unordered_set<int>{y});
            } else {
                iter1->second->emplace(y);
            }

            auto iter2 = y_to_x->find(y);
            if (iter2 == y_to_x->end()) {
                y_to_x->emplace(y, new std::unordered_set<int>{x});
            } else {
                iter2->second->emplace(x);
            }

            auto iter3 = x_to_z->find(x);
            if (iter3 != x_to_z->end()) {
                auto iter4 = y_to_z->find(y);
                if (iter4 != y_to_z->end()) {
                    auto set1 = iter3->second;
                    auto set2 = iter4->second;
                    if (set1->size() < set2->size()) {
                        for (int z: *set1) {
                            if (set2->find(z) != set2->end()) {
                                insert_right_triangle_row(x, y, z);
                            }
                        }
                    } else {
                        for (int z: *set2) {
                            if (set1->find(z) != set1->end()) {
                                insert_right_triangle_row(x, y, z);
                            }
                        }
                    }
                }
            }
            break;
        }
        case 4: {
            int y = next_int(line, &begin, lineSize, ',');
            int z = next_int(line, &begin, lineSize, ',');
            auto iter1 = y_to_z->find(y);
            if (iter1 == y_to_z->end()) {
                y_to_z->emplace(y, new std::unordered_set<int>{z});
            } else {
                iter1->second->emplace(z);
            }

            auto iter2 = z_to_y->find(z);
            if (iter2 == z_to_y->end()) {
                z_to_y->emplace(z, new std::unordered_set<int>{y});
            } else {
                iter2->second->emplace(y);
            }

            auto iter3 = y_to_x->find(y);
            if (iter3 != y_to_x->end()) {
                auto iter4 = z_to_x->find(z);
                if (iter4 != z_to_x->end()) {
                    auto set1 = iter3->second;
                    auto set2 = iter4->second;
                    if (set1->size() < set2->size()) {
                        for (int x: *set1) {
                            if (set2->find(x) != set2->end()) {
                                insert_right_triangle_row(x, y, z);
                            }
                        }
                    } else {
                        for (int x: *set2) {
                            if (set1->find(x) != set1->end()) {
                                insert_right_triangle_row(x, y, z);
                            }
                        }
                    }
                }
            }
            break;
        }
        case 5: {
            int z = next_int(line, &begin, lineSize, ',');
            int x = next_int(line, &begin, lineSize, ',');
            auto iter1 = z_to_x->find(z);
            if (iter1 == z_to_x->end()) {
                z_to_x->emplace(z, new std::unordered_set<int>{x});
            } else {
                iter1->second->emplace(x);
            }

            auto iter2 = x_to_z->find(x);
            if (iter2 == x_to_z->end()) {
                x_to_z->emplace(x, new std::unordered_set<int>{z});
            } else {
                iter2->second->emplace(z);
            }

            auto iter3 = z_to_y->find(z);
            if (iter3 != z_to_y->end()) {
                auto iter4 = x_to_y->find(x);
                if (iter4 != x_to_y->end()) {
                    auto set1 = iter3->second;
                    auto set2 = iter4->second;
                    if (set1->size() < set2->size()) {
                        for (int y: *set1) {
                            if (set2->find(y) != set2->end()) {
                                insert_right_triangle_row(x, y, z);
                            }
                        }
                    } else {
                        for (int y: *set2) {
                            if (set1->find(y) != set1->end()) {
                                insert_right_triangle_row(x, y, z);
                            }
                        }
                    }
                }
            }
            break;
        }
        case 6: {
            int src = next_int(line, &begin, lineSize, ',');
            int dst = next_int(line, &begin, lineSize, ',');
            auto row = new GraphRow(src, dst);

            auto iter3 = src_to_row7->find(src);
            if (iter3 != src_to_row7->end()) {
                iter3->second->emplace_back(row);
            } else {
                src_to_row7->emplace(src, new std::vector<GraphRow*>{row});
            }

            auto iter4 = dst_to_row7->find(dst);
            if (iter4 != dst_to_row7->end()) {
                iter4->second->emplace_back(row);
            } else {
                dst_to_row7->emplace(dst, new std::vector<GraphRow*>{row});
            }

            unsigned long wlcnt = dumbbell_attribute_left_middle->get_wlcnt(src);
            if (wlcnt > 0) {
                dumbbell_attribute_middle_right->update_from_left(row, 0, wlcnt);
            }

            unsigned long wrcnt = dumbbell_attribute_middle_right->get_wrcnt(dst);
            if (wrcnt > 0) {
                dumbbell_attribute_left_middle->update_from_right(row, 0, wrcnt);
            }

            if (wlcnt > 0 && wrcnt > 0) {
                middle_batch->init(dumbbell_attribute_left_middle->get_left_layer(src),
                                   dumbbell_attribute_middle_right->get_right_layer(dst));
                middle_batch->fill(reservoir);
            }
            break;
        }
    }
}

void DumbbellAlgorithm::insert_left_triangle_row(int a, int b, int c) {
    auto row = new TriangleRow(a, b, c);
    dumbbell_attribute_left_middle->update_from_left(row);
    left_batch->set_left_row(row);
    left_batch->fill(reservoir);
}

void DumbbellAlgorithm::insert_right_triangle_row(int x, int y, int z) {
    auto row = new TriangleRow(x, y, z);
    dumbbell_attribute_middle_right->update_from_right(row);
    right_batch->set_right_row(row);
    right_batch->fill(reservoir);
}