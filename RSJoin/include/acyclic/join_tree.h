#ifndef RESERVOIR_JOIN_TREE_H
#define RESERVOIR_JOIN_TREE_H

#include <unordered_map>
#include <functional>
#include <vector>
#include "reservoir.h"

template<typename R>
class TreeNode;
template<typename R>
class JoinTreeBatch;
template<typename R, typename T, typename H>
class Relation;


template<typename R>
class JoinTreeBatch {
public:
    JoinTreeBatch();
    ~JoinTreeBatch();

    void fill(Reservoir<R>* reservoir);

    bool is_inited = false;
    R* buffer;
    bool is_buffer_valid;

    TreeNode<R>* root_node;
    std::vector<TreeNode<R>*>* tree_nodes;

    unsigned long size = 0;
    unsigned long position = 0;
    unsigned long position_after_next_real = 0;

private:
    void init_real_iterator();
    bool has_next_real();
    R* next_real();
    void compute_next_real();
    void fill_with_skip(Reservoir<R>* reservoir);
};


template<typename R>
class TreeNode {
public:
    virtual unsigned long get_cnt(unsigned long key) { return 0; }
    virtual unsigned long get_wcnt(unsigned long key) { return 0; }
    virtual void update(int child_id, unsigned long key, unsigned long old_count, unsigned long new_count) {};

    virtual void register_in_batch(JoinTreeBatch<R>* batch) {};
    virtual void init_real_iterator(JoinTreeBatch<R>* batch) {};
    virtual unsigned long get_join_key(int id) { return 0; };
    virtual unsigned long get_size_factor(int id) { return 0; };
    virtual bool fill_in_next(JoinTreeBatch<R>* batch) { return false; };

    virtual bool fill_in_tuple(JoinTreeBatch<R>* batch, unsigned long location) { return false; };
    virtual bool fill_in_children_tuples(JoinTreeBatch<R>* batch, unsigned long location) { return false; };
};


template<typename R, typename T, typename H>
class Relation final : public TreeNode<R> {
public:
    explicit Relation(std::function<void(R*, T*)> fill_func);
    ~Relation();

    int register_child(TreeNode<R>* child, std::function<unsigned long(T*)> project_func);
    void register_parent(TreeNode<R>* parent, int id, std::function<unsigned long(T*)> project_func);
    JoinTreeBatch<R>* insert(T* tuple);

    // methods from TreeNode<R>
    unsigned long get_cnt(unsigned long key) override;
    unsigned long get_wcnt(unsigned long key) override;
    void update(int id, unsigned long key, unsigned long old_count, unsigned long new_count) override;
    void register_in_batch(JoinTreeBatch<R>* batch) override;
    void init_real_iterator(JoinTreeBatch<R>* batch) override;
    unsigned long get_join_key(int id) override;
    unsigned long get_size_factor(int id) override;
    bool fill_in_next(JoinTreeBatch<R>* batch) override;
    bool fill_in_tuple(JoinTreeBatch<R>* batch, unsigned long location) override;
    bool fill_in_children_tuples(JoinTreeBatch<R>* batch, unsigned long location) override;

private:
    JoinTreeBatch<R>* join_tree_batch = nullptr;  // used only in root nodes

    std::unordered_map<unsigned long, unsigned long> cnt;
    std::unordered_map<unsigned long, unsigned long> wcnt;

    TreeNode<R>* parent_node = nullptr;
    int id_in_parent_node = -1;
    std::function<unsigned long(T*)> project_to_parent_node;

    std::vector<TreeNode<R>*> children_nodes;
    std::vector<std::function<unsigned long(T*)>> project_to_children_nodes;
    std::vector<std::unordered_map<unsigned long, std::vector<T*>*>*> children_indices;

    std::unordered_map<T, unsigned long, H> tuple_counts;
    std::unordered_map<T, unsigned long, H> tuple_layer_indices;

    // join_key -> unit_size -> tuples
    std::unordered_map<unsigned long, std::unordered_map<unsigned long, std::vector<T*>*>*> join_key_to_layers;
    std::unordered_map<unsigned long, unsigned long> join_key_to_max_layer_unit_sizes;

    std::function<void(R*,T*)> fill_func;
    std::unordered_map<unsigned long, std::vector<T*>*>* current_layers;
    unsigned long current_layer_unit_size;
    unsigned long current_layer_index;

    std::vector<unsigned long> children_join_keys;
    std::vector<unsigned long> children_size_factors;

    unsigned long insert_and_compute_tuple_count(T* tuple);
    unsigned long compute_existed_tuple_count(T* tuple);
    void update_inserted_tuple_layer(T* tuple, unsigned long join_key, unsigned long tuple_count);
    void update_existed_tuple_layer(T* tuple, unsigned long join_key, unsigned old_tuple_count, unsigned long new_tuple_count);
    void update_cnt(unsigned long join_key, unsigned long old_tuple_count, unsigned long new_tuple_count);
    void update_wcnt(unsigned long join_key);
    void propagate_to_parent_node(unsigned long join_key, unsigned long old_count, unsigned long new_count);
    void update_current_tuple_in_batch(JoinTreeBatch<R>* batch, unsigned long base);
};

// ------ implementation of JoinTreeBatch<R> begins ------

template<typename R>
JoinTreeBatch<R>::JoinTreeBatch() {
    this->buffer = new R;
    this->tree_nodes = new std::vector<TreeNode<R>*>;
}

template<typename R>
JoinTreeBatch<R>::~JoinTreeBatch() {
    delete buffer;
    delete tree_nodes;
}

template<typename R>
void JoinTreeBatch<R>::fill(Reservoir<R> *reservoir) {
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

template<typename R>
void JoinTreeBatch<R>::init_real_iterator() {
    for (int i=0; i<tree_nodes->size(); i++) {
        tree_nodes->at(i)->init_real_iterator(this);
    }
    position_after_next_real = 1;
    is_buffer_valid = true;
}

template<typename R>
inline bool JoinTreeBatch<R>::has_next_real() {
    return is_buffer_valid;
}

template<typename R>
inline R* JoinTreeBatch<R>::next_real() {
    position = position_after_next_real;
    R* next_row = new R(*buffer);
    compute_next_real();
    return next_row;
}

template<typename R>
void JoinTreeBatch<R>::compute_next_real() {
    position_after_next_real = position + 1;
    int i = tree_nodes->size() - 1;
    bool filled;
    while (i >= 0) {
        filled = tree_nodes->at(i)->fill_in_next(this);
        if (filled) {
            break;
        }
        i--;
    }

    if (filled) {
        // tree_nodes[i] still have new tuples, re-init the tree_nodes starting from i+1
        // Note that the following for-loop is skipped if i == tree_nodes->size() - 1
        for (int j = i+1; j < tree_nodes->size(); j++) {
            tree_nodes->at(j)->init_real_iterator(this);
        }
    } else {
        // we have reach the head of tree_nodes but the tuple in buffer is still invalid.
        // the real tuples are exhausted, no next real in this batch.
        is_buffer_valid = false;
    }
}

template<typename R>
void JoinTreeBatch<R>::fill_with_skip(Reservoir<R> *reservoir) {
    while (true) {
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
            bool filled = root_node->fill_in_children_tuples(this, offset);

            if (filled) {
                // the tuple at (position - 1) is already filled in buffer
                R* row = new R(*buffer);
                reservoir->replace(row);
                reservoir->update_w();
                reservoir->update_t();
            } else {
                // pick another t and try again
                reservoir->update_t();
            }
        }
    }
}

// ------ implementation of JoinTreeBatch<R> ends ------

// ------ implementation of Relation<R,T,H> begins ------

template<typename R, typename T, typename H>
Relation<R,T,H>::~Relation() {
    if (join_tree_batch != nullptr)
        delete join_tree_batch;

    auto iter1 = join_key_to_layers.begin();
    while (iter1 != join_key_to_layers.end()) {
        std::unordered_map<unsigned long, std::vector<T*>*>* layers = iter1->second;
        auto iter2 = layers->begin();
        while (iter2 != layers->end()) {
            std::vector<T*>* vec = iter2->second;
            vec->clear();
            delete vec;
            iter2++;
        }
        layers->clear();
        delete layers;
        iter1++;
    }

    for (auto index : children_indices) {
        auto iter3 = index->begin();
        while (iter3 != index->end()) {
            std::vector<T*>* vec = iter3->second;
            vec->clear();
            delete vec;
            iter3++;
        }
        index->clear();
        delete index;
    }
}

template<typename R, typename T, typename H>
int Relation<R,T,H>::register_child(TreeNode<R>* child, std::function<unsigned long(T*)> project_func) {
    int id = children_nodes.size();
    children_nodes.emplace_back(child);
    project_to_children_nodes.emplace_back(project_func);
    children_indices.emplace_back(new std::unordered_map<unsigned long, std::vector<T*>*>);
    children_join_keys.emplace_back(0);     // placeholder
    children_size_factors.emplace_back(1);  // placeholder
    return id;
}

template<typename R, typename T, typename H>
void Relation<R,T,H>::register_parent(TreeNode<R>* parent, int id, std::function<unsigned long(T*)> project_func) {
    parent_node = parent;
    id_in_parent_node = id;
    project_to_parent_node = project_func;
}

template<typename R, typename T, typename H>
JoinTreeBatch<R>* Relation<R,T,H>::insert(T *tuple) {
    if (parent_node == nullptr) {
        // current node is root, check if delta size > 0
        // here we use cnt instead of wcnt to make the result more accurate
        unsigned long tuple_count = 1;
        int size = children_nodes.size();
        for (int i=0; i<size; i++) {
            auto project = project_to_children_nodes.at(i);
            unsigned long key = project(tuple);
            tuple_count *= children_nodes.at(i)->get_cnt(key);
        }

        if (tuple_count > 0) {
            // generate a batch based on the witness tuple
            if (join_tree_batch == nullptr) {
                join_tree_batch = new JoinTreeBatch<R>();
                join_tree_batch->root_node = this;
            }
            join_tree_batch->size = tuple_count;
            join_tree_batch->position = 0;
            join_tree_batch->position_after_next_real = 0;
            join_tree_batch->is_buffer_valid = false;
            // fill in the witness tuple
            fill_func(join_tree_batch->buffer, tuple);
            auto children_nodes_size = children_nodes.size();
            if (children_nodes_size > 0) {
                for (int i = 0; i < children_nodes_size; i++) {
                    children_join_keys.at(i) = project_to_children_nodes.at(i)(tuple);
                }

                children_size_factors.at(children_nodes_size - 1) = 1;
                for (int i = children_nodes_size-2; i >= 0; i--) {
                    // Note that we use get_cnt instead of get_wcnt at the root node.
                    children_size_factors.at(i) = children_nodes.at(i+1)->get_cnt(children_join_keys.at(i+1)) * children_size_factors.at(i+1);
                }

                if (!join_tree_batch->is_inited) {
                    for (int i = 0; i < children_nodes_size; i++) {
                        children_nodes.at(i)->register_in_batch(join_tree_batch);
                    }
                    join_tree_batch->is_inited = true;
                }
            }
            return join_tree_batch;
        } else {
            return nullptr;     // delta size = 0, no need to produce an empty batch
        }
    } else {
        // current node is non-root, insert into indices and propagate to parent if needed
        unsigned long tuple_count = insert_and_compute_tuple_count(tuple);
        tuple_counts.emplace(*tuple, tuple_count);

        if (tuple_count > 0) {
            auto join_key = project_to_parent_node(tuple);
            update_inserted_tuple_layer(tuple, join_key, tuple_count);
            update_cnt(join_key, 0, tuple_count);
            update_wcnt(join_key);
        }
        return nullptr;     // non-root node won't produce batch
    }
}

template<typename R, typename T, typename H>
unsigned long Relation<R,T,H>::get_cnt(unsigned long key) {
    auto iter = cnt.find(key);
    if (iter == cnt.end()) {
        return 0;
    } else {
        return iter->second;
    }
}

template<typename R, typename T, typename H>
unsigned long Relation<R,T,H>::get_wcnt(unsigned long key) {
    auto iter = wcnt.find(key);
    if (iter == wcnt.end()) {
        return 0;
    } else {
        return iter->second;
    }
}

template<typename R, typename T, typename H>
void Relation<R,T,H>::update(int id, unsigned long key, unsigned long old_count, unsigned long new_count) {
    std::unordered_map<unsigned long, std::vector<T*>*>* children_index = children_indices.at(id);
    auto iter1 = children_index->find(key);
    if (iter1 != children_index->end()) {
        auto tuples = iter1->second;

        for (auto tuple: *tuples) {
            unsigned long old_tuple_count = tuple_counts.at(*tuple);
            unsigned long new_tuple_count = compute_existed_tuple_count(tuple);
            tuple_counts[*tuple] = new_tuple_count;

            if (project_to_parent_node != NULL && new_tuple_count > 0) {
                auto join_key = project_to_parent_node(tuple);
                update_existed_tuple_layer(tuple, join_key, old_tuple_count, new_tuple_count);
                update_cnt(join_key, old_tuple_count, new_tuple_count);
                update_wcnt(join_key);
            }
        }
    }
}

template<typename R, typename T, typename H>
void Relation<R,T,H>::register_in_batch(JoinTreeBatch<R> *batch) {
    batch->tree_nodes->emplace_back(this);
    auto children_nodes_size = children_nodes.size();
    if (children_nodes_size > 0) {
        for (int i = 0; i < children_nodes_size; i++) {
            children_nodes.at(i)->register_in_batch(batch);
        }
    }
}

template<typename R, typename T, typename H>
void Relation<R,T,H>::init_real_iterator(JoinTreeBatch<R>* batch) {
    auto join_key = parent_node->get_join_key(id_in_parent_node);
    this->current_layers = join_key_to_layers.at(join_key);
    this->current_layer_unit_size = 1;
    while (current_layers->find(current_layer_unit_size) == current_layers->end()) {
        current_layer_unit_size *= 2;
    }
    this->current_layer_index = 0;
    update_current_tuple_in_batch(batch, parent_node->get_size_factor(id_in_parent_node));
}

template<typename R, typename T, typename H>
unsigned long Relation<R,T,H>::get_join_key(int id) {
    return children_join_keys.at(id);
}

template<typename R, typename T, typename H>
unsigned long Relation<R,T,H>::get_size_factor(int id) {
    return children_size_factors.at(id);
}

template<typename R, typename T, typename H>
bool Relation<R,T,H>::fill_in_next(JoinTreeBatch<R> *batch) {
    if (current_layer_index < current_layers->at(current_layer_unit_size)->size() - 1) {
        current_layer_index += 1;
        update_current_tuple_in_batch(batch, parent_node->get_size_factor(id_in_parent_node));
        return true;
    } else if (current_layer_unit_size < join_key_to_max_layer_unit_sizes.at(parent_node->get_join_key(id_in_parent_node))) {
        current_layer_unit_size *= 2;
        while (current_layers->find(current_layer_unit_size) == current_layers->end()) {
            current_layer_unit_size *= 2;
        }
        this->current_layer_index = 0;
        update_current_tuple_in_batch(batch, parent_node->get_size_factor(id_in_parent_node));
        return true;
    } else {
        auto join_key = parent_node->get_join_key(id_in_parent_node);
        auto size_factor = parent_node->get_size_factor(id_in_parent_node);
        batch->position_after_next_real += (wcnt.at(join_key) - cnt.at(join_key)) * size_factor;
        return false;
    }
}

template<typename R, typename T, typename H>
bool Relation<R,T,H>::fill_in_tuple(JoinTreeBatch<R> *batch, unsigned long location) {
    auto join_key = parent_node->get_join_key(id_in_parent_node);
    if (cnt.at(join_key) <= location) {
        return false;   // falls in the over-estimated part
    } else {
        unsigned long offset = location;
        current_layers = join_key_to_layers.at(join_key);
        current_layer_unit_size = 1;
        while (current_layer_unit_size <= join_key_to_max_layer_unit_sizes.at(join_key)) {
            if (current_layers->find(current_layer_unit_size) != current_layers->end()) {
                if (offset < current_layers->at(current_layer_unit_size)->size() * current_layer_unit_size) {
                    current_layer_index = offset / (current_layer_unit_size);
                    offset -= current_layer_index * current_layer_unit_size;
                    break;
                } else {
                    offset -= current_layers->at(current_layer_unit_size)->size() * current_layer_unit_size;
                }
            }
            current_layer_unit_size *= 2;
        }
        update_current_tuple_in_batch(batch, 1);

        if (children_nodes.size() > 0) {
            return fill_in_children_tuples(batch, offset);
        } else {
            return true;
        }
    }
}

template<typename R, typename T, typename H>
bool Relation<R,T,H>::fill_in_children_tuples(JoinTreeBatch<R> *batch, unsigned long location) {
    // assuming join_keys and size_factors for children nodes are already set.
    // for the children nodes of the root, they are set in the initialization of the batch;
    // for "deeper" nodes, they are set in fill_in_tuple() just before calling this method
    unsigned long offset = location;
    for (int i=0; i<children_nodes.size(); i++) {
        auto size_factor = children_size_factors.at(i);
        auto children_location = offset / size_factor;
        offset -= children_location * size_factor;
        bool filled = children_nodes.at(i)->fill_in_tuple(batch, children_location);

        if (!filled) {
            return false;   // the tuple is dummy at children_location in  children_nodes[i]
        }
    }
    return true;    // return ture if all the children_nodes fill in a real tuple
}

template<typename R, typename T, typename H>
inline unsigned long Relation<R,T,H>::insert_and_compute_tuple_count(T* tuple) {
    unsigned long count = 1;
    int size = children_nodes.size();
    for (int i=0; i<size; i++) {
        auto project = project_to_children_nodes.at(i);
        unsigned long key = project(tuple);
        auto index = children_indices.at(i);
        auto iter1 = index->find(key);
        if (iter1 == index->end()) {
            index->emplace(key, new std::vector<T*>{tuple});
        } else {
            iter1->second->emplace_back(tuple);
        }
        count *= children_nodes.at(i)->get_wcnt(key);
    }
    return count;
}

template<typename R, typename T, typename H>
inline unsigned long Relation<R,T,H>::compute_existed_tuple_count(T *tuple) {
    unsigned long count = 1;
    int size = children_nodes.size();
    for (int i=0; i<size; i++) {
        auto project = project_to_children_nodes.at(i);
        unsigned long key = project(tuple);
        count *= children_nodes.at(i)->get_wcnt(key);
    }
    return count;
}

template<typename R, typename T, typename H>
inline void Relation<R,T,H>::update_inserted_tuple_layer(T *tuple, unsigned long join_key, unsigned long tuple_count) {
    // the newly inserted tuple always have 0 old_count
    // we don't need to remove it from the previous layer
    auto iter1 = join_key_to_layers.find(join_key);
    if (iter1 == join_key_to_layers.end()) {
        auto layer = new std::unordered_map<unsigned long, std::vector<T*>*>;
        layer->emplace(tuple_count, new std::vector<T*>{tuple});
        join_key_to_layers.emplace(join_key, layer);
        if (join_key_to_max_layer_unit_sizes[join_key] < tuple_count) {
            join_key_to_max_layer_unit_sizes[join_key] = tuple_count;
        }
        tuple_layer_indices.emplace(*tuple, 0);
    } else {
        auto layer = iter1->second;
        auto iter2 = layer->find(tuple_count);
        if (iter2 == layer->end()) {
            layer->emplace(tuple_count, new std::vector<T*>{tuple});
            if (join_key_to_max_layer_unit_sizes[join_key] < tuple_count) {
                join_key_to_max_layer_unit_sizes[join_key] = tuple_count;
            }
            tuple_layer_indices.emplace(*tuple, 0);
        } else {
            auto location = iter2->second->size();
            iter2->second->emplace_back(tuple);
            tuple_layer_indices.emplace(*tuple, location);
        }
    }
}

template<typename R, typename T, typename H>
void Relation<R,T,H>::update_existed_tuple_layer(T *tuple, unsigned long join_key, unsigned int old_tuple_count, unsigned long new_tuple_count) {
    if (old_tuple_count > 0) {
        // old_tuple_count > 0, layer for join_key must exist
        std::unordered_map<unsigned long, std::vector<T*>*>* layer = join_key_to_layers.at(join_key);
        // remove tuple in the old layer
        std::vector<T*>* old_vector = layer->at(old_tuple_count);
        if (old_vector->size() == 1) {
            old_vector->clear();
            layer->erase(old_tuple_count);
        } else {
            auto last_tuple = old_vector->back();
            auto index = tuple_layer_indices[*tuple];
            old_vector->at(index) = last_tuple;
            tuple_layer_indices[*last_tuple] = index;
            old_vector->pop_back();
        }

        auto iter1 = layer->find(new_tuple_count);
        if (iter1 == layer->end()) {
            tuple_layer_indices[*tuple] = 0;
            layer->emplace(new_tuple_count, new std::vector<T*>{tuple});
            if (join_key_to_max_layer_unit_sizes[join_key] < new_tuple_count) {
                join_key_to_max_layer_unit_sizes[join_key] = new_tuple_count;
            }
        } else {
            tuple_layer_indices[*tuple] = iter1->second->size();
            iter1->second->emplace_back(tuple);
        }
    } else {
        // old_tuple_count == 0, layer for join_key may not exist
        auto iter1 = join_key_to_layers.find(join_key);
        if (iter1 == join_key_to_layers.end()) {
            auto layer = new std::unordered_map<unsigned long, std::vector<T*>*>;
            layer->emplace(new_tuple_count, new std::vector<T*>{tuple});
            if (join_key_to_max_layer_unit_sizes[join_key] < new_tuple_count) {
                join_key_to_max_layer_unit_sizes[join_key] = new_tuple_count;
            }
            join_key_to_layers.emplace(join_key, layer);
            tuple_layer_indices.emplace(*tuple, 0);
        } else {
            auto layer = iter1->second;
            auto iter2 = layer->find(new_tuple_count);
            if (iter2 == layer->end()) {
                tuple_layer_indices.emplace(*tuple, 0);
                layer->emplace(new_tuple_count, new std::vector<T*>{tuple});
                if (join_key_to_max_layer_unit_sizes[join_key] < new_tuple_count) {
                    join_key_to_max_layer_unit_sizes[join_key] = new_tuple_count;
                }
            } else {
                tuple_layer_indices.emplace(*tuple, iter2->second->size());
                iter2->second->emplace_back(tuple);
            }
        }
    }
}

template<typename R, typename T, typename H>
inline void Relation<R,T,H>::update_cnt(unsigned long join_key, unsigned long old_tuple_count, unsigned long new_tuple_count) {
    auto iter1 = cnt.find(join_key);
    if (iter1 == cnt.end()) {
        cnt.emplace(join_key, new_tuple_count);
    } else {
        cnt[join_key] += (new_tuple_count - old_tuple_count);
    }
}

template<typename R, typename T, typename H>
inline void Relation<R,T,H>::update_wcnt(unsigned long join_key) {
    auto iter1 = wcnt.find(join_key);
    if (iter1 == wcnt.end()) {
        wcnt.emplace(join_key, cnt.at(join_key));
        propagate_to_parent_node(join_key, 0, wcnt.at(join_key));
    } else {
        auto old_wcnt = iter1->second;
        if (old_wcnt < cnt.at(join_key)) {
            auto new_wcnt = old_wcnt * 2;
            while (new_wcnt < cnt.at(join_key)) {
                new_wcnt *= 2;
            }
            wcnt.at(join_key) = new_wcnt;
            propagate_to_parent_node(join_key, old_wcnt, new_wcnt);
        }
    }
}

template<typename R, typename T, typename H>
inline void Relation<R,T,H>::propagate_to_parent_node(unsigned long join_key, unsigned long old_count, unsigned long new_count) {
    if (parent_node != nullptr) {
        parent_node->update(id_in_parent_node, join_key, old_count, new_count);
    }
}

template<typename R, typename T, typename H>
inline void Relation<R,T,H>::update_current_tuple_in_batch(JoinTreeBatch<R>* batch, unsigned long base) {
    T* tuple = current_layers->at(current_layer_unit_size)->at(current_layer_index);
    fill_func(batch->buffer, tuple);

    auto children_nodes_size = children_nodes.size();
    if (children_nodes_size > 0) {
        for (int i = 0; i < children_nodes_size; i++) {
            children_join_keys.at(i) = project_to_children_nodes.at(i)(tuple);
        }

        children_size_factors.at(children_nodes_size - 1) = base;
        for (int i = children_nodes_size-2; i >= 0; i--) {
            children_size_factors.at(i) = children_nodes.at(i+1)->get_wcnt(children_join_keys.at(i+1)) * children_size_factors.at(i+1);
        }
    }
}

template<typename R, typename T, typename H>
Relation<R,T,H>::Relation(std::function<void(R*, T*)> fill_func): fill_func(fill_func)  {}

// ------ implementation of Relation<R,T,H> ends ------

#endif