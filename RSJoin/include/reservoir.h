#ifndef RESERVOIR_RESERVOIR_H
#define RESERVOIR_RESERVOIR_H

#include <random>
#include <vector>
#include <cmath>

template<typename T>
class Reservoir {
public:
    explicit Reservoir(int size);
    virtual ~Reservoir();

    void update_w();
    void update_t();
    unsigned long get_t();
    void t_subtract(unsigned long l);
    void replace(T* row);
    void insert(T* row);
    bool is_full();
    std::vector<T*>* get_results();

private:
    int k;
    unsigned long t = 0;
    double w = 1.0;

    std::mt19937 rng;
    std::uniform_real_distribution<> uni;
    std::uniform_int_distribution<> dist;

    std::vector<T*>* results;
};

template<typename T> Reservoir<T>::Reservoir(int size) {
    k = size;
    results = new std::vector<T*>;
    std::random_device rd;
    rng = std::mt19937(rd());
    uni = std::uniform_real_distribution<>(0.0, 1.0);
    dist = std::uniform_int_distribution<>(0, k-1);
}

template<typename T> Reservoir<T>::~Reservoir<T>() {
    for (auto row: *results) {
        delete row;
    }
    results->clear();
    delete results;
}

template<typename T> void Reservoir<T>::update_w() {
    w = w * exp(log(uni(rng))/k);
}

template<typename T> void Reservoir<T>::update_t() {
    t = (unsigned long)floor(log(uni(rng)) / log(1.0-w));
}

template<typename T> unsigned long Reservoir<T>::get_t() {
    return t;
}

template<typename T> void Reservoir<T>::t_subtract(unsigned long l) {
    t = t - l;
}

template<typename T> void Reservoir<T>::insert(T* row) {
    results->emplace_back(row);
}

template<typename T> void Reservoir<T>::replace(T* row) {
    int pick = dist(rng);
    delete results->at(pick);
    results->at(pick) = row;
}

template<typename T> bool Reservoir<T>::is_full() {
    return results->size() >= k;
}

template<typename T> std::vector<T*>* Reservoir<T>::get_results() {
    return results;
}

#endif
