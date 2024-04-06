#ifndef RESERVOIR_STAR_JOIN_BATCH_H
#define RESERVOIR_STAR_JOIN_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
struct GraphRow;
struct StarJoinRow;

class StarJoinBatch {
public:
    StarJoinBatch(std::unordered_map<int, std::vector<std::vector<GraphRow*>*>*>* relation_indices, int n);
    ~StarJoinBatch();
    void fill(Reservoir<StarJoinRow>* reservoir);
    void set_row(GraphRow* row, int id);

private:
    std::unordered_map<int, std::vector<std::vector<GraphRow*>*>*>* relation_indices;
    std::vector<std::vector<GraphRow*>*>* relations;
    int n;
    int sid;

    unsigned long size;
    unsigned long position;
    std::vector<GraphRow*> rows;
    StarJoinRow* next_row;
    unsigned long* indices;
    unsigned long* unit_sizes;

    void init_real_iterator();
    bool has_next_real();
    StarJoinRow* next_real();
    void compute_next_real();

    void fill_with_skip(Reservoir<StarJoinRow>* reservoir);
};


#endif
