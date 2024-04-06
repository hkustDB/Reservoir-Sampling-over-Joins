#ifndef RESERVOIR_LINE_JOIN_BATCH_H
#define RESERVOIR_LINE_JOIN_BATCH_H

#include <vector>
#include <unordered_map>

template<typename T>
class Reservoir;
class LineJoinAttribute;
struct GraphRow;
struct LineJoinRow;

class LineJoinBatch {
public:
    LineJoinBatch(std::vector<LineJoinAttribute*>* line_join_attributes, int n, GraphRow *row, int sid);
    ~LineJoinBatch();
    void fill(Reservoir<LineJoinRow>* reservoir);

private:
    std::vector<LineJoinAttribute*>* line_join_attributes;
    int n;
    int sid;

    unsigned long size;
    unsigned long position;
    unsigned long position_after_next_real;
    std::vector<GraphRow*> rows;
    LineJoinRow* next_row;

    unsigned long* layer_unit_sizes;
    unsigned long* layer_indices;
    std::vector<std::unordered_map<unsigned long, std::vector<GraphRow*>*>*> layers;

    void init_real_iterator();
    bool has_next_real();
    LineJoinRow* next_real();
    void compute_next_real();

    void search_layer_unit_size(int i);
    void reset_layer_unit_size(int i);
    void next_layer_unit_size(int i);
    void reset_layer_index(int i);
    void update_row(int i);
    void update_layer(int i, bool from_left);

    void fill_with_skip(Reservoir<LineJoinRow>* reservoir);
};


#endif
