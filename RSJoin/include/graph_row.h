#ifndef RESERVOIR_GRAPH_ROW_H
#define RESERVOIR_GRAPH_ROW_H

#include <utility>

struct GraphRow {
    int src;
    int dst;

    GraphRow(int src, int dst) : src(src), dst(dst) {}

    bool operator==(const GraphRow &rhs) const {
        return src == rhs.src &&
               dst == rhs.dst;
    }
};

struct GraphRowHash
{
    std::size_t operator()(const GraphRow& row) const
    {
        using std::size_t;
        using std::hash;

        return ((hash<int>()(row.src))
                ^ (hash<int>()(row.dst)));
    }
};

#endif
