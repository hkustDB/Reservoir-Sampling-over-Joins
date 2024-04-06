#ifndef RESERVOIR_LINE3_TREE_ROWS_H
#define RESERVOIR_LINE3_TREE_ROWS_H

#include <utility>
#include "../graph_row.h"

struct Line3TreeGraphRow {
    unsigned long src;
    unsigned long dst;

    Line3TreeGraphRow(unsigned long src, unsigned long dst) : src(src), dst(dst) {}

    bool operator==(const Line3TreeGraphRow &rhs) const {
        return src == rhs.src &&
               dst == rhs.dst;
    }
};

struct Line3TreeGraphRowHash {
    std::size_t operator()(const Line3TreeGraphRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<unsigned long>()(row.src)) ^ (hash<unsigned long>()(row.dst));
    }
};

struct Line3TreeRow {
    Line3TreeGraphRow* row1;
    Line3TreeGraphRow* row2;
    Line3TreeGraphRow* row3;
};



#endif
