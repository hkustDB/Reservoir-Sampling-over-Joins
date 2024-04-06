#ifndef RESERVOIR_STAR_JOIN_ROW_H
#define RESERVOIR_STAR_JOIN_ROW_H

#include <vector>

struct GraphRow;

struct StarJoinRow {
    std::vector<GraphRow*> rows;
    int n;

    StarJoinRow(std::vector<GraphRow*> rows, int n): rows(std::move(rows)), n(n) {}
};

#endif
