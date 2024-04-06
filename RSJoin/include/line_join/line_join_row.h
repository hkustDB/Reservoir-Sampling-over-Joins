#ifndef RESERVOIR_LINE_JOIN_ROW_H
#define RESERVOIR_LINE_JOIN_ROW_H

#include <vector>

struct GraphRow;

struct LineJoinRow {
    std::vector<GraphRow*> rows;
    int n;

    LineJoinRow(std::vector<GraphRow*> rows, int n): rows(std::move(rows)), n(n) {}
};

#endif
