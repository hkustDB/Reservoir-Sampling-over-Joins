#ifndef RESERVOIR_DUMBBELL_ROWS_H
#define RESERVOIR_DUMBBELL_ROWS_H

#include <string>
#include <utility>
#include "../graph_row.h"

struct DumbbellRow {
    int a;
    int b;
    int c;
    int x;
    int y;
    int z;

    DumbbellRow(int a, int b, int c, int x, int y, int z) : a(a), b(b), c(c), x(x), y(y), z(z) {}
};

struct TriangleRow {
    int a;
    int b;
    int c;

    TriangleRow(int a, int b, int c) : a(a), b(b), c(c) {}

    bool operator==(const TriangleRow &rhs) const {
        return a == rhs.a && b == rhs.b && c == rhs.c;
    }
};

struct TriangleRowHash {
    std::size_t operator()(const TriangleRow& row) const
    {
        using std::size_t;
        using std::hash;

        return (hash<int>()(row.a)) ^ (hash<int>()(row.b)) ^ (hash<int>()(row.c));
    }
};

#endif
