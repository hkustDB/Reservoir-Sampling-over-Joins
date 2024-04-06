#ifndef RESERVOIR_DUMBBELL_ALGORITHM_H
#define RESERVOIR_DUMBBELL_ALGORITHM_H

#include <string>
#include <vector>
#include <chrono>
#include <unordered_map>
#include <unordered_set>

template<typename T>
class Reservoir;
struct DumbbellRow;
struct GraphRow;
struct TriangleRow;
class DumbbellLeftBatch;
class DumbbellMiddleBatch;
class DumbbellRightBatch;
class DumbbellAttributeLeftMiddle;
class DumbbellAttributeMiddleRight;

class DumbbellAlgorithm {
public:
    ~DumbbellAlgorithm();
    void init(const std::string& file, int k);
    Reservoir<DumbbellRow>* run(bool print_info);

private:
    std::string input;
    DumbbellLeftBatch* left_batch = nullptr;
    DumbbellMiddleBatch* middle_batch = nullptr;
    DumbbellRightBatch* right_batch = nullptr;
    Reservoir<DumbbellRow>* reservoir = nullptr;

    // left triangle
    std::unordered_map<int, std::unordered_set<int>*>* a_to_b = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* b_to_a = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* b_to_c = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* c_to_b = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* c_to_a = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* a_to_c = nullptr;

    // right triangle
    std::unordered_map<int, std::unordered_set<int>*>* x_to_y = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* y_to_x = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* y_to_z = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* z_to_y = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* z_to_x = nullptr;
    std::unordered_map<int, std::unordered_set<int>*>* x_to_z = nullptr;

    // middle
    std::unordered_map<int, std::vector<GraphRow*>*>* src_to_row7 = nullptr;
    std::unordered_map<int, std::vector<GraphRow*>*>* dst_to_row7 = nullptr;

    DumbbellAttributeLeftMiddle* dumbbell_attribute_left_middle;
    DumbbellAttributeMiddleRight* dumbbell_attribute_middle_right;

    void process(const std::string& line);
    void insert_left_triangle_row(int a, int b, int c);
    void insert_right_triangle_row(int x, int y, int z);
};


#endif
