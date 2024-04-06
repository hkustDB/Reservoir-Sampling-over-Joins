#include <sstream>
#include <cstring>
#include "../include/reservoir.h"
#include "../include/graph_row.h"
#include "../include/line_join/line_join_algorithm.h"
#include "../include/line_join/line_join_update_time_algorithm.h"
#include "../include/line_join/line_join_row.h"
#include "../include/qx/qx_algorithm.h"
#include "../include/qx/qx_rows.h"
#include "../include/qx/qx_fk_algorithm.h"
#include "../include/qy/qy_algorithm.h"
#include "../include/qy/qy_rows.h"
#include "../include/qy/qy_fk_algorithm.h"
#include "../include/qz/qz_algorithm.h"
#include "../include/qz/qz_rows.h"
#include "../include/qz/qz_fk_algorithm.h"
#include "../include/qz/qz_fk_group_algorithm.h"
#include "../include/dumbbell/dumbbell_algorithm.h"
#include "../include/dumbbell/dumbbell_rows.h"
#include "../include/line3_tree/line3_tree_algorithm.h"
#include "../include/line3_tree/line3_tree_rows.h"
#include "../include/q10/q10_algorithm.h"
#include "../include/q10/q10_fk_plan1_algorithm.h"
#include "../include/q10/q10_fk_plan2_algorithm.h"
#include "../include/q10/q10_rows.h"
#include "../include/star_join/star_join_algorithm.h"
#include "../include/star_join/star_join_row.h"
#include "../include/predicate/predicate_algorithm.h"
#include "../include/predicate/predicate_baseline_algorithm.h"

int run_line_join(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: line_join n file k");
        exit(1);
    }

    int n = std::stoi(argv[2]);
    std::string file = argv[3];
    int k = std::stoi(argv[4]);
    printf("using line join algorithm\n");
    printf("n = %d, file = %s, k = %d\n", n, file.c_str(), k);

    LineJoinAlgorithm algorithm = LineJoinAlgorithm();
    algorithm.init(n, file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_line_join(int argc, char** argv) {
    if (argc < 5) {
        printf("arguments: line_join n file k repeat");
        exit(1);
    }

    int n = std::stoi(argv[2]);
    std::string file = argv[3];
    int k = std::stoi(argv[4]);
    int repeat = std::stoi(argv[5]);
    printf("running line join check\n");
    printf("n = %d, file = %s, k = %d, repeat = %d\n", n, file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        LineJoinAlgorithm algorithm = LineJoinAlgorithm();
        algorithm.init(n, file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->rows.at(0)->src << "," << r->rows.at(0)->dst << ","
                   << r->rows.at(1)->dst << "," << r->rows.at(2)->dst << "," << r->rows.at(3)->dst << "," << r->rows.at(4)->dst << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_line_join_update_time(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: line_join_update_time n file");
        exit(1);
    }

    int n = std::stoi(argv[2]);
    std::string file = argv[3];
    printf("using line join update time algorithm\n");
    printf("n = %d, file = %s\n", n, file.c_str());

    LineJoinUpdateTimeAlgorithm algorithm = LineJoinUpdateTimeAlgorithm();
    algorithm.init(n, file);
    algorithm.run(true);
    return 0;
}

int run_qx(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qx file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qx algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QxAlgorithm algorithm = QxAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qx(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qx_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qx check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QxAlgorithm algorithm = QxAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->date_dim_row->d_date_sk << "," << r->store_sales_row->ss_item_sk_with_ticket_number << ","
                   << r->catalog_sales_row->cs_item_sk << "," << r->catalog_sales_row->cs_order_number << "," << r->date_dim_row2->d_date_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_qx_fk(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qx_fk file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qx_fk algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QxFkAlgorithm algorithm = QxFkAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qx_fk(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qx_fk_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qx_fk check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QxFkAlgorithm algorithm = QxFkAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->date_dim_row->d_date_sk << "," << r->store_sales_row->ss_item_sk_with_ticket_number << ","
                   << r->catalog_sales_row->cs_item_sk << "," << r->catalog_sales_row->cs_order_number << "," << r->date_dim_row2->d_date_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_qy(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qy file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qy algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QyAlgorithm algorithm = QyAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qy(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qy_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qy check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QyAlgorithm algorithm = QyAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->store_sales_row->ss_item_sk_with_ticket_number << "," << r->customer_row->c_customer_sk << ","
                   << r->household_demographics_row->hd_demo_sk << "," << r->household_demographics_row2->hd_demo_sk << ","
                   << r->customer_row2->c_customer_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_qy_fk(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qy_fk file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qy_fk algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QyFkAlgorithm algorithm = QyFkAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qy_fk(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qy_fk_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qy_fk check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QyFkAlgorithm algorithm = QyFkAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->store_sales_row->ss_item_sk_with_ticket_number << "," << r->customer_row->c_customer_sk << ","
                   << r->household_demographics_row->hd_demo_sk << "," << r->household_demographics_row2->hd_demo_sk << ","
                   << r->customer_row2->c_customer_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_qz(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qz file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qz algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QzAlgorithm algorithm = QzAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qz(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qz_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qz check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QzAlgorithm algorithm = QzAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->item_row->i_item_sk << "," << r->item_row2->i_item_sk << ","
                << r->store_sales_row->ss_item_sk_with_ticket_number << "," << r->customer_row->c_customer_sk << ","
                << r->household_demographics_row->hd_demo_sk << "," << r->household_demographics_row2->hd_demo_sk << ","
                << r->customer_row2->c_customer_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_qz_fk(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qz_fk file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qz_fk algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QzFkAlgorithm algorithm = QzFkAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qz_fk(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qz_fk_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qz_fk check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QzFkAlgorithm algorithm = QzFkAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->item_row->i_item_sk << "," << r->item_row2->i_item_sk << ","
                   << r->store_sales_row->ss_item_sk_with_ticket_number << "," << r->customer_row->c_customer_sk << ","
                   << r->household_demographics_row->hd_demo_sk << "," << r->household_demographics_row2->hd_demo_sk << ","
                   << r->customer_row2->c_customer_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_qz_fk_group(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: qz_fk_group file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using qz_fk_group algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    QzFkGroupAlgorithm algorithm = QzFkGroupAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_qz_fk_group(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: qz_fk_group_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running qz_fk_group check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        QzFkGroupAlgorithm algorithm = QzFkGroupAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->item_row->i_item_sk << "," << r->item_row2->i_item_sk << ","
                   << r->store_sales_row->ss_item_sk_with_ticket_number << "," << r->customer_row->c_customer_sk << ","
                   << r->household_demographics_row->hd_demo_sk << "," << r->household_demographics_row2->hd_demo_sk << ","
                   << r->customer_row2->c_customer_sk << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_dumbbell(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: dumbbell file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using dumbbell algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    DumbbellAlgorithm algorithm = DumbbellAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_dumbbell(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: dumbbell_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running dumbbell check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        DumbbellAlgorithm algorithm = DumbbellAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->a << "," << r->b << "," << r->c << "," << r->x << "," << r->y << "," << r->z << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_line3_tree(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: line3_tree file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using line3_tree algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    Line3TreeAlgorithm algorithm = Line3TreeAlgorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_line3_tree(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: line3_tree_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running line3_tree check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        Line3TreeAlgorithm algorithm = Line3TreeAlgorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->row1->src << "," << r->row2->src << "," << r->row3->src << "," << r->row3->dst << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_q10(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: q10 file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using q10 algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    Q10Algorithm algorithm = Q10Algorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_q10(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: q10_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running q10 check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        Q10Algorithm algorithm = Q10Algorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->message_row->id << ","
                   << r->person_row->id << ","
                   << r->city_row->id << ","
                   << r->country_row->id << ","
                   << r->tag_row->id << ","
                   << r->tag_row2->id << ","
                   << r->tag_class_row->id << ","
                   << r->person_row2->id << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_q10_fk_plan1(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: q10_fk_plan1 file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using q10_fk_plan1 algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    Q10FkPlan1Algorithm algorithm = Q10FkPlan1Algorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_q10_fk_plan1(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: q10_fk_plan1_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running q10_fk_plan1 check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        Q10FkPlan1Algorithm algorithm = Q10FkPlan1Algorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->message_person_city_country_row->message_row->id << ","
                << r->message_person_city_country_row->person_row->id << ","
                << r->message_person_city_country_row->city_row->id << ","
                << r->message_person_city_country_row->country_row->id << ","
                << r->message_has_tag_tag_row->tag_row->id << ","
                << r->message_has_tag_tag_tag_class_row->tag_row2->id << ","
                << r->message_has_tag_tag_tag_class_row->tag_class_row->id << ","
                << r->person_knows_person_person_row->person_row2->id << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_q10_fk_plan2(int argc, char** argv) {
    if (argc < 3) {
        printf("arguments: q10_fk_plan2 file k");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    printf("using q10_fk_plan2 algorithm\n");
    printf("file = %s, k = %d\n", file.c_str(), k);

    Q10FkPlan2Algorithm algorithm = Q10FkPlan2Algorithm();
    algorithm.init(file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_q10_fk_plan2(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: q10_fk_plan2_check file k repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    int repeat = std::stoi(argv[4]);
    printf("running q10_fk_plan2 check\n");
    printf("file = %s, k = %d, repeat = %d\n", file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        Q10FkPlan2Algorithm algorithm = Q10FkPlan2Algorithm();
        algorithm.init(file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->middle_row->message_row->id << ","
                   << r->left_row->person_row->id << ","
                   << r->left_row->city_row->id << ","
                   << r->left_row->country_row->id << ","
                   << r->middle_row->tag_row->id << ","
                   << r->right_row->tag_row2->id << ","
                   << r->right_row->tag_class_row->id << ","
                   << r->left_row->person_row2->id << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_star_join(int argc, char** argv) {
    if (argc < 4) {
        printf("arguments: star_join n file k");
        exit(1);
    }

    int n = std::stoi(argv[2]);
    std::string file = argv[3];
    int k = std::stoi(argv[4]);
    printf("using star join algorithm\n");
    printf("n = %d, file = %s, k = %d\n", n, file.c_str(), k);

    StarJoinAlgorithm algorithm = StarJoinAlgorithm();
    algorithm.init(n, file, k);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_star_join(int argc, char** argv) {
    if (argc < 5) {
        printf("arguments: star_join n file k repeat");
        exit(1);
    }

    int n = std::stoi(argv[2]);
    std::string file = argv[3];
    int k = std::stoi(argv[4]);
    int repeat = std::stoi(argv[5]);
    printf("running star join check\n");
    printf("n = %d, file = %s, k = %d, repeat = %d\n", n, file.c_str(), k, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        StarJoinAlgorithm algorithm = StarJoinAlgorithm();
        algorithm.init(n, file, k);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            std::ostringstream buffer;
            buffer << "(" << r->rows.at(0)->src << "," << r->rows.at(0)->dst << ","
                   << r->rows.at(1)->dst << "," << r->rows.at(2)->dst << "," << r->rows.at(3)->dst << "," << r->rows.at(4)->dst << ")";
            std::string row_str = buffer.str();
            auto iter = counters.find(row_str);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(row_str, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_predicate(int argc, char** argv) {
    if (argc < 5) {
        printf("arguments: predicate file k str threshold");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    std::string str = argv[4];
    int threshold = std::stoi(argv[5]);
    printf("using predicate algorithm\n");
    printf("file = %s, k = %d, str = %s, threshold = %d\n", file.c_str(), k, str.c_str(), threshold);

    PredicateAlgorithm algorithm = PredicateAlgorithm();
    algorithm.init(file, k, str, threshold);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_predicate(int argc, char** argv) {
    if (argc < 6) {
        printf("arguments: predicate_check file k str threshold repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    std::string str = argv[4];
    int threshold = std::stoi(argv[5]);
    int repeat = std::stoi(argv[6]);
    printf("running predicate check\n");
    printf("file = %s, k = %d, str = %s, threshold = %d, repeat = %d\n", file.c_str(), k, str.c_str(), threshold, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        PredicateAlgorithm algorithm = PredicateAlgorithm();
        algorithm.init(file, k, str, threshold);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            auto iter = counters.find(*r);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(*r, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int run_predicate_baseline(int argc, char** argv) {
    if (argc < 5) {
        printf("arguments: predicate_baseline file k str threshold");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    std::string str = argv[4];
    int threshold = std::stoi(argv[5]);
    printf("using predicate_baseline algorithm\n");
    printf("file = %s, k = %d, str = %s, threshold = %d\n", file.c_str(), k, str.c_str(), threshold);

    PredicateBaselineAlgorithm algorithm = PredicateBaselineAlgorithm();
    algorithm.init(file, k, str, threshold);
    auto result = algorithm.run(true);

    printf("size = %lu\n", result->get_results()->size());
    return 0;
}

int check_predicate_baseline(int argc, char** argv) {
    if (argc < 6) {
        printf("arguments: predicate_baseline_check file k str threshold repeat");
        exit(1);
    }

    std::string file = argv[2];
    int k = std::stoi(argv[3]);
    std::string str = argv[4];
    int threshold = std::stoi(argv[5]);
    int repeat = std::stoi(argv[6]);
    printf("running predicate_baseline check\n");
    printf("file = %s, k = %d, str = %s, threshold = %d, repeat = %d\n", file.c_str(), k, str.c_str(), threshold, repeat);

    std::unordered_map<std::string, unsigned long> counters;

    for (int i=0; i<repeat; i++) {
        PredicateBaselineAlgorithm algorithm = PredicateBaselineAlgorithm();
        algorithm.init(file, k, str, threshold);
        auto result = algorithm.run(false);
        for (auto r: *result->get_results()) {
            auto iter = counters.find(*r);
            if (iter != counters.end()) {
                iter->second += 1;
            } else {
                counters.emplace(*r, 1);
            }
        }
    }

    int print_elements = 100;
    unsigned long max = 0;
    unsigned long min = repeat;
    printf("counters for first %d elements\n", print_elements);
    int cnt = 0;
    for (const auto& kv: counters) {
        if (cnt <= print_elements)
            printf("%s -> %lu\n", kv.first.c_str(), kv.second);

        if (kv.second > max)
            max = kv.second;

        if (kv.second < min)
            min = kv.second;

        cnt++;
    }
    printf("size = %lu, max = %lu, min = %lu\n", counters.size(), max, min);
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 1) {
        printf("algorithm is missing.");
        exit(1);
    }

    const char *alg = argv[1];
    if (strcmp(alg, "line_join") == 0) {
        return run_line_join(argc, argv);
    } else if (strcmp(alg, "line_join_check") == 0) {
        return check_line_join(argc, argv);
    } else if (strcmp(alg, "line_join_update_time") == 0) {
        return run_line_join_update_time(argc, argv);
    } else if (strcmp(alg, "qx") == 0) {
        return run_qx(argc, argv);
    } else if (strcmp(alg, "qx_check") == 0) {
        return check_qx(argc, argv);
    } else if (strcmp(alg, "qx_fk") == 0) {
        return run_qx_fk(argc, argv);
    } else if (strcmp(alg, "qx_fk_check") == 0) {
        return check_qx_fk(argc, argv);
    } else if (strcmp(alg, "qy") == 0) {
        return run_qy(argc, argv);
    } else if (strcmp(alg, "qy_check") == 0) {
        return check_qy(argc, argv);
    } else if (strcmp(alg, "qy_fk") == 0) {
        return run_qy_fk(argc, argv);
    } else if (strcmp(alg, "qy_fk_check") == 0) {
        return check_qy_fk(argc, argv);
    } else if (strcmp(alg, "qz") == 0) {
        return run_qz(argc, argv);
    } else if (strcmp(alg, "qz_check") == 0) {
        return check_qz(argc, argv);
    } else if (strcmp(alg, "qz_fk") == 0) {
        return run_qz_fk(argc, argv);
    } else if (strcmp(alg, "qz_fk_check") == 0) {
        return check_qz_fk(argc, argv);
    } else if (strcmp(alg, "qz_fk_group") == 0) {
        return run_qz_fk_group(argc, argv);
    } else if (strcmp(alg, "qz_fk_group_check") == 0) {
        return check_qz_fk_group(argc, argv);
    } else if (strcmp(alg, "dumbbell") == 0) {
        return run_dumbbell(argc, argv);
    } else if (strcmp(alg, "dumbbell_check") == 0) {
        return check_dumbbell(argc, argv);
    } else if (strcmp(alg, "line3_tree") == 0) {
        return run_line3_tree(argc, argv);
    } else if (strcmp(alg, "line3_tree_check") == 0) {
        return check_line3_tree(argc, argv);
    } else if (strcmp(alg, "q10") == 0) {
        return run_q10(argc, argv);
    } else if (strcmp(alg, "q10_check") == 0) {
        return check_q10(argc, argv);
    } else if (strcmp(alg, "q10_fk_plan1") == 0) {
        return run_q10_fk_plan1(argc, argv);
    } else if (strcmp(alg, "q10_fk_plan1_check") == 0) {
        return check_q10_fk_plan1(argc, argv);
    } else if (strcmp(alg, "q10_fk_plan2") == 0) {
        return run_q10_fk_plan2(argc, argv);
    } else if (strcmp(alg, "q10_fk_plan2_check") == 0) {
        return check_q10_fk_plan2(argc, argv);
    } else if (strcmp(alg, "star_join") == 0) {
        return run_star_join(argc, argv);
    } else if (strcmp(alg, "star_join_check") == 0) {
        return check_star_join(argc, argv);
    } else if (strcmp(alg, "predicate") == 0) {
        return run_predicate(argc, argv);
    } else if (strcmp(alg, "predicate_check") == 0) {
        return check_predicate(argc, argv);
    } else if (strcmp(alg, "predicate_baseline") == 0) {
        return run_predicate_baseline(argc, argv);
    } else if (strcmp(alg, "predicate_baseline_check") == 0) {
        return check_predicate_baseline(argc, argv);
    }

    return 0;
}
