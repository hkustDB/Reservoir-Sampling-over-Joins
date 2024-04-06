cmake_minimum_required(VERSION 3.23)
project(Reservoir)

set(CMAKE_CXX_STANDARD 17)

include_directories(include)
add_executable(Reservoir src/main.cpp src/utils.cpp
        src/line_join/line_join_algorithm.cpp src/line_join/line_join_batch.cpp src/line_join/line_join_attribute.cpp

        src/line_join/line_join_update_time_algorithm.cpp

        src/qx/qx_algorithm.cpp src/qx/qx_batch.cpp
        src/qx/qx_attribute_date_dim_store_sales.cpp src/qx/qx_attribute_store_sales_store_returns.cpp
        src/qx/qx_attribute_store_returns_catalog_sales.cpp src/qx/qx_attribute_catalog_sales_date_dim.cpp

        src/qx/qx_fk_algorithm.cpp src/qx/qx_fk_left_batch.cpp src/qx/qx_fk_right_batch.cpp

        src/qy/qy_algorithm.cpp src/qy/qy_batch.cpp
        src/qy/qy_attribute_store_sales_customer.cpp src/qy/qy_attribute_customer_household_demographics.cpp
        src/qy/qy_attribute_household_demographics_household_demographics.cpp
        src/qy/qy_attribute_household_demographics_customer.cpp

        src/qy/qy_fk_algorithm.cpp src/qy/qy_fk_left_batch.cpp src/qy/qy_fk_right_batch.cpp

        src/qz/qz_algorithm.cpp src/qz/qz_batch.cpp
        src/qz/qz_attribute_item_item.cpp src/qz/qz_attribute_item_store_sales.cpp
        src/qz/qz_attribute_store_sales_customer.cpp src/qz/qz_attribute_customer_household_demographics.cpp
        src/qz/qz_attribute_household_demographics_household_demographics.cpp
        src/qz/qz_attribute_household_demographics_customer.cpp

        src/qz/qz_fk_algorithm.cpp
        src/qz/qz_fk_attribute_item_middle.cpp src/qz/qz_fk_attribute_middle_right.cpp
        src/qz/qz_fk_left_batch.cpp src/qz/qz_fk_middle_batch.cpp src/qz/qz_fk_right_batch.cpp

        src/qz/qz_fk_group_algorithm.cpp
        src/qz/qz_fk_group_attribute_item_middle.cpp src/qz/qz_fk_group_attribute_middle_right.cpp
        src/qz/qz_fk_group_left_batch.cpp src/qz/qz_fk_group_right_batch.cpp

        src/dumbbell/dumbbell_algorithm.cpp
        src/dumbbell/dumbbell_attribute_left_middle.cpp src/dumbbell/dumbbell_attribute_middle_right.cpp
        src/dumbbell/dumbbell_left_batch.cpp src/dumbbell/dumbbell_middle_batch.cpp src/dumbbell/dumbbell_right_batch.cpp

        src/line3_tree/line3_tree_algorithm.cpp

        src/star_join/star_join_algorithm.cpp src/star_join/star_join_batch.cpp

        src/q10/q10_algorithm.cpp

        src/q10/q10_fk_plan1_algorithm.cpp

        src/q10/q10_fk_plan2_algorithm.cpp
        src/q10/q10_fk_plan2_attribute_left_middle.cpp src/q10/q10_fk_plan2_attribute_middle_right.cpp
        src/q10/q10_fk_plan2_left_batch.cpp src/q10/q10_fk_plan2_middle_batch.cpp src/q10/q10_fk_plan2_right_batch.cpp

        src/predicate/predicate_algorithm.cpp
        src/predicate/predicate_baseline_algorithm.cpp)
