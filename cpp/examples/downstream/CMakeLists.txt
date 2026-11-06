# A project outside this repository's build: finds the installed tparquet package (and,
# through it, the patched Arrow and Parquet) with find_package. `make -C cpp downstream`.
cmake_minimum_required(VERSION 3.20)
project(tparquet_downstream LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(tparquet 0.1 REQUIRED CONFIG)

add_executable(downstream main.cc)
target_link_libraries(downstream PRIVATE tparquet::tparquet)
if(TARGET tparquet::read)
  # the C stream reader, used from C
  add_executable(downstream-c main.c)
  target_link_libraries(downstream-c PRIVATE tparquet::read)
endif()
