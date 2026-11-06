# tparquet for C++: a small library over the patched Arrow C++ (cpp/arrow/FORK.md), its
# tests, the tparquet-cpp-vectors CLI and libFuzzer targets. See cpp/README.md.
cmake_minimum_required(VERSION 3.20)
project(tparquet_cpp VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(TPARQUET_BUILD_TESTS "Build the tests and the tparquet-cpp-vectors CLI" ON)
option(TPARQUET_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(TPARQUET_FUZZ "Build the libFuzzer targets (clang)" OFF)
# the C stream reader of rust/cread (cargo build --release -p tparquet-cread); the tests
# compare it with the fork, and the package exports it as tparquet::read when given
set(TPARQUET_READ_LIB "" CACHE FILEPATH "Path of libtparquet_read.so")
set(TPARQUET_READ_INCLUDE_DIR "" CACHE PATH "Directory of tparquet_read.h")
set(TPARQUET_TESTDATA "${CMAKE_CURRENT_SOURCE_DIR}/../testdata" CACHE PATH "The conformance vectors")

find_package(Arrow 26.0.1 EXACT REQUIRED CONFIG)
find_package(Parquet 26.0.1 EXACT REQUIRED CONFIG)

if(TPARQUET_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-sanitize-recover=all -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # third-party headers only (see the file)
    add_compile_options(-fsanitize-ignorelist=${CMAKE_CURRENT_SOURCE_DIR}/tests/ubsan-ignorelist.txt)
  endif()
endif()

set(TPARQUET_WARNINGS -Wall -Wextra -Werror)

add_library(tparquet src/read.cc src/write.cc src/canonical.cc)
add_library(tparquet::tparquet ALIAS tparquet)
target_compile_options(tparquet PRIVATE ${TPARQUET_WARNINGS})
target_include_directories(
  tparquet PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(tparquet PUBLIC Parquet::parquet_shared Arrow::arrow_shared)
set_target_properties(tparquet PROPERTIES POSITION_INDEPENDENT_CODE ON)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS tparquet EXPORT tparquetTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT tparquetTargets NAMESPACE tparquet:: DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tparquet)

set(TPARQUET_HAVE_READ OFF)
if(TPARQUET_READ_LIB)
  if(NOT EXISTS "${TPARQUET_READ_LIB}" OR NOT EXISTS "${TPARQUET_READ_INCLUDE_DIR}/tparquet_read.h")
    message(FATAL_ERROR "TPARQUET_READ_LIB / TPARQUET_READ_INCLUDE_DIR do not name libtparquet_read and tparquet_read.h")
  endif()
  set(TPARQUET_HAVE_READ ON)
  add_library(tparquet_read SHARED IMPORTED GLOBAL)
  add_library(tparquet::read ALIAS tparquet_read)
  set_target_properties(tparquet_read PROPERTIES IMPORTED_LOCATION "${TPARQUET_READ_LIB}"
                                                 INTERFACE_INCLUDE_DIRECTORIES "${TPARQUET_READ_INCLUDE_DIR}")
  get_filename_component(TPARQUET_READ_LIB_NAME "${TPARQUET_READ_LIB}" NAME)
  install(FILES "${TPARQUET_READ_LIB}" DESTINATION ${CMAKE_INSTALL_LIBDIR})
  install(FILES "${TPARQUET_READ_INCLUDE_DIR}/tparquet_read.h" DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
endif()

configure_package_config_file(cmake/tparquetConfig.cmake.in
                              "${CMAKE_CURRENT_BINARY_DIR}/tparquetConfig.cmake"
                              INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tparquet
                              PATH_VARS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR)
write_basic_package_version_file("${CMAKE_CURRENT_BINARY_DIR}/tparquetConfigVersion.cmake"
                                 COMPATIBILITY SameMinorVersion)
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/tparquetConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/tparquetConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/tparquet)

if(TPARQUET_BUILD_TESTS)
  add_executable(tparquet-cpp-vectors tools/vectors.cc)
  target_compile_options(tparquet-cpp-vectors PRIVATE ${TPARQUET_WARNINGS})
  target_link_libraries(tparquet-cpp-vectors PRIVATE tparquet)
  install(TARGETS tparquet-cpp-vectors RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})

  enable_testing()
  find_package(GTest REQUIRED)
  add_executable(tparquet-tests
                 tests/canonical_test.cc tests/codec_test.cc tests/conformance_test.cc
                 tests/writer_test.cc tests/properties_test.cc tests/cli_test.cc tests/common.cc
                 tests/table_gen.cc)
  target_compile_options(tparquet-tests PRIVATE ${TPARQUET_WARNINGS})
  target_link_libraries(tparquet-tests PRIVATE tparquet GTest::gtest GTest::gtest_main)
  target_compile_definitions(
    tparquet-tests PRIVATE TPARQUET_TESTDATA="${TPARQUET_TESTDATA}"
                           TPARQUET_VECTORS_CLI="$<TARGET_FILE:tparquet-cpp-vectors>")
  if(TPARQUET_HAVE_READ)
    target_link_libraries(tparquet-tests PRIVATE tparquet::read)
    target_compile_definitions(tparquet-tests PRIVATE TPARQUET_HAVE_READ=1)
  endif()
  include(GoogleTest)
  gtest_discover_tests(tparquet-tests DISCOVERY_TIMEOUT 60)
endif()

if(TPARQUET_FUZZ)
  foreach(target pcodec_decompress_fuzz parquet_read_fuzz)
    add_executable(${target} fuzz/${target}.cc)
    target_compile_options(${target} PRIVATE -fsanitize=fuzzer ${TPARQUET_WARNINGS})
    target_link_options(${target} PRIVATE -fsanitize=fuzzer)
    target_link_libraries(${target} PRIVATE tparquet)
  endforeach()
endif()
