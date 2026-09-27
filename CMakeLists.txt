cmake_minimum_required(VERSION 3.20)
project(memshare CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

set(MEMSHARE_SANITIZE "" CACHE STRING "Build with a sanitizer: tsan or asan")
if(MEMSHARE_SANITIZE STREQUAL "tsan")
  add_compile_options(-fsanitize=thread -g)
  add_link_options(-fsanitize=thread)
elseif(MEMSHARE_SANITIZE STREQUAL "asan")
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=address,undefined)
endif()

find_package(Threads REQUIRED)
add_compile_options(-Wall -Wextra)

add_library(memshare
  src/memshare/arbiter.cpp
  src/memshare/cache.cpp
  src/memshare/cleaner.cpp
  src/memshare/epoch.cpp
  src/memshare/hash_table.cpp
  src/memshare/log.cpp
  src/memshare/shadow_queue.cpp)
target_include_directories(memshare PUBLIC src)
target_link_libraries(memshare PUBLIC Threads::Threads)

add_executable(memshare_server src/server/main.cpp)
target_link_libraries(memshare_server PRIVATE memshare)

add_executable(memshare_sim
  src/sim/main.cpp
  src/sim/slab_cache.cpp
  src/sim/trace.cpp)
target_link_libraries(memshare_sim PRIVATE memshare)

enable_testing()
add_executable(memshare_tests tests/tests.cpp)
target_link_libraries(memshare_tests PRIVATE memshare)
add_test(NAME memshare_tests COMMAND memshare_tests)
