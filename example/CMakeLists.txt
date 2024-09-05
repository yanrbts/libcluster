
# 查找所有的源文件
file(GLOB_RECURSE SOURCE_FILES "cluster.c")

link_directories(${CMAKE_CURRENT_SOURCE_DIR}/build/src)

add_executable(cluster ${SOURCE_FILES})
target_link_libraries(cluster cls_shared)