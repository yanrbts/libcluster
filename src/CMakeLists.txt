# 查找所有的源文件
# file(GLOB_RECURSE SOURCE_FILES "*.c")

set(SOURCE_FILES
    ae.c
    anet.c
    cls_adlist.c
    cls_dict.c
    cls_main.c
    cls_malloc.c
    cls_rax.c
    cls_sds.c 
    cls_util.c
    sha1.c
)

# 生成共享库 libalgorithm.so
add_library(cls_shared SHARED ${SOURCE_FILES})

# 生成静态库 libalgorithm.a
add_library(cls_static STATIC ${SOURCE_FILES})

# 设置库的版本号和 SOVERSION
set_target_properties(cls_shared PROPERTIES VERSION ${VERSION})
set_target_properties(cls_static PROPERTIES VERSION ${VERSION})

# 设置库的输出名称
set_target_properties(cls_shared PROPERTIES OUTPUT_NAME "cls")
set_target_properties(cls_static PROPERTIES OUTPUT_NAME "cls")

target_link_libraries(cls_shared PRIVATE m)
target_link_libraries(cls_static PRIVATE m)

# 指定库的安装路径
install(TARGETS cls_shared cls_static
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)