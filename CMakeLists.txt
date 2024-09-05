# Copyright (c) 2024-2024, yanruibinghxu@gmail.com
 # All rights reserved.
 #
 # Redistribution and use in source and binary forms, with or without
 # modification, are permitted provided that the following conditions are met:
 #
 #   # Redistributions of source code must retain the above copyright notice,
 #     this list of conditions and the following disclaimer.
 #   # Redistributions in binary form must reproduce the above copyright
 #     notice, this list of conditions and the following disclaimer in the
 #     documentation and/or other materials provided with the distribution.
 #   # Neither the name of Redis nor the names of its contributors may be used
 #     to endorse or promote products derived from this software without
 #     specific prior written permission.
 #
 # THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 # AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 # IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 # ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 # LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 # CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 # SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 # INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 # CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 # ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 # POSSIBILITY OF SUCH DAMAGE.
 #
 cmake_minimum_required(VERSION 3.9)
 project(libalgorithm LANGUAGES C)
 set(VERSION 0.0.18)
 set(CMAKE_C_STANDARD 99)
 enable_language(C)
 enable_testing()

 set(CMAKE_C_FLAGS_DEBUG "-g -O0")  # -g 生成调试信息, -O0 关闭优化
 set(CMAKE_BUILD_TYPE Debug)
 add_definitions (-DCMAKE -DVERSION=\"${VERSION}\")
 # 添加 include 目录
 include_directories(${CMAKE_SOURCE_DIR}/include)
 include_directories(${CMAKE_SOURCE_DIR}/src)
#  include_directories(${CMAKE_SOURCE_DIR}/test/Unity)
 
 add_subdirectory(src)
 add_subdirectory(example)
 