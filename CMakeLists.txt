cmake_minimum_required(VERSION 3.20)

project(rtcvpn LANGUAGES C CXX)

# ============================================================
# C++ 标准
# ============================================================

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ============================================================
# 平台
# ============================================================

if(MSVC)

    message(STATUS "Compiler: MSVC")

    add_compile_options(
        /W4
        /utf-8
    )
    set(MY_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/ThirdParty/Windows")
elseif(MINGW)

    message(STATUS "Compiler: MinGW")

    add_compile_options(
        -Wall
    )
    set(MY_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/ThirdParty/Windows")
elseif(UNIX)

    message(STATUS "Compiler: Unix")

    add_compile_options(
        -Wall
    )
    set(MY_INSTALL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/ThirdParty/Linux")

else()

    message(FATAL_ERROR "Unsupported compiler/platform")

endif()

if(NOT EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/ThirdParty/asio-1.38.2)
    execute_process(COMMAND ${CMAKE_COMMAND} -E tar xzf "${CMAKE_CURRENT_SOURCE_DIR}/downloads/asio-1.38.2.zip" WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/ThirdParty/)
endif()
message(STATUS "uncompress asio-1.38.2.zip")

set(DOWNLOAD_DIR "${CMAKE_CURRENT_SOURCE_DIR}/downloads")
set(BUILD_DIR "${CMAKE_BINARY_DIR}/builds")

file(MAKE_DIRECTORY ${DOWNLOAD_DIR})
file(MAKE_DIRECTORY ${BUILD_DIR})

include(FetchContent)

FetchContent_Declare(
    libdatachannel
    GIT_REPOSITORY https://github.com/paullouisageneau/libdatachannel.git
    GIT_TAG v0.24.5
)

FetchContent_MakeAvailable(libdatachannel)

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.12.0
)

FetchContent_MakeAvailable(nlohmann_json)



add_subdirectory(Src/client)
add_subdirectory(Src/server)