cmake_minimum_required(VERSION 3.20)

project(client LANGUAGES C CXX)

add_executable(client
    main.cpp
)

target_include_directories(client
    PRIVATE
    ${CMAKE_SOURCE_DIR}/ThirdParty/asio-1.38.2/include
    ${CMAKE_SOURCE_DIR}/Src/common/include)

if(UNIX)

elseif(WIN32)
    set(windows_libs ws2_32 mswsock)
endif()


target_link_libraries(client
    PRIVATE
    datachannel
    nlohmann_json::nlohmann_json
    plog::plog

    ${windows_libs}
)

# main.c 使用 C++ 库，确保最终链接使用 C++ linker
set_target_properties(client PROPERTIES
    LINKER_LANGUAGE CXX
)