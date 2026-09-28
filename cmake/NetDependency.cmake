# Respect a provider supplied by the embedding application.
if(TARGET net::net)
    return()
endif()

set(LRPC_NET_SOURCE_DIR "" CACHE PATH "Path to the local net source checkout")
if(NOT LRPC_NET_SOURCE_DIR)
    find_package(net CONFIG REQUIRED)
    return()
endif()
if(NOT EXISTS "${LRPC_NET_SOURCE_DIR}/CMakeLists.txt")
    message(FATAL_ERROR "LRPC_NET_SOURCE_DIR must point to a net source checkout")
endif()

# Prevent net's fallback from downloading an unpinned co2 branch. Accept an
# existing target/package or require local sources before adding net.
if(NOT TARGET co2::co2)
    find_package(co2 CONFIG QUIET)
    if(NOT TARGET co2::co2)
        set(NET_CO2_DIR "${LRPC_NET_SOURCE_DIR}/../../coro/coro"
            CACHE PATH "Path to the local co2 source checkout")
        if(NOT EXISTS "${NET_CO2_DIR}/CMakeLists.txt")
            message(FATAL_ERROR "Provide co2::co2, an installed co2 package, or NET_CO2_DIR; automatic downloads are disabled")
        endif()
    endif()
endif()

set(NET_BUILD_TESTS OFF)
set(NET_BUILD_EXAMPLES OFF)
set(NET_BUILD_BENCHMARKS OFF)
if(NOT DEFINED NET_TLS_PROVIDER)
    set(NET_TLS_PROVIDER OFF CACHE STRING "TLS provider for net")
endif()
add_subdirectory("${LRPC_NET_SOURCE_DIR}" "${CMAKE_CURRENT_BINARY_DIR}/net" EXCLUDE_FROM_ALL)
