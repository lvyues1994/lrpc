# lrpc_test(name [BACKENDS] [TIMEOUT seconds] LIBRARIES libs...): one
# executable from name.cpp, run once or once per net backend.
function(lrpc_test name)
    cmake_parse_arguments(ARG "BACKENDS" "TIMEOUT" "LIBRARIES;SOURCES" ${ARGN})
    if(NOT ARG_TIMEOUT)
        set(ARG_TIMEOUT 60)
    endif()
    add_executable(${name}_tests ${name}_tests.cpp ${ARG_SOURCES})
    target_link_libraries(${name}_tests PRIVATE ${ARG_LIBRARIES})
    lrpc_configure_target(${name}_tests)
    if(ARG_BACKENDS)
        foreach(backend epoll poll select io_uring)
            add_test(NAME ${name}_${backend} COMMAND ${name}_tests ${backend})
            set_tests_properties(${name}_${backend} PROPERTIES TIMEOUT ${ARG_TIMEOUT} SKIP_RETURN_CODE 77)
        endforeach()
    else()
        add_test(NAME ${name} COMMAND ${name}_tests)
        set_tests_properties(${name} PROPERTIES TIMEOUT ${ARG_TIMEOUT})
    endif()
endfunction()

if(LRPC_BUILD_TESTS)
    lrpc_test(wire LIBRARIES lrpc::wire)
    lrpc_test(compressor LIBRARIES lrpc::compression)

    if(LRPC_BUILD_RPC)
        lrpc_test(core LIBRARIES lrpc::rpc)
        lrpc_test(unary BACKENDS LIBRARIES lrpc::rpc)
        lrpc_test(stream BACKENDS LIBRARIES lrpc::rpc)
        lrpc_test(channel BACKENDS LIBRARIES lrpc::rpc)
        lrpc_test(typed BACKENDS LIBRARIES lrpc::rpc)
        if(LRPC_ENABLE_COMPRESSION)
            lrpc_test(compression BACKENDS TIMEOUT 30 LIBRARIES lrpc::rpc lrpc::compression)
        endif()
        if(LRPC_BUILD_ALLOCATION_TESTS AND NOT CMAKE_CXX_FLAGS MATCHES "fsanitize=thread")
            lrpc_test(gate BACKENDS LIBRARIES lrpc::rpc)
        endif()
    endif()

    # Compiles each snippet of script: the first must build, the others not.
    function(lrpc_compile_test name script target)
        if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            return()
        endif()
        file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${name}-includes.txt"
            CONTENT "$<JOIN:$<TARGET_PROPERTY:${target},INCLUDE_DIRECTORIES>,\n>")
        file(GENERATE OUTPUT "${CMAKE_CURRENT_BINARY_DIR}/${name}-definitions.txt"
            CONTENT "$<JOIN:$<TARGET_PROPERTY:${target},COMPILE_DEFINITIONS>,\n>")
        add_test(NAME ${name} COMMAND "${CMAKE_COMMAND}"
            "-DCOMPILER=${CMAKE_CXX_COMPILER}" "-DSOURCE=${PROJECT_SOURCE_DIR}"
            "-DINCLUDES=${CMAKE_CURRENT_BINARY_DIR}/${name}-includes.txt"
            "-DDEFINITIONS=${CMAKE_CURRENT_BINARY_DIR}/${name}-definitions.txt"
            "-DWORK=${CMAKE_CURRENT_BINARY_DIR}/${name}"
            -P "${CMAKE_CURRENT_SOURCE_DIR}/${script}")
        set_tests_properties(${name} PROPERTIES TIMEOUT 30)
    endfunction()

    if(LRPC_BUILD_JSON)
        lrpc_test(json TIMEOUT 20 LIBRARIES lrpc::json)
        lrpc_test(json_rpc TIMEOUT 20 LIBRARIES lrpc::json)
        lrpc_compile_test(json_compile json_compile_tests.cmake json_tests)
    endif()

    if(LRPC_BUILD_PROTOBUF AND LRPC_BUILD_CODEGEN)
        lrpc_generate_cpp(test_protos IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto"
            PROTOS payload.proto echo.proto nested/lite.proto)
        lrpc_test(protobuf BACKENDS TIMEOUT 30 LIBRARIES test_protos)
        find_package(Python3 COMPONENTS Interpreter REQUIRED)
        add_test(NAME codegen COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_SOURCE_DIR}/codegen_tests.py"
            "$<TARGET_FILE:protobuf::protoc>" "$<TARGET_FILE:protoc-gen-rpc>" "${CMAKE_CURRENT_SOURCE_DIR}/proto")
        set_tests_properties(codegen PROPERTIES TIMEOUT 30)
        if(LRPC_BUILD_JSON)
            lrpc_generate_cpp(mapped_users_protocol IMPORT_DIR "${PROJECT_SOURCE_DIR}/examples/proto" PROTOS users.proto)
            lrpc_test(service_rpc TIMEOUT 20 LIBRARIES mapped_users_protocol lrpc::json)
            lrpc_compile_test(protobuf_mapping_compile protobuf_mapping_compile_tests.cmake service_rpc_tests)
        endif()
    endif()
endif()

if(LRPC_BUILD_FUZZER)
    add_executable(wire_fuzz wire_fuzz.cpp)
    target_link_libraries(wire_fuzz PRIVATE lrpc::wire)
    lrpc_configure_target(wire_fuzz)
    target_compile_options(wire_fuzz PRIVATE -fsanitize=fuzzer)
    target_link_options(wire_fuzz PRIVATE -fsanitize=fuzzer)
endif()
