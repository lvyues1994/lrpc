# Generate protobuf messages and RPC adapters from one dependency graph.
# IMPORT_DIR is a single logical proto root; relative directories are preserved.
function(lrpc_generate_cpp target)
    cmake_parse_arguments(ARG "" "IMPORT_DIR;OUTPUT_DIR" "PROTOS" ${ARGN})
    if(ARG_UNPARSED_ARGUMENTS OR NOT ARG_PROTOS OR NOT ARG_IMPORT_DIR)
        message(FATAL_ERROR "lrpc_generate_cpp(target IMPORT_DIR root PROTOS files... [OUTPUT_DIR dir])")
    endif()
    if(NOT TARGET lrpc::protobuf OR NOT TARGET protoc-gen-rpc OR NOT TARGET protobuf::protoc)
        message(FATAL_ERROR "lrpc_generate_cpp requires LRPC_BUILD_PROTOBUF and LRPC_BUILD_CODEGEN")
    endif()
    get_filename_component(root "${ARG_IMPORT_DIR}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}")
    if(NOT ARG_OUTPUT_DIR)
        set(ARG_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/${target}-generated")
    endif()
    get_filename_component(output "${ARG_OUTPUT_DIR}" ABSOLUTE BASE_DIR "${CMAKE_CURRENT_BINARY_DIR}")
    set(outputs)
    set(inputs)
    foreach(proto IN LISTS ARG_PROTOS)
        get_filename_component(input "${proto}" ABSOLUTE BASE_DIR "${root}")
        file(RELATIVE_PATH relative "${root}" "${input}")
        if(relative MATCHES "^\\.\\./" OR NOT relative MATCHES "\\.proto$")
            message(FATAL_ERROR "Proto must be inside IMPORT_DIR and end in .proto: ${proto}")
        endif()
        string(REGEX REPLACE "\\.proto$" "" base "${relative}")
        list(APPEND inputs "${input}")
        list(APPEND outputs "${output}/${base}.pb.cc" "${output}/${base}.pb.h"
             "${output}/${base}.rpc.cpp" "${output}/${base}.rpc.hpp")
    endforeach()
    # Track imported schemas too, including imports not passed as generation roots.
    file(GLOB_RECURSE imports CONFIGURE_DEPENDS "${root}/*.proto")
    add_custom_command(OUTPUT ${outputs}
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${output}"
        COMMAND protobuf::protoc "--proto_path=${root}" "--cpp_out=${output}" "--rpc_out=${output}"
            "--plugin=protoc-gen-rpc=$<TARGET_FILE:protoc-gen-rpc>" ${inputs}
        DEPENDS ${inputs} ${imports} protobuf::protoc protoc-gen-rpc
        VERBATIM)
    add_library(${target} STATIC ${outputs})
    target_include_directories(${target} PUBLIC "${output}")
    target_link_libraries(${target} PUBLIC lrpc::protobuf)
    set_target_properties(${target} PROPERTIES CXX_STANDARD 14 CXX_STANDARD_REQUIRED YES CXX_EXTENSIONS NO)
endfunction()
