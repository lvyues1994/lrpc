include_guard(GLOBAL)
# Share the embedding application's provider; never download during configure.
if(NOT TARGET protobuf::libprotobuf)
    set(lrpc_new_protobuf_targets)
    foreach(target protobuf::libprotobuf protobuf::libprotobuf-lite protobuf::libprotoc protobuf::protoc)
        if(NOT TARGET ${target})
            list(APPEND lrpc_new_protobuf_targets ${target})
        endif()
    endforeach()
    find_package(Protobuf 3.21 REQUIRED)
    # The generation helper is also called by parents after add_subdirectory.
    # Promote only targets created here; respect an embedding provider's scope.
    foreach(target IN LISTS lrpc_new_protobuf_targets)
        if(TARGET ${target})
            set_property(TARGET ${target} PROPERTY IMPORTED_GLOBAL TRUE)
        endif()
    endforeach()
    unset(lrpc_new_protobuf_targets)
endif()
