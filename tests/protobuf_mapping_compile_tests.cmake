file(STRINGS "${INCLUDES}" include_paths)
set(flags)
foreach(path IN LISTS include_paths)
    list(APPEND flags "-I${path}")
endforeach()
file(STRINGS "${DEFINITIONS}" definitions)
foreach(definition IN LISTS definitions)
    list(APPEND flags "-D${definition}")
endforeach()
file(MAKE_DIRECTORY "${WORK}")
set(prelude "#include \"${SOURCE}/examples/protobuf_users.hpp\"\n")
set(positive [=[
void invoke(rpc::client &c) {
    example::UserStub<rpc::mapped_protobuf_codec_policy> users{c, example::users_contract(rpc::mapped_protobuf_codec_policy{})};
    example::GetUserRequest request; example::GetUserReply reply; users.GetUser(request, reply);
}
]=])
set(missing_mapping [=[
struct Unmapped { int id; };
void invoke() { rpc::mapped_protobuf_codec_policy::operations<Unmapped>(); }
]=])
set(invalid_wire_type [=[
struct Invalid {};
template <> struct rpc::protobuf_mapping<Invalid> { using message_type = int; };
void invoke() { rpc::mapped_protobuf_codec_policy::operations<Invalid>(); }
]=])
set(wrong_conversion [=[
struct Invalid {};
template <> struct rpc::protobuf_mapping<Invalid> {
    using message_type = example::wire::GetUserRequest;
    static void to_protobuf(Invalid const &, message_type &);
    static bool from_protobuf(message_type const &, Invalid &);
    static std::size_t upper_bound(Invalid const &);
};
void invoke() { rpc::mapped_protobuf_codec_policy::operations<Invalid>(); }
]=])
set(wrong_request [=[
void invoke(rpc::client &c) {
    example::UserStub<rpc::mapped_protobuf_codec_policy> users{c, example::users_contract(rpc::mapped_protobuf_codec_policy{})};
    example::wire::GetUserRequest request; example::GetUserReply reply; users.GetUser(request, reply);
}
]=])
foreach(name positive missing_mapping invalid_wire_type wrong_conversion wrong_request)
    file(WRITE "${WORK}/${name}.cpp" "${prelude}${${name}}")
    execute_process(COMMAND "${COMPILER}" -std=c++14 -fsyntax-only ${flags} "${WORK}/${name}.cpp"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(name STREQUAL "positive")
        if(NOT result EQUAL 0)
            message(FATAL_ERROR "Valid mapped protobuf stub failed to compile:\n${error}")
        endif()
    elseif(result EQUAL 0)
        message(FATAL_ERROR "Invalid protobuf mapping compiled: ${name}")
    endif()
endforeach()
message(STATUS "Mapped protobuf positive and four negative compile checks passed")
