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
set(prelude "#include \"${SOURCE}/examples/json_users.hpp\"\n")
set(positive [=[
rpc::unary_call invoke(example::UserStub<rpc::json_codec_policy> const &s, example::GetUserRequest const &r,
                       example::GetUserReply &v) {
    return s.GetUser(r, v);
}
]=])
set(wrong_request [=[
void invoke(example::UserStub<rpc::json_codec_policy> const &s) {
    example::RenameUserRequest r; example::GetUserReply v; s.GetUser(r, v);
}
]=])
set(wrong_response [=[
void invoke(example::UserStub<rpc::json_codec_policy> const &s) {
    example::GetUserRequest r; example::RenameUserReply v; s.GetUser(r, v);
}
]=])
set(wrong_handler [=[
struct S { net::task<rpc::status_code> f(rpc::server_context &, example::RenameUserRequest const &, example::GetUserReply &); };
void invoke(S &s) { rpc::bind_method(example::get_user_method(), s, &S::f); }
]=])
set(forged_method [=[
void invoke(rpc::client &c) { rpc::json_bound_method<example::GetUserRequest, example::GetUserReply> f{c, c.bind("users/GetUser")}; }
]=])
set(unmapped [=[
struct Unmapped { int value; };
void invoke() { rpc::json_codec_policy::operations<Unmapped>(); }
]=])
set(wrong_service_call [=[
void invoke(rpc::client &c) {
    auto s = rpc::bind_service(c, example::users_contract(rpc::json_codec_policy{}));
    example::RenameUserRequest r; example::GetUserReply v; s.call<0>(r, v);
}
]=])
set(wrong_service_handler [=[
struct S {
    net::task<rpc::status_code> GetUser(rpc::server_context &, example::RenameUserRequest const &, example::GetUserReply &);
    net::task<rpc::status_code> RenameUser(rpc::server_context &, example::RenameUserRequest const &, example::RenameUserReply &);
};
void invoke(S &s) { example::users_bindings(example::users_contract(rpc::json_codec_policy{}), s); }
]=])
set(unnamed_enum [=[
enum class Mood { calm };
struct WithMood { Mood mood = Mood::calm; };
RPC_JSON_FIELDS(WithMood, mood);
bool invoke(WithMood &m) { return rpc::json_codec<WithMood>::decode({}, m); }
]=])
set(integer_keys [=[
struct WithMap { std::map<int, int> values; };
RPC_JSON_FIELDS(WithMap, values);
std::size_t invoke(WithMap const &m) { return rpc::json_codec<WithMap>::upper_bound(m); }
]=])
foreach(name positive wrong_request wrong_response wrong_handler forged_method unmapped wrong_service_call wrong_service_handler
        unnamed_enum integer_keys)
    file(WRITE "${WORK}/${name}.cpp" "${prelude}${${name}}")
    execute_process(COMMAND "${COMPILER}" -std=c++14 -fsyntax-only ${flags} "${WORK}/${name}.cpp"
        RESULT_VARIABLE result OUTPUT_VARIABLE output ERROR_VARIABLE error)
    if(name STREQUAL "positive")
        if(NOT result EQUAL 0)
            message(FATAL_ERROR "Valid typed stub failed to compile:\n${error}")
        endif()
    elseif(result EQUAL 0)
        message(FATAL_ERROR "Invalid typed contract compiled: ${name}")
    endif()
endforeach()
message(STATUS "Typed method/service stub positive and nine negative compile checks passed")
