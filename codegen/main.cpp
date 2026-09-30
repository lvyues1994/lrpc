#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/cpp/names.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/io/printer.h>

#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <vector>

namespace {
namespace pb = google::protobuf;

bool keyword(std::string const &name) {
    static std::set<std::string> const words{
        "alignas", "alignof", "and", "and_eq", "asm", "auto", "bitand", "bitor", "bool", "break",
        "case", "catch", "char", "char16_t", "char32_t", "class", "compl", "const", "constexpr",
        "const_cast", "continue", "decltype", "default", "delete", "do", "double", "dynamic_cast",
        "else", "enum", "explicit", "export", "extern", "false", "float", "for", "friend", "goto",
        "if", "inline", "int", "long", "mutable", "namespace", "new", "noexcept", "not", "not_eq",
        "nullptr", "operator", "or", "or_eq", "private", "protected", "public", "register",
        "reinterpret_cast", "return", "short", "signed", "sizeof", "static", "static_assert",
        "static_cast", "struct", "switch", "template", "this", "thread_local", "throw", "true",
        "try", "typedef", "typeid", "typename", "union", "unsigned", "using", "virtual", "void",
        "volatile", "wchar_t", "while", "xor", "xor_eq"};
    return words.count(name) != 0;
}

std::string identifier(std::string name) { if (keyword(name)) name += '_'; return name; }
std::string message_type(pb::Descriptor const *message) { return pb::compiler::cpp::QualifiedClassName(message); }
std::string wire_name(pb::MethodDescriptor const &method) { return method.service()->full_name() + "/" + method.name(); }

std::vector<std::string> namespaces(std::string const &package) {
    std::vector<std::string> result;
    std::istringstream input{package};
    for (std::string item; std::getline(input, item, '.');) result.push_back(std::move(item));
    return result;
}

std::string scope(pb::FileDescriptor const &file) {
    std::string result = "::";
    for (auto const &part : namespaces(file.package())) result += part + "::";
    return result;
}

void collect_enum(pb::EnumDescriptor const &value, std::set<std::string> &symbols) {
    symbols.insert(pb::compiler::cpp::QualifiedClassName(&value));
    symbols.insert(pb::compiler::cpp::QualifiedClassName(&value) + "_descriptor");
    auto const prefix = scope(*value.file()) + (value.containing_type() ? pb::compiler::cpp::ClassName(&value) + "_" : "");
    for (int i = 0; i < value.value_count(); ++i) symbols.insert(prefix + pb::compiler::cpp::EnumValueName(value.value(i)));
}

void collect_message(pb::Descriptor const &value, std::set<std::string> &symbols) {
    symbols.insert(message_type(&value));
    for (int i = 0; i < value.nested_type_count(); ++i) collect_message(*value.nested_type(i), symbols);
    for (int i = 0; i < value.enum_type_count(); ++i) collect_enum(*value.enum_type(i), symbols);
}

void collect_symbols(pb::FileDescriptor const &file, std::set<std::string> &symbols, std::set<std::string> &visited) {
    if (!visited.insert(file.name()).second) return;
    std::string prefix;
    for (auto const &part : namespaces(file.package())) { prefix += "::" + part; symbols.insert(prefix); }
    for (int i = 0; i < file.message_type_count(); ++i) collect_message(*file.message_type(i), symbols);
    for (int i = 0; i < file.enum_type_count(); ++i) collect_enum(*file.enum_type(i), symbols);
    if (file.options().cc_generic_services())
        for (int i = 0; i < file.service_count(); ++i) symbols.insert(scope(file) + file.service(i)->name());
    for (int i = 0; i < file.dependency_count(); ++i) collect_symbols(*file.dependency(i), symbols, visited);
}

bool streaming(pb::MethodDescriptor const &method) { return method.client_streaming() || method.server_streaming(); }
char const *kind(pb::MethodDescriptor const &method) {
    if (method.client_streaming()) return method.server_streaming() ? "bidirectional" : "client_streaming";
    return method.server_streaming() ? "server_streaming" : "unary";
}
std::string types(pb::MethodDescriptor const &method) {
    return message_type(method.input_type()) + ", " + message_type(method.output_type());
}
std::string open_type(pb::MethodDescriptor const &method) {
    return "::rpc::typed_open_operation<" + types(method) + ", ::rpc::default_codec_policy>";
}
// Declarations; stub definitions repeat them without default arguments.
std::string service_signature(pb::MethodDescriptor const &method) {
    auto const name = identifier(method.name());
    if (streaming(method))
        return "::net::task<::rpc::status_code> " + name + "(::rpc::server_context &context, ::rpc::typed_server_stream<" +
               types(method) + ", ::rpc::default_codec_policy> &stream)";
    return "::net::task<::rpc::status_code> " + name + "(::rpc::server_context &context, " +
           message_type(method.input_type()) + " const &request, " + message_type(method.output_type()) + " &response)";
}
std::string stub_signature(pb::MethodDescriptor const &method, std::string const &scope_prefix, bool defaults) {
    auto const name = scope_prefix + identifier(method.name());
    auto const spec = std::string{"::rpc::call_spec const *spec"} + (defaults ? " = nullptr" : "");
    if (streaming(method)) return open_type(method) + " " + name + "(" + spec + ") const noexcept";
    return "::rpc::unary_call " + name + "(" + message_type(method.input_type()) + " const &request, " +
           message_type(method.output_type()) + " &response, " + spec + ", ::rpc::response_trailer *trailer" +
           (defaults ? " = nullptr" : "") + ") const noexcept";
}

bool validate(pb::FileDescriptor const &file, std::set<std::string> const &native,
              std::set<std::string> &symbols, std::string &error) {
    if (file.name().size() < 6 || file.name().substr(file.name().size() - 6) != ".proto" ||
        file.name().find_first_of("\"\\\r\n") != std::string::npos) {
        error = "rpc: expected a .proto path without quote, backslash or newline"; return false;
    }
    for (auto const &part : namespaces(file.package())) {
        if (keyword(part)) { error = "rpc: C++ keyword in package: " + part; return false; }
    }
    if (file.options().cc_generic_services()) { error = "rpc: cc_generic_services is not supported"; return false; }
    for (int s = 0; s < file.service_count(); ++s) {
        auto const &service = *file.service(s);
        for (auto const *suffix : {"Service", "Stub", "Limits", "_bindings"}) {
            auto const symbol = service.name() + suffix;
            auto const full = scope(file) + symbol;
            if (!symbols.insert(full).second || native.count(full) != 0) {
                error = "rpc: generated C++ symbol collision: " + full; return false;
            }
        }
        std::set<std::string> methods;
        for (int i = 0; i < service.method_count(); ++i) {
            auto const &method = *service.method(i);
            auto const name = identifier(method.name());
            if (!methods.insert(name).second || name == service.name() + "Service" ||
                name == service.name() + "Stub" || name == "lrpc_target_" || name == "lrpc_methods_") {
                error = "rpc: generated C++ method collision: " + method.full_name(); return false;
            }
        }
    }
    return true;
}

void write_header(pb::ServiceDescriptor const &service, std::ostream &out) {
    auto const &name = service.name();
    auto const count = service.method_count();
    out << "struct " << name << "Service {\n    virtual ~" << name << "Service() = default;\n";
    for (int i = 0; i < count; ++i) out << "    virtual " << service_signature(*service.method(i)) << " = 0;\n";
    out << "};\n\nstruct " << name << "Limits {\n";
    for (int i = 0; i < count; ++i) out << "    ::rpc::method_limits " << identifier(service.method(i)->name()) << "{};\n";
    out << "};\n\nstd::vector<::rpc::method_binding> " << name << "_bindings(" << name << "Service &service, " << name
        << "Limits const &limits = {});\n\n"
        << "// Calls through a client or a channel, which must outlive the stub.\n"
        << "class " << name << "Stub {\npublic:\n    explicit " << name << "Stub(::rpc::call_target"
        << (count == 0 ? ") noexcept {}\n" : " target);\n");
    for (int i = 0; i < count; ++i) out << "    " << stub_signature(*service.method(i), "", true) << ";\n";
    if (count != 0)
        out << "private:\n    ::rpc::call_target lrpc_target_;\n    std::array<::rpc::method_ref, " << count
            << "> lrpc_methods_;\n";
    out << "};\n\n";
}

void write_bindings(pb::ServiceDescriptor const &service, std::ostream &out) {
    auto const &name = service.name();
    out << "std::vector<::rpc::method_binding> " << name << "_bindings(" << name << "Service &service, " << name
        << "Limits const &limits) {\n"
        << "    std::vector<::rpc::method_binding> result;\n    result.reserve(" << service.method_count() << ");\n";
    if (service.method_count() == 0) out << "    (void)service; (void)limits;\n";
    for (int i = 0; i < service.method_count(); ++i) {
        auto const &method = *service.method(i);
        out << "    result.push_back(::rpc::" << (streaming(method) ? "bind_stream_method" : "bind_method")
            << "(::rpc::method<" << types(method) << ">{\"" << wire_name(method) << "\"}, ";
        if (streaming(method)) out << "::rpc::method_kind::" << kind(method) << ", ";
        out << "service, &" << name << "Service::" << identifier(method.name()) << ", limits."
            << identifier(method.name()) << "));\n";
    }
    out << "    return result;\n}\n\n";
}

void write_stubs(pb::ServiceDescriptor const &service, std::ostream &out) {
    auto const &name = service.name();
    auto const count = service.method_count();
    if (count == 0) return;
    out << name << "Stub::" << name << "Stub(::rpc::call_target target)\n    : lrpc_target_(target), lrpc_methods_{{";
    for (int i = 0; i < count; ++i) out << (i == 0 ? "" : ", ") << "target.bind(\"" << wire_name(*service.method(i)) << "\")";
    out << "}} {}\n\n";
    for (int i = 0; i < count; ++i) {
        auto const &method = *service.method(i);
        out << stub_signature(method, name + "Stub::", false) << " {\n";
        if (streaming(method))
            out << "    return " << open_type(method) << "{lrpc_target_.open(lrpc_methods_[" << i
                << "], ::rpc::method_kind::" << kind(method) << ", spec)};\n}\n\n";
        else
            out << "    return lrpc_target_.call(lrpc_methods_[" << i << "], ::rpc::encoded<"
                << message_type(method.input_type()) << ">(request), ::rpc::decoded<"
                << message_type(method.output_type()) << ">(response), spec, trailer);\n}\n\n";
    }
}

struct rpc_generator final : pb::compiler::CodeGenerator {
    std::uint64_t GetSupportedFeatures() const override { return FEATURE_PROTO3_OPTIONAL; }
    bool GenerateAll(std::vector<pb::FileDescriptor const *> const &files, std::string const &parameter,
                     pb::compiler::GeneratorContext *context, std::string *error) const override {
        std::set<std::string> native, visited, generated;
        for (auto const *file : files) collect_symbols(*file, native, visited);
        for (auto const *file : files) if (!validate(*file, native, generated, *error)) return false;
        return pb::compiler::CodeGenerator::GenerateAll(files, parameter, context, error);
    }
    bool Generate(pb::FileDescriptor const *file, std::string const &parameter,
                  pb::compiler::GeneratorContext *context, std::string *error) const override {
        if (!parameter.empty()) { *error = "rpc: generator parameters are not supported"; return false; }
        std::set<std::string> native, visited, generated;
        collect_symbols(*file, native, visited);
        if (!validate(*file, native, generated, *error)) return false;
        auto const base = file->name().substr(0, file->name().size() - 6);
        std::ostringstream header, source;
        header << "// Generated by protoc-gen-rpc.\n#pragma once\n#include <rpc/protobuf.hpp>\n\n#include <array>\n#include <vector>\n\n#include \""
               << base << ".pb.h\"\n\n";
        source << "// Generated by protoc-gen-rpc.\n#include \"" << base << ".rpc.hpp\"\n\n";
        for (auto const &part : namespaces(file->package())) { header << "namespace " << part << " {\n"; source << "namespace " << part << " {\n"; }
        for (int i = 0; i < file->service_count(); ++i) {
            auto const &service = *file->service(i);
            write_header(service, header);
            write_bindings(service, source);
            write_stubs(service, source);
        }
        auto const parts = namespaces(file->package());
        for (auto it = parts.rbegin(); it != parts.rend(); ++it) { header << "} // namespace " << *it << '\n'; source << "} // namespace " << *it << '\n'; }
        for (auto const &item : {std::make_pair(base + ".rpc.hpp", header.str()), std::make_pair(base + ".rpc.cpp", source.str())}) {
            std::unique_ptr<pb::io::ZeroCopyOutputStream> stream{context->Open(item.first)};
            pb::io::Printer output{stream.get(), '$'};
            output.PrintRaw(item.second);
            if (output.failed()) { *error = "rpc: failed to write " + item.first; return false; }
        }
        return true;
    }
};
} // namespace

int main(int argc, char **argv) {
    rpc_generator generator;
    return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
