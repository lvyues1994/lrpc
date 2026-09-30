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
std::string signature(pb::MethodDescriptor const &method, bool service) {
    auto const request = message_type(method.input_type());
    auto const response = message_type(method.output_type());
    if (streaming(method)) {
        if (service) return "::net::task<::rpc::status_code> " + identifier(method.name()) +
            "(::rpc::server_context &context, ::rpc::server_stream<" + request + ", " + response + "> &stream)";
        return "::net::task<::rpc::stream_call<" + request + ", " + response + ">> " + identifier(method.name()) + "(::rpc::call_options options = {})";
    }
    return std::string{"::net::task<::rpc::"} + (service ? "status_code> " : "call_result> ") +
        identifier(method.name()) + "(" + (service ? "::rpc::server_context &context, " : "") +
        request + " const &request, " + response + " &response" +
        (service ? ")" : ", ::rpc::call_options options = {})");
}

std::string semantics(pb::MethodDescriptor const &method) {
    switch (method.options().idempotency_level()) {
    case pb::MethodOptions::NO_SIDE_EFFECTS: return "no_side_effects";
    case pb::MethodOptions::IDEMPOTENT: return "idempotent";
    default: return "unknown";
    }
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
        for (auto const *suffix : {"Service", "Stub", "Limits", "_service_descriptor", "_bindings"}) {
            auto const symbol = service.name() + suffix;
            auto const full = scope(file) + symbol;
            if (!symbols.insert(full).second || native.count(full) != 0) {
                error = "rpc: generated C++ symbol collision: " + full; return false;
            }
        }
        auto const registration = scope(file) + "add_" + service.name() + "_service";
        if (!symbols.insert(registration).second || native.count(registration) != 0) {
            error = "rpc: generated C++ symbol collision: " + registration; return false;
        }
        std::set<std::string> methods;
        for (int i = 0; i < service.method_count(); ++i) {
            auto const &method = *service.method(i);
            auto const name = identifier(method.name());
            if (!methods.insert(name).second || name == service.name() + "Service" ||
                name == service.name() + "Stub" || name == "lrpc_channel_" || name == "lrpc_methods_") {
                error = "rpc: generated C++ method collision: " + method.full_name(); return false;
            }
        }
    }
    return true;
}

void write_header(pb::ServiceDescriptor const &service, std::ostream &out) {
    auto const &name = service.name();
    out << "struct " << name << "Service {\n    virtual ~" << name << "Service() = default;\n";
    for (int i = 0; i < service.method_count(); ++i) out << "    virtual " << signature(*service.method(i), true) << " = 0;\n";
    out << "};\n\nstruct " << name << "Limits {\n";
    for (int i = 0; i < service.method_count(); ++i)
        out << "    ::rpc::method_limits " << identifier(service.method(i)->name()) << "{};\n";
    out << "};\n\n::rpc::service_descriptor const &" << name << "_service_descriptor();\n"
        << "std::vector<::rpc::method_binding> " << name << "_bindings(" << name << "Service &service, " << name << "Limits const &limits);\n\n"
        << "::rpc::server_builder &add_" << name << "_service(::rpc::server_builder &builder, " << name << "Service &service, "
        << name << "Limits const &limits);\n\n"
        << "class " << name << "Stub {\npublic:\n    explicit " << name << "Stub(::rpc::client &"
        << (service.method_count() == 0 ? ") noexcept {}\n" : "channel);\n");
    for (int i = 0; i < service.method_count(); ++i) out << "    " << signature(*service.method(i), false) << ";\n";
    if (service.method_count() != 0) out << "private:\n    ::rpc::client *lrpc_channel_;\n    std::vector<::rpc::method_handle> lrpc_methods_;\n";
    out << "};\n\n";
}

void write_descriptors(pb::ServiceDescriptor const &service, std::ostream &out) {
    auto const &name = service.name();
    out << "::rpc::service_descriptor const &" << name << "_service_descriptor() {\n";
    if (service.method_count() != 0) {
        out << "    static ::rpc::method_descriptor const methods[] = {\n";
        for (int i = 0; i < service.method_count(); ++i) {
            auto const &method = *service.method(i);
            out << "        {\"" << wire_name(method) << "\", ::rpc::method_kind::" << kind(method) << ", ::rpc::idempotency::" << semantics(method)
                << ", &::rpc::codec_for<" << message_type(method.input_type()) << ">(), &::rpc::codec_for<"
                << message_type(method.output_type()) << ">(), " << i << "},\n";
        }
        out << "    };\n";
    }
    out << "    static ::rpc::service_descriptor const value{\"" << service.full_name() << "\", "
        << (service.method_count() == 0 ? "nullptr" : "methods") << ", " << service.method_count() << "};\n    return value;\n}\n\n";
}

void write_bindings(pb::ServiceDescriptor const &service, std::ostream &out) {
    auto const &name = service.name();
    out << "std::vector<::rpc::method_binding> " << name << "_bindings(" << name << "Service &service, " << name << "Limits const &limits) {\n"
        << "    std::vector<::rpc::method_binding> result;\n    result.reserve(" << service.method_count() << ");\n";
    if (service.method_count() == 0) out << "    (void)service; (void)limits;\n";
    for (int i = 0; i < service.method_count(); ++i) {
        auto const &method = *service.method(i);
        out << "    result.push_back(::rpc::" << (streaming(method) ? "bind_stream_method" : "bind_method") << "(::rpc::method<" << message_type(method.input_type()) << ", "
            << message_type(method.output_type()) << ">{\"" << wire_name(method) << "\"}, ";
        if (streaming(method)) out << "::rpc::method_kind::" << kind(method) << ", ";
        out << "service, &" << name
            << "Service::" << identifier(method.name()) << ", limits." << identifier(method.name()) << "));\n";
    }
    out << "    return result;\n}\n\n"
        << "::rpc::server_builder &add_" << name << "_service(::rpc::server_builder &builder, " << name << "Service &service, "
        << name << "Limits const &limits) {\n    return builder.add(" << name << "_bindings(service, limits));\n}\n\n";
}

void write_stubs(pb::ServiceDescriptor const &service, std::ostream &out) {
    if (service.method_count() != 0)
        out << service.name() << "Stub::" << service.name() << "Stub(::rpc::client &channel)\n"
            << "    : lrpc_channel_(&channel), lrpc_methods_(channel.bind(" << scope(*service.file()) << service.name()
            << "_service_descriptor())) {}\n\n";
    for (int i = 0; i < service.method_count(); ++i) {
        auto const &method = *service.method(i);
        if (streaming(method)) {
            out << "::net::task<::rpc::stream_call<" << message_type(method.input_type()) << ", " << message_type(method.output_type()) << ">> "
                << service.name() << "Stub::" << identifier(method.name()) << "(::rpc::call_options options) {\n"
                << "    return ::rpc::open_stream<" << message_type(method.input_type()) << ", " << message_type(method.output_type())
                << ">(*lrpc_channel_, lrpc_methods_[" << i << "], options);\n}\n\n";
            continue;
        }
        out << "::net::task<::rpc::call_result> " << service.name() << "Stub::" << identifier(method.name()) << "("
            << message_type(method.input_type()) << " const &request, " << message_type(method.output_type())
            << " &response, ::rpc::call_options options) {\n    return lrpc_channel_->call_encoded(lrpc_methods_[" << i
            << "], {&request, &::rpc::codec_for<" << message_type(method.input_type())
            << ">()}, {&response, &::rpc::codec_for<" << message_type(method.output_type()) << ">()}, options);\n}\n\n";
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
        header << "// Generated by protoc-gen-rpc.\n#pragma once\n#include <rpc/protobuf.hpp>\n#include <rpc/stream.hpp>\n#include \"" << base << ".pb.h\"\n\n";
        source << "// Generated by protoc-gen-rpc.\n#include \"" << base << ".rpc.hpp\"\n\n";
        for (auto const &part : namespaces(file->package())) { header << "namespace " << part << " {\n"; source << "namespace " << part << " {\n"; }
        for (int i = 0; i < file->service_count(); ++i) {
            auto const &service = *file->service(i);
            write_header(service, header); write_descriptors(service, source);
            write_bindings(service, source); write_stubs(service, source);
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
