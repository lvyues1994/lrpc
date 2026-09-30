#pragma once

#include <rpc/typed.hpp>

#include <array>
#include <tuple>

namespace rpc {

// The types and wire name belong to the service; encoding is selected when its
// contract is created. This declaration does not depend on a codec module.
template <class Request, class Response> struct unary_method {
    using request_type = Request;
    using response_type = Response;
    std::string name;
    idempotency semantics = idempotency::unknown;
};

namespace detail {
struct method_codecs { codec_ops const *request; codec_ops const *response; };
}

template <class... Methods> class bound_service;

// Owns names, borrows process-lifetime codec tables. Copying/moving a contract
// never copies pointers into its own strings. Binding and registration consume
// temporary descriptor views synchronously; neither retains this contract.
template <class... Methods> class service_contract {
public:
    static constexpr std::size_t size = sizeof...(Methods);
    using limits_type = std::array<method_limits, size>;
    template <std::size_t I> using method_type = typename std::tuple_element<I, std::tuple<Methods...>>::type;

    template <class Policy>
    service_contract(std::string name, Policy, Methods... methods)
        : name_(std::move(name)), methods_(std::move(methods)...),
          codecs_{{{&Policy::template operations<typename Methods::request_type>(),
                    &Policy::template operations<typename Methods::response_type>()}...}} {
        if (name_.empty() || name_.find('\0') != std::string::npos) throw std::invalid_argument{"invalid service name"};
        auto const table = descriptors(std::index_sequence_for<Methods...>{});
        for (std::size_t i = 0; i < size; ++i) {
            auto const &name = method_name_at(i, std::index_sequence_for<Methods...>{});
            if (name.empty() || name.find('\0') != std::string::npos ||
                static_cast<unsigned>(table[i].semantics) > static_cast<unsigned>(idempotency::idempotent))
                throw std::invalid_argument{"invalid service method"};
            auto valid = [](codec_ops const *ops) { return ops && ops->size && ops->encode && ops->decode; };
            if (!valid(table[i].request_codec) || !valid(table[i].response_codec))
                throw std::invalid_argument{"invalid service codec"};
            for (std::size_t j = 0; j < i; ++j)
                if (name == table[j].name) throw std::invalid_argument{"duplicate service method"};
        }
    }

    template <class Service, class... Functions>
    std::vector<method_binding> bindings(Service &service, limits_type const &limits, Functions... functions) const {
        static_assert(sizeof...(Functions) == size, "one handler is required for each service method");
        return bindings_impl(service, limits, std::make_tuple(functions...), std::index_sequence_for<Methods...>{});
    }
private:
    friend class bound_service<Methods...>;
    template <std::size_t... I>
    std::array<method_descriptor, size> descriptors(std::index_sequence<I...>) const {
        return {{{std::get<I>(methods_).name.c_str(), method_kind::unary, std::get<I>(methods_).semantics,
                  codecs_[I].request, codecs_[I].response, I}...}};
    }
    template <std::size_t... I>
    std::string const &method_name_at(std::size_t index, std::index_sequence<I...>) const {
        std::array<std::string const *, size> const names{{&std::get<I>(methods_).name...}};
        return *names[index];
    }
    template <std::size_t I, class Service, class Result>
    method_binding binding(Service &service,
        net::task<Result> (Service::*function)(server_context &, typename method_type<I>::request_type const &,
                                             typename method_type<I>::response_type &), method_limits limits) const {
        static_assert(std::is_same<Result, status_code>::value || std::is_same<Result, status>::value,
            "RPC handlers must return task<status_code> or task<status>");
        using handler_type = detail::typed_method_handler<typename method_type<I>::request_type,
            typename method_type<I>::response_type, Service, Result>;
        auto handler = std::make_shared<handler_type>(service, function, limits.message_cache_entries,
                                                      *codecs_[I].request, *codecs_[I].response);
        return {std::get<I>(methods_).name, limits.max_response_bytes, handler.get(),
                limits.max_response_head_bytes, std::move(handler)};
    }
    template <class Service, class Functions, std::size_t... I>
    std::vector<method_binding> bindings_impl(Service &service, limits_type const &limits,
                                             Functions const &functions, std::index_sequence<I...>) const {
        std::vector<method_binding> result;
        result.reserve(size);
        // Braced expansion is sequenced; a failed adapter leaves no builder state.
        using expansion = int[];
        (void)expansion{0, (result.push_back(binding<I>(service, std::get<I>(functions), limits[I])), 0)...};
        return result;
    }
    std::string name_;
    std::tuple<Methods...> methods_;
    std::array<detail::method_codecs, size> codecs_;
};

template <class Policy, class... Methods>
service_contract<Methods...> make_service_contract(std::string name, Policy policy, Methods... methods) {
    return {std::move(name), policy, std::move(methods)...};
}

template <class... Methods> class bound_service {
public:
    bound_service(client &channel, service_contract<Methods...> const &contract)
        : channel_(channel), codecs_(contract.codecs_), handles_(bind(channel, contract)) {}
    template <std::size_t I>
    net::task<call_result> call(typename service_contract<Methods...>::template method_type<I>::request_type const &request,
                              typename service_contract<Methods...>::template method_type<I>::response_type &response,
                              call_options options = {}) const {
        return channel_.call_encoded(handles_[I], {&request, codecs_[I].request}, {&response, codecs_[I].response}, options);
    }
private:
    static std::vector<method_handle> bind(client &channel, service_contract<Methods...> const &contract) {
        auto const methods = contract.descriptors(std::index_sequence_for<Methods...>{});
        return channel.bind({contract.name_.c_str(), methods.data(), methods.size()});
    }
    client &channel_; // Borrowed; must outlive calls through this stub.
    std::array<detail::method_codecs, sizeof...(Methods)> codecs_;
    std::vector<method_handle> handles_;
};

template <class... Methods>
bound_service<Methods...> bind_service(client &channel, service_contract<Methods...> const &contract) {
    return {channel, contract};
}

template <class... Methods, class Service, class... Functions>
server_builder &add_service(server_builder &builder, service_contract<Methods...> const &contract,
                            Service &service, typename service_contract<Methods...>::limits_type const &limits,
                            Functions... functions) {
    return builder.add(contract.bindings(service, limits, functions...));
}

} // namespace rpc
