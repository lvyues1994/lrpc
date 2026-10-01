#pragma once

#include <rpc/typed.hpp>

#include <array>
#include <cstddef>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace rpc {

// A method's types and wire name; the encoding belongs to the contract.
template <class Request, class Response> struct unary_method {
    using request_type = Request;
    using response_type = Response;
    std::string name;
};

// A service's methods and the one encoding they share. It owns the names;
// bindings and stubs copy what they need.
template <class Policy, class... Methods> class service_contract {
public:
    static constexpr std::size_t size = sizeof...(Methods);
    using limits_type = std::array<method_limits, size>;
    template <std::size_t I> using method_type = typename std::tuple_element<I, std::tuple<Methods...>>::type;
    template <std::size_t I>
    using operation_type =
        method<typename method_type<I>::request_type, typename method_type<I>::response_type, Policy>;

    explicit service_contract(std::string name, Methods... methods)
        : name_(std::move(name)), methods_(std::move(methods)...) {
        if (!valid(name_)) throw std::invalid_argument{"invalid service name"};
        auto const names = method_names(std::index_sequence_for<Methods...>{});
        for (std::size_t i = 0; i < size; ++i) {
            if (!valid(*names[i])) throw std::invalid_argument{"invalid service method"};
            for (std::size_t j = 0; j < i; ++j)
                if (*names[i] == *names[j]) throw std::invalid_argument{"duplicate service method"};
        }
    }

    std::string const &name() const noexcept { return name_; }
    template <std::size_t I> operation_type<I> operation() const { return {std::get<I>(methods_).name}; }

    // One handler per method, in declaration order.
    template <class Service, class... Functions>
    std::vector<method_binding> bindings(Service &service, limits_type const &limits, Functions... functions) const {
        static_assert(sizeof...(Functions) == size, "one handler is required for each service method");
        return bindings_impl(service, limits, std::make_tuple(functions...), std::index_sequence_for<Methods...>{});
    }

private:
    static bool valid(std::string const &text) noexcept {
        return !text.empty() && text.find('\0') == std::string::npos;
    }
    template <std::size_t... I>
    std::array<std::string const *, size> method_names(std::index_sequence<I...>) const {
        return {{&std::get<I>(methods_).name...}};
    }
    template <class Service, class Functions, std::size_t... I>
    std::vector<method_binding> bindings_impl(Service &service, limits_type const &limits, Functions const &functions,
                                              std::index_sequence<I...>) const {
        std::vector<method_binding> result;
        result.reserve(size);
        using expansion = int[];
        static_cast<void>(
            expansion{0, (result.push_back(bind_method(operation<I>(), service, std::get<I>(functions), limits[I])), 0)...});
        return result;
    }

    std::string name_;
    std::tuple<Methods...> methods_;
};

template <class Policy, class... Methods>
service_contract<Policy, Methods...> make_service_contract(std::string name, Policy, Methods... methods) {
    return service_contract<Policy, Methods...>{std::move(name), std::move(methods)...};
}

// A contract bound to one client or channel.
template <class Policy, class... Methods> class bound_service {
public:
    using contract_type = service_contract<Policy, Methods...>;

    bound_service(call_target target, contract_type const &contract)
        : target_(target), methods_(bind_all(target, contract, std::index_sequence_for<Methods...>{})) {}

    template <std::size_t I>
    unary_call call(typename contract_type::template method_type<I>::request_type const &request,
                    typename contract_type::template method_type<I>::response_type &response,
                    call_spec const *spec = nullptr, response_trailer *trailer = nullptr) const noexcept {
        using types = typename contract_type::template method_type<I>;
        return target_.call(methods_[I], encoded<typename types::request_type, Policy>(request),
                            decoded<typename types::response_type, Policy>(response), spec, trailer);
    }

private:
    template <std::size_t... I>
    static std::array<method_ref, sizeof...(Methods)> bind_all(call_target target, contract_type const &contract,
                                                                std::index_sequence<I...>) {
        return {{target.bind(contract.template operation<I>().name,
                             codec_label<typename contract_type::template method_type<I>::request_type, Policy>())...}};
    }

    call_target target_;
    std::array<method_ref, sizeof...(Methods)> methods_;
};

template <class Policy, class... Methods>
bound_service<Policy, Methods...> bind_service(call_target target, service_contract<Policy, Methods...> const &contract) {
    return {target, contract};
}

} // namespace rpc
