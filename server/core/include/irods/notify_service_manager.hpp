#ifndef IRODS_NOTIFY_SERVICE_MANAGER_HPP
#define IRODS_NOTIFY_SERVICE_MANAGER_HPP

#include <fmt/compile.h>
#include <fmt/format.h>

#include <cstddef>
#include <ctime>
#include <string>
#include <utility>

namespace irods
{
    void do_notify_service_manager(const char* _msg, const std::size_t _msg_length, const char* _socket_path);

    static inline void notify_service_manager(const std::string& _msg)
    {
        const char* sm_socket_path = std::getenv("NOTIFY_SOCKET");
        if (nullptr == sm_socket_path) {
            // if NOTIFY_SOCKET is not set, we're done
            return;
        }

        do_notify_service_manager(_msg.data(), _msg.size(), sm_socket_path);
    }

    template <typename... Args>
    static inline void notify_service_manager(const fmt::format_string<Args...>& _format, Args&&... _args)
    {
        const char* sm_socket_path = std::getenv("NOTIFY_SOCKET");
        if (nullptr == sm_socket_path) {
            // if NOTIFY_SOCKET is not set, we're done
            return;
        }

        auto msg = fmt::format(_format, std::forward<Args>(_args)...);
        do_notify_service_manager(msg.data(), msg.size(), sm_socket_path);
    }

    template <typename CompiledFormat, typename... Args>
        requires fmt::detail::is_compiled_string<CompiledFormat>::value ||
                 fmt::detail::is_compiled_format<CompiledFormat>::value
    static inline void notify_service_manager(const CompiledFormat& _format, Args&&... _args)
    {
        const char* sm_socket_path = std::getenv("NOTIFY_SOCKET");
        if (nullptr == sm_socket_path) {
            // if NOTIFY_SOCKET is not set, we're done
            return;
        }

        auto msg = fmt::format(_format, std::forward<Args>(_args)...);
        do_notify_service_manager(msg.data(), msg.size(), sm_socket_path);
    }
} //namespace irods

#endif // IRODS_NOTIFY_SERVICE_MANAGER_HPP
