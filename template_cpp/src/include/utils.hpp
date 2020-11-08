//
// Created by ofacklam on 08/11/2020.
//

#ifndef DA_PROJECT_UTILS_HPP
#define DA_PROJECT_UTILS_HPP

#include <endian.h>
#include "parser.hpp"

namespace Utils {
    // Taken from "barrier.hpp"
    template<class T>
    inline T htonT(T t) {
#if __BYTE_ORDER == __LITTLE_ENDIAN
        char *ptr = reinterpret_cast<char *>(&t);
        std::reverse(ptr, ptr + sizeof(T));
#endif
        return t;
    }

    template<class T>
    inline T ntohT(T t) {
        return Utils::htonT<T>(t);
    }

    // Taken from "barrier.hpp" logic
    inline sockaddr_in getSocketAddress(Parser::Host host) {
        sockaddr_in address{};
        std::memset(&address, 0, sizeof(address));
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = host.ip;
        address.sin_port = host.port;
        return address;
    }
}


#endif //DA_PROJECT_UTILS_HPP
