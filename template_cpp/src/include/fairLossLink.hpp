//
// Created by ofacklam on 08/11/2020.
//

#ifndef DA_PROJECT_FAIRLOSSLINK_HPP
#define DA_PROJECT_FAIRLOSSLINK_HPP

#include <functional>
#include <thread>
#include <sys/socket.h>
#include <netdb.h>
#include <unordered_map>

#include "stoppable.hpp"
#include "utils.hpp"


/**
 * Implementation of the fair-loss link interface
 * All properties are directly guaranteed by the underlying UDP connection
 */
template<class T>
class FairLossLink : public Stoppable {
private:
    // Connection info
    int socketFD;
    unsigned long ownId;
    std::unordered_map<unsigned long, sockaddr_in> idToAddress;
    std::unordered_map<in_addr_t, std::unordered_map<in_port_t, unsigned long>> addressToId;

    // Receiving structures
    std::function<void(T, long, unsigned long)> flDeliver;

    // Workers
    std::thread receiver;

public:
    FairLossLink(unsigned long id,
                 const std::vector<Parser::Host> &hosts,
                 const std::function<void(T, long, unsigned long)> &flDeliver);

    void flSend(T msg, unsigned long dst);

private:
    bool setupSocket();

    void receiveLoop();

protected:
    void waitForStop() override;
};

template<class T>
FairLossLink<T>::FairLossLink(unsigned long id,
                              const std::vector<Parser::Host> &hosts,
                              const std::function<void(T, long, unsigned long)> &flDeliver)
        : ownId(id), flDeliver(flDeliver) {
    // Create correspondence maps id <-> address
    for (auto &h: hosts) {
        auto address = Utils::getSocketAddress(h);
        auto ip = address.sin_addr.s_addr;
        auto port = address.sin_port;

        idToAddress[h.id] = address;
        addressToId[ip][port] = h.id;
    }

    // Create socket
    socketFD = socket(AF_INET, SOCK_DGRAM, 0);
    if (!setupSocket())
        throw std::runtime_error("Failed to bind to socket!");

    // Start workers
    receiver = std::thread(&FairLossLink::receiveLoop, this);
}

template<class T>
bool FairLossLink<T>::setupSocket() {
    // Receive timeout setup (from https://beej.us/guide/bgnet/html/#common-questions)
    timeval tv{0, 100000};
    setsockopt(socketFD, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    // Bind to own address
    auto &address = idToAddress[ownId];
    int res = bind(socketFD, reinterpret_cast<sockaddr *>(&address), sizeof(address));
    return res == 0;
}

template<class T>
void FairLossLink<T>::flSend(T msg, unsigned long dst) {
    if (!shouldStop()) {
        // Get destination address
        auto &dstAddr = idToAddress[dst];

        // Convert message
        auto netMsg = Utils::htonT<T>(msg);

        // Send to socket
        sendto(socketFD, &netMsg, sizeof(netMsg), 0, reinterpret_cast<sockaddr *>(&dstAddr), sizeof(dstAddr));
    }
}

// Socket reception logic inspired from https://beej.us/guide/bgnet/html/#datagram
template<class T>
void FairLossLink<T>::receiveLoop() {
    while (!shouldStop()) {
        // Receive a packet from network
        T netMsg;
        sockaddr_storage srcAddr{};
        socklen_t addrLen = sizeof(srcAddr);
        auto size = recvfrom(socketFD, &netMsg, sizeof(netMsg), 0, reinterpret_cast<sockaddr *>(&srcAddr), &addrLen);

        if (shouldStop())
            break;

        // Only treat non-empty messages from IPv4 addresses
        if (size > 0 && srcAddr.ss_family == AF_INET) {
            // Convert message
            T msg = Utils::ntohT<T>(netMsg);

            // Extract host ID
            auto src = reinterpret_cast<sockaddr_in *>(&srcAddr);
            auto id = addressToId[src->sin_addr.s_addr][src->sin_port];

            // Deliver
            flDeliver(msg, size, id);
        }
    }
}

template<class T>
void FairLossLink<T>::waitForStop() {
    // Wait for receiver to terminate & close socket
    receiver.join();
    close(socketFD);
}

#endif //DA_PROJECT_FAIRLOSSLINK_HPP
