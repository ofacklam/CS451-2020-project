//
// Created by ofacklam on 09/11/2020.
//

#ifndef DA_PROJECT_BEBBROADCAST_HPP
#define DA_PROJECT_BEBBROADCAST_HPP

#include <vector>

#include "utils/stoppable.hpp"
#include "perfectLink/perfectLink.hpp"


/**
 * Implementation of best-effort broadcast interface
 * All properties are guaranteed by underlying perfect links
 */
template<class T>
class BebBroadcast : public Stoppable {
private:
    std::vector<Parser::Host> hosts;
    PerfectLink<T> pLink;

public:
    BebBroadcast(unsigned long id,
                 const std::vector<Parser::Host> &hosts,
                 const std::function<void(T, unsigned long)> &bebDeliver);

    void bebBroadcast(T msg);

protected:
    void waitForStop() override;
};

template<class T>
BebBroadcast<T>::BebBroadcast(unsigned long id, const std::vector<Parser::Host> &hosts,
                              const std::function<void(T, unsigned long)> &bebDeliver)
        : hosts(hosts), pLink(id, hosts, bebDeliver, 1) {}

template<class T>
void BebBroadcast<T>::bebBroadcast(T msg) {
    for (auto &h: hosts) {
        if (shouldStop())
            return;
        pLink.pSend(msg, h.id);
    }
}

template<class T>
void BebBroadcast<T>::waitForStop() {
    pLink.stop();
}

#endif //DA_PROJECT_BEBBROADCAST_HPP
