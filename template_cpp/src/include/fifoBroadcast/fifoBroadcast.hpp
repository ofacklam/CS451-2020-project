//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_FIFOBROADCAST_HPP
#define DA_PROJECT_FIFOBROADCAST_HPP

#include <mutex>

#include "../stoppable.hpp"
#include "fifoReceptionStore.hpp"
#include "../urbBroadcast/urbBroadcast.hpp"


template<class T>
class FifoBroadcast : public Stoppable {
private:
    // Connection
    UrbBroadcast<T> urb;

    // Receiving structures
    std::unordered_map<unsigned long, FifoReceptionStore<T>> receptionStores;

public:
    FifoBroadcast(unsigned long id,
                  const std::vector<Parser::Host> &hosts,
                  const std::function<void(T, unsigned long)> &fifoDeliver,
                  unsigned long long capacity = 100);

    void fifoBroadcast(T msg);

private:
    void urbDeliver(T msg, unsigned long src, sequence seq);

protected:
    void waitForStop() override;
};

template<class T>
FifoBroadcast<T>::FifoBroadcast(unsigned long id, const std::vector<Parser::Host> &hosts,
                                const std::function<void(T, unsigned long)> &fifoDeliver,
                                unsigned long long int capacity)
        : urb(id, hosts, std::bind(&FifoBroadcast::urbDeliver, this, _1, _2, _3), capacity) {
    // Initialize reception stores
    for (auto &h: hosts) {
        receptionStores.emplace(std::piecewise_construct, // https://stackoverflow.com/a/33423214
                                std::make_tuple(h.id),
                                std::make_tuple(std::bind(fifoDeliver, _1, h.id)));
    }
}

template<class T>
void FifoBroadcast<T>::fifoBroadcast(T msg) {
    if (!shouldStop())
        urb.urbBroadcast(msg);
}

template<class T>
void FifoBroadcast<T>::urbDeliver(T msg, unsigned long src, sequence seq) {
    if (!shouldStop() && receptionStores.count(src) > 0)
        receptionStores.at(src).urbDeliver(msg, seq);
}

template<class T>
void FifoBroadcast<T>::waitForStop() {
    urb.stop();
}

#endif //DA_PROJECT_FIFOBROADCAST_HPP
