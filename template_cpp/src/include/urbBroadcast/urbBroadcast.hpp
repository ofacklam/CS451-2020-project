//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_URBBROADCAST_HPP
#define DA_PROJECT_URBBROADCAST_HPP

#include <unordered_set>
#include <unordered_map>

#include "../stoppable.hpp"
#include "urbBroadcastData.hpp"
#include "urbReceptionStore.hpp"
#include "../bebBroadcast/bebBroadcast.hpp"


/**
 * Implementation of the uniform reliable broadcast interface
 * Supposes a majority of correct processes
 */
template<class T>
class UrbBroadcast : public Stoppable {
private:
    // Connection
    BebBroadcast<UrbDataPacket<T>> beb;
    unsigned long ownID;

    // Sending structures
    std::atomic<sequence> nextID;

    // Receiving structures
    UrbReceptionStore receptionStore;
    std::function<void(T, unsigned long)> urbDeliver;

public:
    UrbBroadcast(unsigned long id,
                 const std::vector<Parser::Host> &hosts,
                 const std::function<void(T, unsigned long)> &urbDeliver);

    void urbBroadcast(T msg);

private:
    void bebDeliver(UrbDataPacket<T> msg, unsigned long src);

protected:
    void waitForStop() override;
};

template<class T>
UrbBroadcast<T>::UrbBroadcast(unsigned long id, const std::vector<Parser::Host> &hosts,
                              const std::function<void(T, unsigned long)> &urbDeliver)
        : beb(id, hosts, std::bind(&UrbBroadcast::bebDeliver, this, _1, _2)),
          ownID(id), nextID(0), receptionStore(hosts.size()), urbDeliver(urbDeliver) {}

template<class T>
void UrbBroadcast<T>::urbBroadcast(T msg) {
    if (!shouldStop()) {
        sequence seq = nextID++;
        UrbDataPacket<T> data(ownID, seq, msg);
        bebDeliver(data, ownID);
    }
}

template<class T>
void UrbBroadcast<T>::bebDeliver(UrbDataPacket<T> msg, unsigned long src) {
    if (!shouldStop()) {
        std::pair<bool, bool> newMsg = receptionStore.addMessage(msg.emitter, msg.seq, src);
        bool isNew = newMsg.first;
        bool canDeliver = newMsg.second;

        if (isNew)
            beb.bebBroadcast(msg);

        if (canDeliver)
            urbDeliver(msg.data, msg.emitter);
    }
}

template<class T>
void UrbBroadcast<T>::waitForStop() {
    beb.stop();
}

#endif //DA_PROJECT_URBBROADCAST_HPP
