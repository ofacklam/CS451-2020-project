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
    SynchronizedQueue<UrbDataPacket<T>> appQueue;
    SynchronizedQueue<UrbDataPacket<T>> internalQueue;

    // Receiving structures
    UrbReceptionStore receptionStore;
    std::function<void(T, unsigned long, sequence)> urbDeliver;

    // Worker
    std::thread prioritySender;

public:
    UrbBroadcast(unsigned long id,
                 const std::vector<Parser::Host> &hosts,
                 const std::function<void(T, unsigned long, sequence)> &urbDeliver,
                 unsigned long long capacity = 100);

    void urbBroadcast(T msg);

private:
    void bebDeliver(bool fromApp, UrbDataPacket<T> msg, unsigned long src);

    void sendLoop();

protected:
    void waitForStop() override;
};

template<class T>
UrbBroadcast<T>::UrbBroadcast(unsigned long id, const std::vector<Parser::Host> &hosts,
                              const std::function<void(T, unsigned long, sequence)> &urbDeliver,
                              unsigned long long capacity)
        : beb(id, hosts, std::bind(&UrbBroadcast::bebDeliver, this, false, _1, _2)), ownID(id),
          nextID(0), appQueue(capacity), internalQueue(0),
          receptionStore(hosts.size()), urbDeliver(urbDeliver),
          prioritySender(&UrbBroadcast::sendLoop, this) {}

template<class T>
void UrbBroadcast<T>::urbBroadcast(T msg) {
    if (!shouldStop()) {
        sequence seq = nextID++;
        UrbDataPacket<T> data(ownID, seq, msg);
        bebDeliver(true, data, ownID);
    }
}

template<class T>
void UrbBroadcast<T>::bebDeliver(bool fromApp, UrbDataPacket<T> msg, unsigned long src) {
    if (!shouldStop()) {
        std::pair<bool, bool> newMsg = receptionStore.addMessage(msg.emitter, msg.seq, src);
        bool isNew = newMsg.first;
        bool canDeliver = newMsg.second;

        if (isNew) {
            if (fromApp)
                appQueue.enqueue(msg);
            else
                internalQueue.enqueue(msg);
        }

        if (canDeliver)
            urbDeliver(msg.data, msg.emitter, msg.seq);
    }
}

template<class T>
void UrbBroadcast<T>::sendLoop() {
    while (!shouldStop()) {
        UrbDataPacket<T> msg{};

        // Try getting from internalQueue (higher priority)
        bool success = internalQueue.dequeue(&msg, 100);

        // Only if internalQueue is empty, try appQueue
        if (!success)
            success = appQueue.dequeue(&msg, 100);

        if (shouldStop())
            break;

        if (success)
            beb.bebBroadcast(msg);
    }
}

template<class T>
void UrbBroadcast<T>::waitForStop() {
    beb.stop();
    prioritySender.join();
}

#endif //DA_PROJECT_URBBROADCAST_HPP
