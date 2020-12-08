//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_LCBROADCAST_HPP
#define DA_PROJECT_LCBROADCAST_HPP

#include <atomic>
#include <utility>

#include "utils/stoppable.hpp"
#include "lcReceptionStore.hpp"
#include "lcBroadcastData.hpp"
#include "urbBroadcast/urbBroadcast.hpp"


template<class T>
class LcBroadcast : public Stoppable {
private:
    // Connection
    UrbBroadcast<LcbDataPacket<T>> urb;

    // Sending structures
    std::atomic<sequence> nextID;
    unsigned long ownID;
    std::vector<unsigned long> dependencies;

    // Receiving structures
    LcReceptionStore<T> receptionStore;

public:
    LcBroadcast(unsigned long id,
                const std::vector<Parser::Host> &hosts,
                std::vector<unsigned long> dependencies,
                const std::function<void(T, unsigned long)> &fifoDeliver,
                unsigned long long capacity = 100);

    void lcBroadcast(T payload);

private:
    void urbDeliver(LcbDataPacket<T> msg, unsigned long src, sequence seq);

protected:
    void waitForStop() override;
};

template<class T>
LcBroadcast<T>::LcBroadcast(unsigned long id, const std::vector<Parser::Host> &hosts,
                            std::vector<unsigned long> dependencies,
                            const std::function<void(T, unsigned long)> &fifoDeliver,
                            unsigned long long int capacity)
        : urb(id, hosts, [this](auto &&msg, auto &&src, auto &&seq) { return urbDeliver(msg, src, seq); }, capacity),
          nextID(0), ownID(id), dependencies(std::move(dependencies)), receptionStore(fifoDeliver) {}

template<class T>
void LcBroadcast<T>::lcBroadcast(T payload) {
    if (!shouldStop()) {
        sequence seq = nextID++;
        LcbDataPacket<T> msg(payload);

        // Add dependencies
        std::unordered_map<unsigned long, sequence> vectorClock = receptionStore.getVectorClock();
        for(unsigned long dep: dependencies) {
            msg.dependencies[dep] = vectorClock[dep];
        }
        // Fifo constraint
        msg.dependencies[ownID] = seq;

        urb.urbBroadcast(msg);
    }
}

template<class T>
void LcBroadcast<T>::urbDeliver(LcbDataPacket<T> msg, unsigned long src, sequence seq) {
    if (!shouldStop())
        receptionStore.urbDeliver(msg, src, seq);
}

template<class T>
void LcBroadcast<T>::waitForStop() {
    urb.stop();
}

#endif //DA_PROJECT_LCBROADCAST_HPP
