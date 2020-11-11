//
// Created by ofacklam on 05/11/2020.
//

#ifndef DA_PROJECT_PERFECTLINK_HPP
#define DA_PROJECT_PERFECTLINK_HPP

#include <functional>
#include <vector>

#include "utils/stoppable.hpp"
#include "fairLossLink/fairLossLink.hpp"
#include "perfectLinkData.hpp"
#include "utils/synchronizedQueue.hpp"
#include "utils/sequenceNumberStore.hpp"

using namespace std::placeholders;


/**
 * Implementation of the perfect link interface, on top of UDP
 * - Validity given by the retransmission strategy
 * - No duplication given by storeDelivered check
 * - No creation by the no-creation property of FairLossLink
 */
template<class T>
class PerfectLink : public Stoppable {
private:
    // Connection
    FairLossLink<PlDataPacket<T>> flLink;

    // Sending structures
    std::unordered_map<unsigned long, std::atomic<sequence>> nextIDs;
    std::unordered_map<unsigned long, SynchronizedQueue<PlDataPacket<T>>> sendQueues;
    std::unordered_map<unsigned long, SequenceNumberStore> storeACKed;

    // Receiving structures
    std::unordered_map<unsigned long, SequenceNumberStore> storeDelivered;
    SynchronizedQueue<std::pair<T, unsigned long>> deliverQueue;
    std::function<void(T, unsigned long)> pDeliver;

    // Workers
    std::vector<std::thread> senders;
    std::thread deliverer;

public:
    PerfectLink(unsigned long id,
                const std::vector<Parser::Host> &hosts,
                const std::function<void(T, unsigned long)> &pDeliver,
                unsigned numWorkers = 10);

    void pSend(T payload, unsigned long dst);

private:
    void sendLoop(unsigned long dst);

    void flDeliver(PlDataPacket<T> dp, unsigned long src);

    void deliverLoop();

protected:
    void waitForStop() override;
};

// Info about function binding from https://stackoverflow.com/a/45525074
template<class T>
PerfectLink<T>::PerfectLink(unsigned long id, const std::vector<Parser::Host> &hosts,
                            const std::function<void(T, unsigned long)> &pDeliver,
                            unsigned int numWorkers)
        : flLink(id, hosts, std::bind(&PerfectLink::flDeliver, this, _1, _2)),
          pDeliver(pDeliver),
          senders(numWorkers * hosts.size()), deliverer(&PerfectLink::deliverLoop, this) {
    // Set up sequence numbers
    for (auto &h: hosts)
        nextIDs[h.id] = 0;

    // Set up workers
    for (unsigned i = 0; i < hosts.size(); i++) {
        for (unsigned j = 0; j < numWorkers; j++) {
            senders[i * numWorkers + j] = std::thread(&PerfectLink::sendLoop, this, hosts[i].id);
        }
    }
}

template<class T>
void PerfectLink<T>::pSend(T payload, unsigned long dst) {
    if (nextIDs.count(dst) <= 0)
        return;

    sequence id = nextIDs[dst]++;
    sendQueues[dst].enqueue(PlDataPacket{id, payload});
}

template<class T>
void PerfectLink<T>::sendLoop(unsigned long dst) {
    while (!shouldStop()) {
        // Get next packet to send
        PlDataPacket<T> msg{};
        bool success = sendQueues[dst].dequeue(&msg, 100);

        if (shouldStop())
            break;

        if (success) {
            // Send the packet, at regular intervals
            while (!storeACKed[dst].contains(msg.id) && !shouldStop()) {
                flLink.flSend(msg, dst);
                std::this_thread::sleep_for(100ms);
            }
        }
    }
}

template<class T>
void PerfectLink<T>::flDeliver(PlDataPacket<T> dp, unsigned long src) {
    if (!shouldStop()) {
        // Differentiate DATA from ACK
        if (dp.isData) {
            // DATA packet: send ACK, add to storeDelivered, deliver (if applicable)
            PlDataPacket<T> ack(dp.id);
            flLink.flSend(ack, src);
            bool delivered = storeDelivered[src].add(dp.id);
            if (!delivered)
                deliverQueue.enqueue(std::make_pair(dp.payload, src));
        } else {
            // ACK packet: add to storeACKed
            storeACKed[src].add(dp.id);
        }
    }
}

template<class T>
void PerfectLink<T>::deliverLoop() {
    while (!shouldStop()) {
        // Get next packet to deliver
        std::pair<T, unsigned long> pkt{};
        bool success = deliverQueue.dequeue(&pkt, 100);

        if (shouldStop())
            break;

        if (success)
            pDeliver(pkt.first, pkt.second);
    }
}

template<class T>
void PerfectLink<T>::waitForStop() {
    // Stop underlying link
    flLink.stop();

    // Wait for threads to terminate
    for (std::thread &s: senders)
        s.join();
    deliverer.join();
    std::cout << "Stopped PerfectLink" << std::endl;
}

#endif //DA_PROJECT_PERFECTLINK_HPP
