//
// Created by ofacklam on 05/11/2020.
//

#ifndef DA_PROJECT_PERFECTLINK_HPP
#define DA_PROJECT_PERFECTLINK_HPP


#include <functional>
#include <vector>

#include "stoppable.hpp"
#include "fairLossLink/fairLossLink.hpp"
#include "perfectLinkData.hpp"
#include "synchronizedQueue.hpp"
#include "sequenceNumberStore.hpp"

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
    struct SendInfo {
        PlDataPacket<T> msg;
        unsigned long dst;
    };

    // Connection
    FairLossLink<PlDataPacket<T>> flLink;

    // Sending structures
    std::unordered_map<unsigned long, std::atomic<sequence>> nextIDs;
    SynchronizedQueue<SendInfo> sendQueue;
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
                unsigned long long capacity = 100,
                unsigned numWorkers = 10);

    void pSend(T payload, unsigned long dst);

private:
    void sendLoop();

    void flDeliver(PlDataPacket<T> dp, unsigned long src);

    void deliverLoop();

protected:
    void waitForStop() override;
};

// Info about function binding from https://stackoverflow.com/a/45525074
template<class T>
PerfectLink<T>::PerfectLink(unsigned long id, const std::vector<Parser::Host> &hosts,
                            const std::function<void(T, unsigned long)> &pDeliver,
                            unsigned long long int capacity, unsigned int numWorkers)
        : flLink(id, hosts, std::bind(&PerfectLink::flDeliver, this, _1, _2)),
          sendQueue(capacity), deliverQueue(0), pDeliver(pDeliver),
          senders(numWorkers), deliverer(&PerfectLink::deliverLoop, this) {
    // Set up sequence numbers
    for (auto &h: hosts)
        nextIDs[h.id] = 0;

    // Set up workers
    for (auto &s: senders)
        s = std::thread(&PerfectLink::sendLoop, this);
}

template<class T>
void PerfectLink<T>::pSend(T payload, unsigned long dst) {
    if (nextIDs.count(dst) <= 0)
        return;

    sequence id = nextIDs[dst]++;
    sendQueue.enqueue(SendInfo{PlDataPacket{id, payload}, dst});
}

template<class T>
void PerfectLink<T>::sendLoop() {
    while (!shouldStop()) {
        // Get next packet to send
        SendInfo info{};
        bool success = sendQueue.dequeue(&info, 100);

        if (shouldStop())
            break;

        if (success) {
            auto msg = info.msg;
            auto dst = info.dst;
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
}

#endif //DA_PROJECT_PERFECTLINK_HPP
