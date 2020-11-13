//
// Created by ofacklam on 05/11/2020.
//

#ifndef DA_PROJECT_PERFECTLINK_HPP
#define DA_PROJECT_PERFECTLINK_HPP

#include <functional>
#include <vector>
#include <unordered_map>

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
                unsigned long numWorkers = 10);

    void pSend(T payload, unsigned long dst);

private:
    void sendLoop(const std::vector<Parser::Host> &hosts);

    void flDeliver(PlDataPacket<T> dp, unsigned long src);

    void deliverLoop();

protected:
    void waitForStop() override;
};

template<class T>
PerfectLink<T>::PerfectLink(unsigned long id, const std::vector<Parser::Host> &hosts,
                            const std::function<void(T, unsigned long)> &pDeliver,
                            unsigned long numWorkers)
        : flLink(id, hosts, [this](auto &&msg, auto &&src) { flDeliver(msg, src); }),
          pDeliver(pDeliver),
          senders(numWorkers), deliverer(&PerfectLink::deliverLoop, this) {
    // Set up sequence numbers
    for (auto &h: hosts)
        nextIDs[h.id] = 0;

    // Set up workers
    for (auto &s: senders)
        s = std::thread(&PerfectLink::sendLoop, this, hosts);
}

template<class T>
void PerfectLink<T>::pSend(T payload, unsigned long dst) {
    if (nextIDs.count(dst) <= 0)
        return;

    sequence id = nextIDs[dst]++;
    sendQueues[dst].enqueue(PlDataPacket{id, payload});
}

template<class T>
void PerfectLink<T>::sendLoop(const std::vector<Parser::Host> &hosts) {
    // Set up timings
    auto retryTime = 100ms;
    auto granularity = 200us;
    auto maxPending = 1000ul;
    unsigned long maxPendingPerDest = maxPending / hosts.size();

    // Internal pending set for the send loop
    std::unordered_map<unsigned long, std::queue<PlDataPacket<T>>> pending;
    auto currentDest = hosts.begin();
    auto nextWakeup = std::chrono::high_resolution_clock::now() + retryTime;

    while (!shouldStop()) {
        auto dst = currentDest->id;
        auto &currentQueue = pending[dst];

        // Send pending packet if applicable
        if (!currentQueue.empty()) {
            auto msg = currentQueue.front();
            currentQueue.pop();
            if (!storeACKed[dst].contains(msg.id)) {
                flLink.flSend(msg, dst);
                currentQueue.push(msg);
            }
        }

        // Try to get new packets
        bool success = true;
        while (currentQueue.size() < maxPendingPerDest && success) {
            PlDataPacket<T> msg{};
            success = sendQueues[dst].dequeue(&msg, 0);
            if (success)
                currentQueue.push(msg);
        }

        // Update pointers
        currentDest++;
        if (currentDest == hosts.end()) {
            currentDest = hosts.begin();
            std::this_thread::sleep_until(nextWakeup);
            nextWakeup = std::chrono::high_resolution_clock::now() + retryTime;
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
        bool success = deliverQueue.dequeue(&pkt, 1000);

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
