//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_LCRECEPTIONSTORE_HPP
#define DA_PROJECT_LCRECEPTIONSTORE_HPP

#include <mutex>
#include <unordered_map>
#include <set>
#include <functional>
#include <iostream>
#include <algorithm>
#include <cassert>

#include "utils/sequenceNumberStore.hpp"
#include "lcBroadcastData.hpp"


typedef std::set<std::pair<unsigned long, sequence>> descendants;

template<class T>
class LcReceptionStore {
private:
    std::mutex m;
    std::unordered_map<unsigned long, sequence> expectedIDs;
    std::unordered_map<unsigned long, std::unordered_map<sequence, LcbDataPacket<T>>> pending;
    std::unordered_map<unsigned long, std::unordered_map<sequence, descendants>> implications;
    std::function<void(T, unsigned long)> lcDeliver;

public:
    explicit LcReceptionStore(std::function<void(T, unsigned long)> lcDeliver) : lcDeliver(lcDeliver) {}

    void urbDeliver(LcbDataPacket<T> msg, unsigned long src, sequence seq);

    std::unordered_map<unsigned long, sequence> getVectorClock();

private:
    void unsafeAddPending(LcbDataPacket<T> &msg, unsigned long src, sequence seq);

    bool unsafeCanDeliver(LcbDataPacket<T> &pkt);

    void unsafeLcDeliver(LcbDataPacket<T> msg, unsigned long src, sequence seq);

    void unsafeDeliverPending(unsigned long src, sequence expectedID);
};

template<class T>
void LcReceptionStore<T>::urbDeliver(LcbDataPacket<T> msg, unsigned long src, sequence seq) {
    std::lock_guard<std::mutex> lk(m);

    if (!unsafeCanDeliver(msg)) { // cannot deliver -> simply add to pending
        unsafeAddPending(msg, src, seq);
    } else { // can deliver -> deliver everything we can
        unsafeLcDeliver(msg, src, seq);
    }
}

template<class T>
std::unordered_map<unsigned long, sequence> LcReceptionStore<T>::getVectorClock() {
    std::lock_guard<std::mutex> lk(m);
    return expectedIDs;
}

template<class T>
void LcReceptionStore<T>::unsafeAddPending(LcbDataPacket<T> &msg, unsigned long src, sequence seq) {
    pending[src][seq] = msg;
    auto msgIdentifier = std::make_pair(src, seq);

    for (std::pair<unsigned long, sequence> dep: msg.dependencies) {
        implications[dep.first][dep.second].insert(msgIdentifier);
    }
}

template<class T>
bool LcReceptionStore<T>::unsafeCanDeliver(LcbDataPacket<T> &pkt) {
    return std::all_of(pkt.dependencies.begin(), pkt.dependencies.end(),
                       [this](std::pair<unsigned long, sequence> const dep) {
                           return expectedIDs[dep.first] >= dep.second;
                       });
}

template<class T>
void LcReceptionStore<T>::unsafeLcDeliver(LcbDataPacket<T> msg, unsigned long src, sequence seq) {
    assert(seq == expectedIDs[src]);

    lcDeliver(msg.data, src);
    expectedIDs[src]++;
    unsafeDeliverPending(src, expectedIDs[src]);
}

template<class T>
void LcReceptionStore<T>::unsafeDeliverPending(unsigned long src, sequence expectedID) {
    descendants imps = implications[src][expectedID];

    // Delete these implications
    implications[src].erase(expectedID);

    // Handle each unlocked implication (delivering if possible)
    for (std::pair<unsigned long, sequence> msgIdentifier: imps) {
        unsigned long msgSrc = msgIdentifier.first;
        sequence msgSeq = msgIdentifier.second;

        LcbDataPacket<T> msg = pending[msgSrc][msgSeq];
        if (unsafeCanDeliver(msg)) {
            pending[msgSrc].erase(msgSeq);
            unsafeLcDeliver(msg, msgSrc, msgSeq);
        }
    }
}

#endif //DA_PROJECT_LCRECEPTIONSTORE_HPP
