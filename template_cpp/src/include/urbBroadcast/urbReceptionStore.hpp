//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_URBRECEPTIONSTORE_HPP
#define DA_PROJECT_URBRECEPTIONSTORE_HPP

#include <mutex>
#include <unordered_map>

#include "../perfectLink/sequenceNumberStore.hpp"


class UrbReceptionStore {
private:
    std::mutex m;
    unsigned long numHosts;

    // Note: a message is uniquely identified by the (emitter, seq) couple
    std::unordered_map<unsigned long, SequenceNumberStore> delivered;
    std::unordered_map<unsigned long, std::unordered_map<sequence, std::unordered_set<unsigned long>>> ack;

public:
    explicit UrbReceptionStore(unsigned long numHosts) : numHosts(numHosts) {}

    // Returns a couple (isNew, canDeliver)
    std::pair<bool, bool> addMessage(unsigned long emitter, sequence seq, unsigned long src) {
        std::lock_guard<std::mutex> lk(m);

        // if delivered -> not new & don't deliver
        if (unsafeIsDelivered(emitter, seq))
            return std::make_pair(false, false);

        // check if pending & add to ack set
        bool isNew = !unsafeIsPending(emitter, seq);
        ack[emitter][seq].insert(src);

        // check if can deliver & deliver if applicable
        bool canDeliver = unsafeCanDeliver(emitter, seq);
        if (canDeliver) {
            ack[emitter].erase(seq);
            delivered[emitter].add(seq);
        }

        return std::make_pair(isNew, canDeliver);
    }

private:
    bool unsafeIsPending(unsigned long emitter, sequence seq) {
        return ack.count(emitter) > 0 && ack[emitter].count(seq) > 0;
    }

    bool unsafeCanDeliver(unsigned long emitter, sequence seq) {
        return unsafeIsPending(emitter, seq) && ack[emitter][seq].size() > numHosts / 2;
    }

    bool unsafeIsDelivered(unsigned long emitter, sequence seq) {
        return delivered[emitter].contains(seq);
    }
};

#endif //DA_PROJECT_URBRECEPTIONSTORE_HPP
