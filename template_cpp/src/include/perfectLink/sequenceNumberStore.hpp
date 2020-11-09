//
// Created by ofacklam on 06/11/2020.
//

#ifndef DA_PROJECT_SEQUENCENUMBERSTORE_HPP
#define DA_PROJECT_SEQUENCENUMBERSTORE_HPP

#include <unordered_set>
#include <mutex>

typedef unsigned long long sequence;

class SequenceNumberStore {
private:
    sequence firstNotStored;
    std::unordered_set<sequence> sparseStorage;
    std::mutex m;

public:
    SequenceNumberStore() : firstNotStored(0) {}

    bool add(sequence seq) {
        std::lock_guard<std::mutex> lk(m);
        bool contained = unsafeContains(seq);

        if (seq > firstNotStored) {
            // If there is a hole => add to sparse storage
            sparseStorage.insert(seq);
        } else if (seq == firstNotStored) {
            // If next value => increment counter & compact the sparse storage
            firstNotStored++;
            while (sparseStorage.count(firstNotStored) > 0) {
                sparseStorage.erase(firstNotStored);
                firstNotStored++;
            }
        }

        return contained;
    }

    bool contains(sequence seq) {
        std::lock_guard<std::mutex> lk(m);
        return unsafeContains(seq);
    }

private:
    bool unsafeContains(sequence seq) {
        return seq < firstNotStored || sparseStorage.count(seq) > 0;
    }
};

#endif //DA_PROJECT_SEQUENCENUMBERSTORE_HPP
