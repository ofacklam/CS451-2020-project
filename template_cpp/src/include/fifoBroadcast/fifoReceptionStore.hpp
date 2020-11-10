//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_FIFORECEPTIONSTORE_HPP
#define DA_PROJECT_FIFORECEPTIONSTORE_HPP

#include <mutex>


template<class T>
class FifoReceptionStore {
private:
    std::mutex m;
    sequence expectedID;
    std::unordered_map<sequence, T> pending;
    std::function<void(T)> fifoDeliver;

public:
    explicit FifoReceptionStore(std::function<void(T)> fifoDeliver) : expectedID(0), fifoDeliver(fifoDeliver) {}

    void urbDeliver(T msg, sequence seq);

private:
    void unsafeDeliverPending();
};

template<class T>
void FifoReceptionStore<T>::urbDeliver(T msg, sequence seq) {
    std::lock_guard<std::mutex> lk(m);

    if(seq > expectedID) { // not the next one -> simply add to pending
        pending[seq] = msg;
    } else if(seq == expectedID) { // next one -> deliver everything we can
        fifoDeliver(msg);
        expectedID++;
        unsafeDeliverPending();
    } else {
        std::cerr << "Received an already-delivered message!!! " << seq << std::endl;
        return;
    }
}

template<class T>
void FifoReceptionStore<T>::unsafeDeliverPending() {
    while(pending.count(expectedID) > 0) {
        fifoDeliver(pending[expectedID]);
        pending.erase(expectedID);
        expectedID++;
    }
}

#endif //DA_PROJECT_FIFORECEPTIONSTORE_HPP
