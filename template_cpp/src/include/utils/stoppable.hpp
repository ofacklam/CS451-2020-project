//
// Created by ofacklam on 05/11/2020.
//

#ifndef DA_PROJECT_STOPPABLE_HPP
#define DA_PROJECT_STOPPABLE_HPP

#include <atomic>

/**
 * An interface to allow interrupting the packet processing
 */
class Stoppable {
private:
    std::atomic<bool> stop_;

public:
    Stoppable() : stop_(false) {}

    virtual ~Stoppable() {
        stop();
    }

    void stop() {
        stop_ = true;
        waitForStop();
    }

    bool shouldStop() {
        return stop_;
    }

protected:
    virtual void waitForStop() = 0;
};


#endif //DA_PROJECT_STOPPABLE_HPP
