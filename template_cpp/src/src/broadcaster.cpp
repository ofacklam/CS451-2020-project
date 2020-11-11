//
// Created by ofacklam on 10/11/2020.
//

#include "broadcaster.hpp"


Broadcaster::Broadcaster(unsigned long id, const std::vector<Parser::Host> &hosts,
                         const std::string &configPath, std::string outputPath,
                         unsigned long long capacity)
        : outputPath(std::move(outputPath)),
          fifo(id, hosts, [this](auto &&msg, auto &&src) { deliver(msg, src); }, capacity),
          ownID(id) {
    // Get number of messages to broadcast
    std::ifstream in(configPath);
    in >> numMessages;
}

void Broadcaster::broadcast() {
    // Broadcast all messages
    for (sequence s = 1; s <= numMessages; s++) {
        std::ostringstream tmp;
        tmp << "b " << s << std::endl;
        log << tmp.str();
        fifo.fifoBroadcast(Integer(s));
    }

    // Wait for finish
    std::unique_lock<std::mutex> lk(m);
    cv.wait(lk, [this] { return numOwnDelivered == numMessages; });
}

void Broadcaster::writeOutput() {
    std::ofstream out(outputPath);
    out << log.str();
    out.flush();

    log.str("");
}

void Broadcaster::deliver(const Integer<sequence>& msg, unsigned long src) {
    // Write to output
    std::ostringstream tmp;
    tmp << "d " << src << " " << msg.val_ << std::endl;
    log << tmp.str();

    if(src == ownID) {
        std::lock_guard<std::mutex> lk(m);
        numOwnDelivered++;
        cv.notify_one();
    }
}

void Broadcaster::waitForStop() {
    fifo.stop();
}
