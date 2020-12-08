//
// Created by ofacklam on 10/11/2020.
//

#include "broadcaster.hpp"


Broadcaster::Broadcaster(unsigned long id, const std::vector<Parser::Host> &hosts,
                         const std::string &configPath, std::string outputPath,
                         unsigned long long capacity)
        : outputPath(std::move(outputPath)),
          lcb(id, hosts, [this](auto &&msg, auto &&src) { deliver(msg, src); }, capacity),
          ownID(id) {
    // Get number of messages to broadcast
    std::ifstream in(configPath);
    in >> numMessages;

    // Parse dependency lines
    std::vector<unsigned long> deps;
    std::string line;
    while(std::getline(in, line)) {
        std::istringstream iss(line);

        unsigned long hostID;
        iss >> hostID;
        if(hostID != id)
            continue;


        unsigned long dep;
        while(iss >> dep) {
            deps.push_back(dep);
        }
    }
    lcb.setDependencies(deps);
}

void Broadcaster::broadcast() {
    // Broadcast all messages
    for (sequence s = 1; s <= numMessages; s++) {
        {
            std::lock_guard<std::mutex> lk(mLog);
            log << "b " << s << std::endl;
        }
        lcb.lcBroadcast(Integer(s));
    }

    // Wait for finish
    std::unique_lock<std::mutex> lk(mNumDelivered);
    cv.wait(lk, [this] { return numOwnDelivered == numMessages; });
}

void Broadcaster::writeOutput() {
    std::lock_guard<std::mutex> lk(mLog);

    std::ofstream out(outputPath);
    out << log.str();
    out.flush();

    log.str("");
}

void Broadcaster::deliver(const Integer<sequence>& msg, unsigned long src) {
    // Write to output
    {
        std::lock_guard<std::mutex> lk(mLog);
        log << "d " << src << " " << msg.val_ << std::endl;
    }

    if(src == ownID) {
        std::lock_guard<std::mutex> lk(mNumDelivered);
        numOwnDelivered++;
        cv.notify_one();
    }
}

void Broadcaster::waitForStop() {
    lcb.stop();
}
