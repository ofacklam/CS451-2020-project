//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_BROADCASTER_HPP
#define DA_PROJECT_BROADCASTER_HPP

#include "fifoBroadcast/fifoBroadcast.hpp"


template<class T>
class Integer : public Serializable {
public:
    T val_;

    Integer() = default;

    explicit Integer(T val) : val_(val) {}

    void serialize(std::ostream &os) override {
        Utils::serializeNumericType(val_, os);
    }

    void deserialize(std::istream &is) override {
        val_ = Utils::deserializeNumericType<T>(is);
    }
};

class Broadcaster : public Stoppable {
private:
    // Config info
    unsigned long long numMessages = 0;
    std::ostringstream log;
    std::string outputPath;

    // Broadcasting abstraction
    FifoBroadcast<Integer<sequence>> fifo;

    // Reception --> for timing end of broadcast
    unsigned long ownID;
    unsigned long long numOwnDelivered = 0;
    std::mutex m;
    std::condition_variable cv;

public:
    Broadcaster(unsigned long id,
                const std::vector<Parser::Host> &hosts,
                const std::string &configPath,
                std::string outputPath,
                unsigned long long capacity = 100);

    void broadcast();

    void writeOutput();

private:
    void deliver(Integer<sequence> msg, unsigned long src);

protected:
    void waitForStop() override;
};

#endif //DA_PROJECT_BROADCASTER_HPP
