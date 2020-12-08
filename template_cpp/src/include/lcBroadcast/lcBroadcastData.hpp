//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_LCBROADCASTDATA_HPP
#define DA_PROJECT_LCBROADCASTDATA_HPP

#include <unordered_map>

#include "utils/serializable.hpp"
#include "utils/sequenceNumberStore.hpp"
#include "utils/utils.hpp"


template<class T>
class LcbDataPacket : public Serializable {
public:
    static unsigned long numTotalHosts;

    static size_t size();

public:
    T data;
    std::unordered_map<unsigned long, sequence> dependencies;

    LcbDataPacket() : data{} {}

    explicit LcbDataPacket(T data) : data(data) {}

    void serialize(std::ostream &os) override;

    void deserialize(std::istream &is) override;
};

template<class T>
unsigned long LcbDataPacket<T>::numTotalHosts = 0;

template<class T>
size_t LcbDataPacket<T>::size() {
    return T::size() + numTotalHosts * (sizeof(unsigned long) + sizeof(sequence));
}

template<class T>
void LcbDataPacket<T>::serialize(std::ostream &os) {
    data.serialize(os);

    for (auto &entry: dependencies) {
        auto &host = entry.first;
        auto &seq = entry.second;

        Utils::serializeNumericType(host, os);
        Utils::serializeNumericType(seq, os);
    }
}

template<class T>
void LcbDataPacket<T>::deserialize(std::istream &is) {
    data.deserialize(is);

    while (is.peek() != std::char_traits<char>::eof()) {
        auto host = Utils::deserializeNumericType<unsigned long>(is);
        auto seq = Utils::deserializeNumericType<sequence>(is);

        dependencies[host] = seq;
    }
}

#endif //DA_PROJECT_LCBROADCASTDATA_HPP
