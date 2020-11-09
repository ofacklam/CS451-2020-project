//
// Created by ofacklam on 09/11/2020.
//

#ifndef DA_PROJECT_PERFECTLINKDATA_HPP
#define DA_PROJECT_PERFECTLINKDATA_HPP

#include <iostream>

#include "../fairLossLink/serializable.hpp"
#include "sequenceNumberStore.hpp"


template<class T>
class DataPacket : public Serializable {
public:
    sequence id;
    T payload;
    bool isData;

    DataPacket() : id(0), isData(false) {}

    explicit DataPacket(sequence seq) : id(seq), isData(false) {}

    DataPacket(sequence id, T payload) : id(id), payload(payload), isData(true) {}

    void serialize(std::ostream &os) override;

    void deserialize(std::istream &is) override;
};

template<class T>
void DataPacket<T>::serialize(std::ostream &os) {
    sequence netId = Utils::htonT(id);
    os.write(reinterpret_cast<char *>(&netId), sizeof(netId));

    if (isData) {
        payload.serialize(os);
    }
}

template<class T>
void DataPacket<T>::deserialize(std::istream &is) {
    sequence netId;
    is.read(reinterpret_cast<char *>(&netId), sizeof(netId));
    id = Utils::ntohT(netId);

    if (is.eof()) {
        isData = false;
    } else {
        payload.deserialize(is);
        isData = true;
    }
}

#endif //DA_PROJECT_PERFECTLINKDATA_HPP
