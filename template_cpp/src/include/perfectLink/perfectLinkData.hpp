//
// Created by ofacklam on 09/11/2020.
//

#ifndef DA_PROJECT_PERFECTLINKDATA_HPP
#define DA_PROJECT_PERFECTLINKDATA_HPP

#include <iostream>

#include "utils/serializable.hpp"
#include "utils/sequenceNumberStore.hpp"


template<class T>
class PlDataPacket : public Serializable {
public:
    static size_t size();

public:
    sequence id;
    std::shared_ptr<T> payload;
    bool isData;

    PlDataPacket() : id(0), payload(std::make_shared<T>()), isData(false) {}

    explicit PlDataPacket(sequence seq) : id(seq), payload(std::make_shared<T>()), isData(false) {}

    PlDataPacket(sequence id, std::shared_ptr<T> payload) : id(id), payload(payload), isData(true) {}

    void serialize(std::ostream &os) override;

    void deserialize(std::istream &is) override;
};

template<class T>
size_t PlDataPacket<T>::size() {
    return sizeof(id) + T::size();
}

template<class T>
void PlDataPacket<T>::serialize(std::ostream &os) {
    Utils::serializeNumericType(id, os);

    if (isData) {
        payload->serialize(os);
    }
}

template<class T>
void PlDataPacket<T>::deserialize(std::istream &is) {
    id = Utils::deserializeNumericType<sequence>(is);

    if (is.peek() == std::char_traits<char>::eof()) {
        isData = false;
    } else {
        payload->deserialize(is);
        isData = true;
    }
}

#endif //DA_PROJECT_PERFECTLINKDATA_HPP
