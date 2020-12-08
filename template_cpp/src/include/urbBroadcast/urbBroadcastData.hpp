//
// Created by ofacklam on 10/11/2020.
//

#ifndef DA_PROJECT_URBBROADCASTDATA_HPP
#define DA_PROJECT_URBBROADCASTDATA_HPP

#include "utils/serializable.hpp"
#include "utils/sequenceNumberStore.hpp"
#include "utils/utils.hpp"


/**
 * Note: a message is uniquely identified by the (emitter, seq) couple
 */
template<class T>
class UrbDataPacket : public Serializable {
public:
    static size_t size();

public:
    unsigned long emitter;
    sequence seq;
    T data;

    UrbDataPacket() : emitter(0), seq(0), data{} {}

    UrbDataPacket(unsigned long emitter, sequence seq, T data) : emitter(emitter), seq(seq), data(data) {}

    void serialize(std::ostream &os) override;

    void deserialize(std::istream &is) override;
};

template<class T>
size_t UrbDataPacket<T>::size() {
    return sizeof(emitter) + sizeof(seq) + T::size();
}

template<class T>
void UrbDataPacket<T>::serialize(std::ostream &os) {
    Utils::serializeNumericType(emitter, os);
    Utils::serializeNumericType(seq, os);

    data.serialize(os);
}

template<class T>
void UrbDataPacket<T>::deserialize(std::istream &is) {
    emitter = Utils::deserializeNumericType<unsigned long>(is);
    seq = Utils::deserializeNumericType<sequence>(is);

    data.deserialize(is);
}

#endif //DA_PROJECT_URBBROADCASTDATA_HPP
