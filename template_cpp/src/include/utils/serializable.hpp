//
// Created by ofacklam on 08/11/2020.
//

#ifndef DA_PROJECT_SERIALIZABLE_HPP
#define DA_PROJECT_SERIALIZABLE_HPP

/**
 * Idea from https://isocpp.org/wiki/faq/serialization
 * Some more help from https://stackoverflow.com/a/1559584
 */
class Serializable {
public:
    virtual void serialize(std::ostream &os) = 0;

    virtual void deserialize(std::istream &is) = 0;
};

#endif //DA_PROJECT_SERIALIZABLE_HPP
