#ifndef IRODS_HASHER_BLAKE2B512_STRATEGY_HPP
#define IRODS_HASHER_BLAKE2B512_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string BLAKE2B512_NAME;

    class BLAKE2B512Strategy : public HashStrategy
    {
      public:
        BLAKE2B512Strategy() {};
        virtual ~BLAKE2B512Strategy() {};

        std::string name() const override
        {
            return BLAKE2B512_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string& data, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_BLAKE2B512_STRATEGY_HPP
