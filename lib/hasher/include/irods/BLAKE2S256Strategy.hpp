#ifndef IRODS_HASHER_BLAKE2S256_STRATEGY_HPP
#define IRODS_HASHER_BLAKE2S256_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string BLAKE2S256_NAME;

    class BLAKE2S256Strategy : public HashStrategy
    {
      public:
        BLAKE2S256Strategy() {};
        virtual ~BLAKE2S256Strategy() {};

        std::string name() const override
        {
            return BLAKE2S256_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string& data, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_BLAKE2S256_STRATEGY_HPP
