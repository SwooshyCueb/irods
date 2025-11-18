#ifndef IRODS_HASHER_SHA3_384_STRATEGY_HPP
#define IRODS_HASHER_SHA3_384_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string SHA3_384_NAME;

    class SHA3_384Strategy : public HashStrategy
    {
      public:
        SHA3_384Strategy() {};
        virtual ~SHA3_384Strategy() {};

        std::string name() const override
        {
            return SHA3_384_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string& data, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_SHA3_384_STRATEGY_HPP
