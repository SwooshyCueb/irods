#ifndef IRODS_HASHER_RIPEMD160_STRATEGY_HPP
#define IRODS_HASHER_RIPEMD160_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string RIPEMD160_NAME;

    class RIPEMD160Strategy : public HashStrategy
    {
      public:
        RIPEMD160Strategy() {};
        virtual ~RIPEMD160Strategy() {};

        std::string name() const override
        {
            return RIPEMD160_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string& data, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_RIPEMD160_STRATEGY_HPP
