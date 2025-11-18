#ifndef IRODS_HASHER_SHA224_STRATEGY_HPP
#define IRODS_HASHER_SHA224_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string SHA224_NAME;

    class SHA224Strategy : public HashStrategy
    {
      public:
        SHA224Strategy() {};
        virtual ~SHA224Strategy() {};

        std::string name() const override
        {
            return SHA224_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string& data, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_SHA224_STRATEGY_HPP
