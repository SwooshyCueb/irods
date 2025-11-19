#ifndef IRODS_HASHER_CRC32_STRATEGY_HPP
#define IRODS_HASHER_CRC32_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string CRC32_NAME;

    class CRC32Strategy : public HashStrategy
    {
      public:
        CRC32Strategy() {}
        virtual ~CRC32Strategy() {}

        std::string name() const override
        {
            return CRC32_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string&, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_CRC32_STRATEGY_HPP
