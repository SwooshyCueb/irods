#ifndef IRODS_HASHER_XXH32_STRATEGY_HPP
#define IRODS_HASHER_XXH32_STRATEGY_HPP

#include "irods/HashStrategy.hpp"
#include "irods/irods_error.hpp"

#include <boost/any.hpp>

#include <string>

namespace irods
{
    extern const std::string XXH32_NAME;

    class XXH32Strategy : public HashStrategy
    {
      public:
        XXH32Strategy() {}
        virtual ~XXH32Strategy() {}

        std::string name() const override
        {
            return XXH32_NAME;
        }
        error init(boost::any& context) const override;
        error update(const std::string&, boost::any& context) const override;
        error digest(std::string& messageDigest, boost::any& context) const override;
        bool isChecksum(const std::string&) const override;
    };
} // namespace irods

#endif // IRODS_HASHER_XXH32_STRATEGY_HPP
