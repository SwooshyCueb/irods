#include "irods/XXH32Strategy.hpp"

#include "irods/checksum.h"
#include "irods/irods_error.hpp"
#include "irods/rodsErrorTable.h"

#include <boost/any.hpp>
#include <boost/config.hpp>

#include <array>
#include <bit>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

namespace irods
{
    const std::string XXH32_NAME("xxh32");

    namespace
    {
        // xxHash primes
        constexpr std::uint32_t xxh32_prime1 = 0x9E3779B1;
        constexpr std::uint32_t xxh32_prime2 = 0x85EBCA77;
        constexpr std::uint32_t xxh32_prime3 = 0xC2B2AE3D;
        constexpr std::uint32_t xxh32_prime4 = 0x27D4EB2F;
        constexpr std::uint32_t xxh32_prime5 = 0x165667B1;

        // size of chunks for processing, in units of std::uint32_t
        constexpr std::size_t xxh32_chunk_size32 = 4;
        // size of chunks for processing, in units of std::uint8_t
        constexpr std::size_t xxh32_chunk_size8 = xxh32_chunk_size32 * (sizeof(std::uint32_t) / sizeof(std::uint8_t));

        class XXH32State
        {
          private:
            // accumulator lanes
            std::array<std::uint32_t, 4> lanes;
            // total length processed
            std::size_t total_len{0};
            // buffer for partial chunks
            std::array<std::uint8_t, xxh32_chunk_size8> buffer;
            // current position in partial chunk buffer
            std::uint_fast8_t buffer_pos{0};

            BOOST_FORCEINLINE constexpr void round(const std::uint32_t* input)
            {
                for (std::uint_fast8_t idx = 0; idx < xxh32_chunk_size32; idx++) {
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index, cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    lanes[idx] += input[idx] * xxh32_prime2;
                    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers, cppcoreguidelines-pro-bounds-constant-array-index)
                    lanes[idx] = std::rotl(lanes[idx], 13) * xxh32_prime1;
                }
            }

          public:
            BOOST_FORCEINLINE constexpr XXH32State(const std::uint32_t seed = 0)
            {
                lanes[0] = seed + xxh32_prime1 + xxh32_prime2;
                lanes[1] = seed + xxh32_prime2;
                lanes[2] = seed;
                lanes[3] = seed - xxh32_prime1;
            }

            BOOST_FORCEINLINE constexpr void update(const std::uint8_t* input, const std::size_t length)
            {
                if (length == 0) {
                    return;
                }

                total_len += length;

                if (buffer_pos + length < xxh32_chunk_size8) {
                    // we have leftovers from a previous update call, but the input isn't enough to make a chunk
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
                    std::memcpy(&buffer[buffer_pos], input, length);
                    buffer_pos += length;
                    return;
                }

                std::size_t pos = 0;

                if (buffer_pos > 0) {
                    // we have leftovers from a previous update call, and enough input to make a chunk
                    pos = xxh32_chunk_size8 - buffer_pos;
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
                    std::memcpy(&buffer[buffer_pos], input, pos);
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                    round(reinterpret_cast<std::uint32_t*>(buffer.data()));
                    buffer_pos = 0;
                }

                for (; pos + xxh32_chunk_size8 <= length; pos += xxh32_chunk_size8) {
                    // process chunks
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-constant-array-index, cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    round(reinterpret_cast<const std::uint32_t*>(&input[pos]));
                }

                if (pos < length) {
                    // leftovers for next time
                    buffer_pos = length - pos;
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index, cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    std::memcpy(buffer.data(), &input[pos], buffer_pos);
                }
            }

            BOOST_FORCEINLINE constexpr std::uint32_t digest()
            {
                std::uint32_t hash = static_cast<std::uint32_t>(total_len);
                if (hash < xxh32_chunk_size8) {
                    hash += lanes[2] + xxh32_prime5;
                }
                else {
                    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
                    hash += std::rotl(lanes[0], 1);
                    hash += std::rotl(lanes[1], 7);
                    hash += std::rotl(lanes[2], 12);
                    hash += std::rotl(lanes[3], 18);
                    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
                }

                // finalize
                std::uint_fast8_t pos = 0;
                for (; pos + xxh32_chunk_size32 <= buffer_pos; pos += xxh32_chunk_size32) {
                    // process4
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast, cppcoreguidelines-pro-bounds-constant-array-index, cppcoreguidelines-pro-bounds-pointer-arithmetic)
                    hash += reinterpret_cast<uint32_t*>(&buffer[pos])[0] * xxh32_prime3;
                    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
                    hash = std::rotl(hash, 17) * xxh32_prime4;
                }
                for (; pos < buffer_pos; pos++) {
                    // process1
                    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
                    hash += buffer[pos] * xxh32_prime5;
                    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
                    hash = std::rotl(hash, 11) * xxh32_prime1;
                }

                // avalanche
                // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
                hash ^= hash >> 15U;
                hash *= xxh32_prime2;
                hash ^= hash >> 13U;
                hash *= xxh32_prime3;
                hash ^= hash >> 16U;
                // NOLINTEND(cppcoreguidelines-avoid-magic-numbers, readability-magic-numbers)
                return hash;
            }
        };
    }

    error XXH32Strategy::init(boost::any& _context) const
    {
        _context = XXH32State();
        return SUCCESS();
    }

    error XXH32Strategy::update(const std::string& data, boost::any& _context) const
    {
        try {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            boost::any_cast<XXH32State&>(_context).update(reinterpret_cast<const std::uint8_t*>(data.c_str()), data.size());
        }
        catch (const boost::bad_any_cast& e) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "Invalid context passed to XXH32Strategy::update");
        }
        return SUCCESS();
    }

    error XXH32Strategy::digest(std::string& messageDigest, boost::any& _context) const
    {
        std::uint32_t checksum; // NOLINT(cppcoreguidelines-init-variables)
        try {
            checksum = boost::any_cast<XXH32State&>(_context).digest();
        }
        catch (const boost::bad_any_cast& e) {
            return ERROR(SYS_INVALID_INPUT_PARAM, "Invalid context passed to XXH32Strategy::digest");
        }

        const std::size_t DIGEST_LENGTH = 4;

        std::stringstream digest_ss;
        digest_ss << XXH32_CHKSUM_PREFIX << std::setfill('0') << std::hex << std::setw(DIGEST_LENGTH * 2) << checksum;

        // XXH32State cannot be reused after calling digest
        _context = nullptr;

        messageDigest = digest_ss.str();

        return SUCCESS();
    }

    bool XXH32Strategy::isChecksum(const std::string& _chksum) const
    {
        return _chksum.starts_with(XXH32_CHKSUM_PREFIX);
    }
} // namespace irods
