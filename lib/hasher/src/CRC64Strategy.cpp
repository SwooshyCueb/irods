#include "irods/CRC64Strategy.hpp"

#include "irods/base64.hpp"
#include "irods/checksum.h"
#include "irods/irods_error.hpp"
#include "irods/rodsErrorTable.h"

#include <boost/any.hpp>
#include <boost/crc.hpp>

#include <array>
#include <cstdint>
#include <cstring>

// not sure about this one
namespace irods
{
    const std::string CRC64_NAME("crc64");

    static constexpr std::size_t crc_bits = 64;

    // The CRC polynomial defines the feedback terms used in the CRC computation.
    // It represents the divisor polynomial in the binary polynomial division process
    // used by the CRC algorithm. Each bit set to '1' in this constant indicates a term
    // in the polynomial. ECMA-182 specifies the 64-bit polynomial:
    // x^64 + x^62 + x^57 + x^55 + x^54 + x^53 + x^52 + x^47 + x^46 + x^45 + x^40 + x^39 + x^38 + x^37 + x^35 + x^33 +
    // x^32 + x^31 + x^29 + x^27 + x^24 + x^23 + x^22 + x^21 + x^19 + x^17 + x^13 + x^12 + x^10 + x^9  + x^7  + x^4  + x + 1
    // (represented here as 0x42f0e1eba9ea3693)
    // This polynomial determines the bit mixing pattern that creates the final checksum.
    static constexpr std::uint64_t crc_polynomial = 0x42f0e1eba9ea3693;

    // The initial remainder (also known as the initial value or seed) specifies the
    // starting value loaded into the CRC register before processing any data.
    // ECMA-182 specifies all bits set to 0 (0x0000000000000000).
    static constexpr std::uint64_t crc_initial_remainder = 0x0000000000000000;

    // The final XOR value (also called the "final remainder") is XORed with the
    // CRC register after all data has been processed. ECMA-182 also specifies all bits
    // set to 0 (0x0000000000000000).
    static constexpr std::uint64_t crc_final_xor = 0x0000000000000000;

    static constexpr bool crc_reflect_input = false;
    static constexpr bool crc_reflect_output = false;

    using crc_hasher_type = typename boost::crc_optimal<crc_bits, crc_polynomial, crc_initial_remainder, crc_final_xor, crc_reflect_input, crc_reflect_output>;

    auto CRC64Strategy::init(boost::any& _context) const -> error
    {
        // Create the CRC calculator object
        crc_hasher_type* context = new crc_hasher_type();

        _context = context;

        return SUCCESS();
    }

    auto CRC64Strategy::update(const std::string& data, boost::any& _context) const -> error
    {
        crc_hasher_type* context;
        try {
            context = boost::any_cast<crc_hasher_type*>(_context);
        }
        catch (const boost::bad_any_cast& e) {
            return ERROR(SYS_INVALID_INPUT_PARAM,
                         "The context sent to CRC64Strategy::update was not a proper boost::crc_optimal<>*");
        }

        if (nullptr == context) {
            return ERROR(INVALID_INPUT_ARGUMENT_NULL_POINTER, "A null context was sent to CRC64Strategy::update.");
        }

        context->process_bytes(data.c_str(), data.length());
        return SUCCESS();
    }

    auto CRC64Strategy::digest(std::string& messageDigest, boost::any& _context) const -> error
    {
        crc_hasher_type* context;
        try {
            context = boost::any_cast<crc_hasher_type*>(_context);
        }
        catch (const boost::bad_any_cast& e) {
            return ERROR(SYS_INVALID_INPUT_PARAM,
                         "The context sent to CRC64Strategy::digest was not a proper boost::crc_optimal<>*");
        }

        if (nullptr == context) {
            return ERROR(INVALID_INPUT_ARGUMENT_NULL_POINTER, "A null context was sent to CRC64Strategy::digest.");
        }

        std::uint64_t checksum = context->checksum();

        // Convert uint64_t to bytes (big-endian for consistency)
        std::array<unsigned char, 8> bytes;
        for (std::size_t i = 0; i < bytes.size(); ++i) {
            bytes[7 - i] = static_cast<unsigned char>((checksum >> (i * 8)) & 0xFF);
        }

        unsigned long out_len = CHKSUM_LEN - std::strlen(CRC64_CHKSUM_PREFIX);

        std::array<unsigned char, CHKSUM_LEN> out_buffer;
        int rc = base64_encode(bytes.data(), bytes.size(), out_buffer.data(), &out_len);
        if (0 != rc) {
            delete context;
            _context = nullptr;
            return ERROR(rc, fmt::format("{}: Failed to base64 encode hash.", __func__));
        }

        // The checksum has been extracted from the context.  The context can be deleted.
        delete context;
        _context = nullptr;

        messageDigest = CRC64_CHKSUM_PREFIX;
        messageDigest += std::string(reinterpret_cast<char*>(out_buffer.data()), out_len);

        return SUCCESS();
    }

    bool CRC64Strategy::isChecksum(const std::string& _chksum) const
    {
        return _chksum.starts_with(CRC64_CHKSUM_PREFIX);
    }
} // namespace irods
