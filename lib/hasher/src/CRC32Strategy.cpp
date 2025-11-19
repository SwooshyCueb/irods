#include "irods/CRC32Strategy.hpp"

#include "irods/checksum.h"
#include "irods/irods_error.hpp"
#include "irods/rodsErrorTable.h"

#include <boost/any.hpp>
#include <boost/crc.hpp>

#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace irods
{
    const std::string CRC32_NAME("crc32");

    static constexpr std::size_t crc_bits = 32;

    // The CRC polynomial defines the feedback terms used in the CRC computation.
    // It represents the divisor polynomial in the binary polynomial division process
    // used by the CRC algorithm. Each bit set to '1' in this constant indicates a term
    // in the polynomial. ISO/IEC 13239 specifies the 32-bit polynomial:
    // x^32 + x^26 + x^23 + x^22 + x^16 + x^12 + x^11 + x^10 + x^8 + x^7 + x^5 + x^4 + x^2 + x + 1
    // (represented here as 0x04c11db7)
    // This polynomial determines the bit mixing pattern that creates the final checksum.
    static constexpr std::uint32_t crc_polynomial = 0x04c11db7;

    // The initial remainder (also known as the initial value or seed) specifies the
    // starting value loaded into the CRC register before processing any data.
    // ISO/IEC 13239 specifies all bits set to 1 (0xffffffff).
    static constexpr std::uint32_t crc_initial_remainder = 0xffffffff;

    // The final XOR value (also called the "final remainder") is XORed with the
    // CRC register after all data has been processed. ISO/IEC 13239 specifies all bits
    // set to 1 (0xffffffff). This step effectively inverts the CRC result,
    // providing better error-detection symmetry for certain data patterns.
    static constexpr std::uint32_t crc_final_xor = 0xffffffff;

    static constexpr bool crc_reflect_input = true;
    static constexpr bool crc_reflect_output = true;

    using crc_hasher_type = typename boost::crc_optimal<crc_bits, crc_polynomial, crc_initial_remainder, crc_final_xor, crc_reflect_input, crc_reflect_output>;

    auto CRC32Strategy::init(boost::any& _context) const -> error
    {
        // Create the CRC calculator object
        crc_hasher_type* context = new crc_hasher_type();

        _context = context;

        return SUCCESS();
    }

    auto CRC32Strategy::update(const std::string& data, boost::any& _context) const -> error
    {
        crc_hasher_type* context;
        try {
            context = boost::any_cast<crc_hasher_type*>(_context);
        }
        catch (const boost::bad_any_cast& e) {
            return ERROR(SYS_INVALID_INPUT_PARAM,
                         "The context sent to CRC32Strategy::update was not a proper boost::crc_optimal<>*");
        }

        if (nullptr == context) {
            return ERROR(INVALID_INPUT_ARGUMENT_NULL_POINTER, "A null context was sent to CRC32Strategy::update.");
        }

        context->process_bytes(data.c_str(), data.length());
        return SUCCESS();
    }

    auto CRC32Strategy::digest(std::string& messageDigest, boost::any& _context) const -> error
    {
        crc_hasher_type* context;
        try {
            context = boost::any_cast<crc_hasher_type*>(_context);
        }
        catch (const boost::bad_any_cast& e) {
            return ERROR(SYS_INVALID_INPUT_PARAM,
                         "The context sent to CRC32Strategy::digest was not a boost::crc_optimal<>*");
        }

        if (nullptr == context) {
            return ERROR(INVALID_INPUT_ARGUMENT_NULL_POINTER, "A null context was sent to CRC32Strategy::digest.");
        }

        const std::size_t DIGEST_LENGTH = crc_bits / 8;

        std::uint32_t checksum = context->checksum();

        std::stringstream ss;
        ss << std::setfill('0') << std::hex << std::setw(DIGEST_LENGTH * 2) << checksum;

        // The checksum has been extracted from the context.  The context can be deleted.
        delete context;
        _context = nullptr;

        messageDigest = CRC32_CHKSUM_PREFIX;
        messageDigest += ss.str();

        return SUCCESS();
    }

    bool CRC32Strategy::isChecksum(const std::string& _chksum) const
    {
        return _chksum.starts_with(CRC32_CHKSUM_PREFIX);
    }
} // namespace irods
