#include "irods/BLAKE2B512Strategy.hpp"

#include "irods/base64.hpp"
#include "irods/checksum.h"
#include "irods/irods_error.hpp"
#include "irods/rodsDef.h"
#include "irods/rodsErrorTable.h"

#include <boost/any.hpp>

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

#include <fmt/format.h>

#include <array>
#include <string>
#include <cstring>
#include <utility>

namespace irods
{

    const std::string BLAKE2B512_NAME("blake2b512");

    error BLAKE2B512Strategy::init(boost::any& _context) const
    {
        EVP_MD* message_digest = EVP_MD_fetch(nullptr, BLAKE2B512_NAME.c_str(), nullptr);

        // Initialize the digest context for the derived digest implementation. Must use _ex or _ex2 to avoid
        // automatically resetting the context with EVP_MD_CTX_reset.
        EVP_MD_CTX* context = EVP_MD_CTX_new();
        if (0 == EVP_DigestInit_ex2(context, message_digest, nullptr)) {
            EVP_MD_free(message_digest);
            EVP_MD_CTX_free(context);
            const auto ssl_error_code = ERR_get_error();
            auto msg = fmt::format("{}: Failed to initialize digest. error code: [{}]", __func__, ssl_error_code);
            return ERROR(DIGEST_INIT_FAILED, std::move(msg));
        }

        _context = context;

        EVP_MD_free(message_digest);

        return SUCCESS();
    }

    error BLAKE2B512Strategy::update(const std::string& data, boost::any& _context) const
    {
        EVP_MD_CTX* context = boost::any_cast<EVP_MD_CTX*>(_context);

        // Hash the specified buffer of bytes and store the results in the context.
        if (0 == EVP_DigestUpdate(context, data.c_str(), data.size())) {
            // Free the context here to ensure that no leaks occur. If the caller wants to try again, it needs to start
            // from init() again. Set the input to nullptr to ensure that the memory can no longer be accessed after
            // freeing.
            EVP_MD_CTX_free(context);
            _context = nullptr;
            const auto ssl_error_code = ERR_get_error();
            auto msg = fmt::format("{}: Failed to calculate digest. error code: [{}]", __func__, ssl_error_code);
            return ERROR(DIGEST_UPDATE_FAILED, std::move(msg));
        }

        return SUCCESS();
    }

    error BLAKE2B512Strategy::digest(std::string& _messageDigest, boost::any& _context) const
    {
        EVP_MD_CTX* context = boost::any_cast<EVP_MD_CTX*>(_context);

        const std::size_t DIGEST_LENGTH = 512 / 8;

        // Finally, retrieve the digest value from the context and place it into final_buffer. Use the _ex function here
        // so that the digest context is not automatically cleaned up with EVP_MD_CTX_reset.
        std::array<unsigned char, DIGEST_LENGTH> final_buffer;
        if (0 == EVP_DigestFinal_ex(context, final_buffer.data(), nullptr)) {
            // Free the context here to ensure that no leaks occur. If the caller wants to try again, it needs to start
            // from init() again. Set the input to nullptr to ensure that the memory can no longer be accessed after
            // freeing.
            EVP_MD_CTX_free(context);
            _context = nullptr;
            const auto ssl_error_code = ERR_get_error();
            auto msg = fmt::format("{}: Failed to finalize digest. error code: [{}]", __func__, ssl_error_code);
            return ERROR(DIGEST_FINAL_FAILED, std::move(msg));
        }

        // The digest has been extracted from the context and placed in a buffer. The context is no longer needed, so
        // it is freed here. The input is set to nullptr to ensure that the memory can no longer be accessed after
        // freeing.
        EVP_MD_CTX_free(context);
        _context = nullptr;

        std::size_t out_len = LONG_CHKSUM_LEN - std::strlen(BLAKE2B512_CHKSUM_PREFIX);

        std::array<unsigned char, LONG_CHKSUM_LEN> out_buffer;
        int rc = base64_encode(final_buffer.data(), DIGEST_LENGTH, out_buffer.data(), &out_len);
        if (0 != rc) {
            auto msg = fmt::format("{}: Failed to base64 encode hash.", __func__);
            return ERROR(rc, std::move(msg));
        }

        _messageDigest = BLAKE2B512_CHKSUM_PREFIX;
        _messageDigest += std::string(reinterpret_cast<char*>(out_buffer.data()), out_len);

        return SUCCESS();
    }

    bool BLAKE2B512Strategy::isChecksum(const std::string& _chksum) const
    {
        return _chksum.starts_with(BLAKE2B512_CHKSUM_PREFIX);
    }
}; // namespace irods
