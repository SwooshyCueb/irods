#include "irods/irods_hasher_factory.hpp"
#include "irods/checksum.h"
#include "irods/MD5Strategy.hpp"
#include "irods/SHA224Strategy.hpp"
#include "irods/SHA256Strategy.hpp"
#include "irods/SHA384Strategy.hpp"
#include "irods/SHA512Strategy.hpp"
#include "irods/SHA3_224Strategy.hpp"
#include "irods/SHA3_256Strategy.hpp"
#include "irods/SHA3_384Strategy.hpp"
#include "irods/SHA3_512Strategy.hpp"
#include "irods/ADLER32Strategy.hpp"
#include "irods/BLAKE2S256Strategy.hpp"
#include "irods/BLAKE2B512Strategy.hpp"
#include "irods/SHA1Strategy.hpp"
#include "irods/CRC64NVMEStrategy.hpp"
#include "irods/rodsErrorTable.h"

#include <boost/container_hash/hash.hpp>

#include <sstream>
#include <unordered_map>

namespace irods {

    namespace {
        const SHA224Strategy _sha224;
        const SHA256Strategy _sha256;
        const SHA384Strategy _sha384;
        const SHA512Strategy _sha512;
        const SHA3_224Strategy _sha3_224;
        const SHA3_256Strategy _sha3_256;
        const SHA3_384Strategy _sha3_384;
        const SHA3_512Strategy _sha3_512;
        const ADLER32Strategy _adler32;
        const BLAKE2S256Strategy _blake2s256;
        const BLAKE2B512Strategy _blake2b512;
        const MD5Strategy _md5;
        const SHA1Strategy _sha1;
        const CRC64NVMEStrategy _crc64nvme;

        auto make_map() {
            std::unordered_map<const std::string, const HashStrategy*, boost::hash<const std::string>> map;
            map[SHA224_NAME] = &_sha224;
            map[SHA256_NAME] = &_sha256;
            map[SHA384_NAME] = &_sha384;
            map[SHA512_NAME] = &_sha512;
            map[SHA3_224_NAME] = &_sha3_224;
            map[SHA3_256_NAME] = &_sha3_256;
            map[SHA3_384_NAME] = &_sha3_384;
            map[SHA3_512_NAME] = &_sha3_512;
            map[MD5_NAME] = &_md5;
            map[ADLER32_NAME] = &_adler32;
            map[BLAKE2S256_NAME] = &_blake2s256;
            map[BLAKE2B512_NAME] = &_blake2b512;
            map[SHA1_NAME] = &_sha1;
            map[CRC64NVME_NAME] = &_crc64nvme;
            return map;
        }

    };

    error
    getHasher( const std::string& _name, Hasher& _hasher ) {

        const auto _strategies{ make_map() };

        auto it = _strategies.find( _name );
        if ( _strategies.end() == it ) {
            std::stringstream msg;
            msg << "Unknown hashing scheme [" << _name << "]";
            return ERROR( SYS_INVALID_INPUT_PARAM, msg.str() );
        }
        return PASS(_hasher.init(it->second));
    }

    error
    get_hash_scheme_from_checksum(
        const std::string& _chksum,
        std::string&       _scheme ) {

        const auto _strategies{ make_map() };

        if ( _chksum.empty() ) {
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       "empty chksum string" );
        }
        for ( const auto& [key, strategy] : _strategies) {
            if ( strategy->isChecksum( _chksum ) ) {
                _scheme = strategy->name();
                return SUCCESS();
            }
        }
        return ERROR(
                   SYS_INVALID_INPUT_PARAM,
                   "hash scheme not found" );

    } // get_hasher_scheme_from_checksum

}; // namespace irods


