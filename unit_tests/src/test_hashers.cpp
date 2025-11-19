#include <catch2/catch_all.hpp>

#include "irods/ADLER32Strategy.hpp"
#include "irods/CRC32Strategy.hpp"
#include "irods/CRC32CStrategy.hpp"
#include "irods/CRC64NVMEStrategy.hpp"
#include "irods/BLAKE2S256Strategy.hpp"
#include "irods/BLAKE2B512Strategy.hpp"
#include "irods/MD5Strategy.hpp"
#include "irods/RIPEMD160Strategy.hpp"
#include "irods/SHA1Strategy.hpp"
#include "irods/SHA224Strategy.hpp"
#include "irods/SHA256Strategy.hpp"
#include "irods/SHA384Strategy.hpp"
#include "irods/SHA512Strategy.hpp"
#include "irods/SHA3_224Strategy.hpp"
#include "irods/SHA3_256Strategy.hpp"
#include "irods/SHA3_384Strategy.hpp"
#include "irods/SHA3_512Strategy.hpp"
#include "irods/irods_hasher_factory.hpp"
#include "irods/Hasher.hpp"

#include <fmt/format.h>

#include <string>
#include <tuple>
#include <vector>

TEST_CASE("checksum hashers", "[string]")
{
    // clang-format off
    const std::tuple<const std::string, const std::string, const std::string> hash_test_vals = GENERATE(
        std::make_tuple("asdf1234ASDF!@#$", irods::MD5_NAME,        "70f597ce53373700ba5e5dfc892bac59"),
        std::make_tuple("asdf1234ASDF!@#$", irods::RIPEMD160_NAME,  "ripemd160:UVHzP22Tjw4a5I6tq6IAov6BhoQ="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA1_NAME,       "sha1:w4pAayodnxhmStdLObUARZUQu68="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA224_NAME,     "sha224:5j1Y61ecdH9cNtodIbqXfcamsyfvxWtOiXxOhw=="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA256_NAME,     "sha2:jwyFBi2ugt4geZKPMJzJlE8eQ/M8qLpAbKNzS0uUBG4="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA384_NAME,     "sha384:BD//lWRFUEz7mMG4zoggITtQjlSF8MBg/fjJl1+uMY57zPizttAJWFTLhrgl4BfI"),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA512_NAME,     "sha512:oOcXC8A34DybRSivQjyGExYLzEmHXzh0KUtZZzE72EDQ3lTcWhOkF0XmLkzX85QrJT80Ral+v4/zDQthDvoj8A=="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA3_224_NAME,   "sha3-224:kDBLP5GPa98mLMoe8/jPB2HGoFfplVgsn692qQ=="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA3_256_NAME,   "sha3-256:vF1z+CEQYIF4ktT8vB4sOSq1Urgo7839PngeDpPxYQ0="),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA3_384_NAME,   "sha3-384:UyR/UMTpWdTI8WkInbQtFHbF74MiwSP7TQs96WsAtLUOwdEdJEutuna3qyIXs8Pj"),
        std::make_tuple("asdf1234ASDF!@#$", irods::SHA3_512_NAME,   "sha3-512:pdy3weePvJChF15U5hsYZqzLZlnFEKZ7e9BLR5j2cbdL63vNI23DUvVYyz2t3eB2ZEMtFmmviSPUUtn34ZkZNA=="),
        std::make_tuple("asdf1234ASDF!@#$", irods::ADLER32_NAME,    "adler32:28b8042f"),
        std::make_tuple("asdf1234ASDF!@#$", irods::BLAKE2S256_NAME, "blake2s256:fqIHD5tDxIA8CzXyJoHG5pVCmDSEunmFIVEwbQchj+Y="),
        std::make_tuple("asdf1234ASDF!@#$", irods::BLAKE2B512_NAME, "blake2b512:8rcItqoe9fBwG146xoP/hFq8YPopVY7lLJfy/WsIB/nIE0qBHzYrZzg8HSGDW1y8Xu6+1OxFJrbk6v1dDLlU+A=="),
        std::make_tuple("asdf1234ASDF!@#$", irods::CRC32_NAME,      "crc32:0689c095"),
        std::make_tuple("asdf1234ASDF!@#$", irods::CRC32C_NAME,     "crc32c:68286c7b"),
        std::make_tuple("asdf1234ASDF!@#$", irods::CRC64NVME_NAME,  "crc64nvme:OTvEv/lA92k="),
        std::make_tuple("",                 irods::MD5_NAME,        "d41d8cd98f00b204e9800998ecf8427e"),
        std::make_tuple("",                 irods::RIPEMD160_NAME,  "ripemd160:nBGFpcXp/FRhKAiXfuj1SLIljTE="),
        std::make_tuple("",                 irods::SHA1_NAME,       "sha1:2jmj7l5rSw0yVb/vlWAYkK/YBwk="),
        std::make_tuple("",                 irods::SHA224_NAME,     "sha224:0UoCjCo6K8lHYQK7KII0xBWisB+CjqYqxbPkLw=="),
        std::make_tuple("",                 irods::SHA256_NAME,     "sha2:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="),
        std::make_tuple("",                 irods::SHA384_NAME,     "sha384:OLBgp1GsljhM2TJ+sbHjaiH9txEUvgdDTAzHv2P24donTt6/529l+9Ua0vFImLlb"),
        std::make_tuple("",                 irods::SHA512_NAME,     "sha512:z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg=="),
        std::make_tuple("",                 irods::SHA3_224_NAME,   "sha3-224:a04DQjZn27c7bhVFTw6xq9RZf5obB44/W1prxw=="),
        std::make_tuple("",                 irods::SHA3_256_NAME,   "sha3-256:p//G+L8e12ZRwUdWoGHWYvWA/03kO0n6gtgKS4D4Q0o="),
        std::make_tuple("",                 irods::SHA3_384_NAME,   "sha3-384:DGOnW4ReT30BEH2FLkwkhcUaUKqqlPxhmV5xu+6YOirDcTgxJkrbR/tr0eBY1fAE"),
        std::make_tuple("",                 irods::SHA3_512_NAME,   "sha3-512:pp9zzKI6msXItWfcGFp1bpfJghZP4lhZ4NHcwUdcgKYVshI68fX5TBHj6UAsOsVY9QAZnZW20+MBdYWGKB3NJg=="),
        std::make_tuple("",                 irods::ADLER32_NAME,    "adler32:00000001"),
        std::make_tuple("",                 irods::BLAKE2S256_NAME, "blake2s256:aSF6MHmQgJThESHQQjVKfB9VtkgsoaUeGyUN/R7Q7vk="),
        std::make_tuple("",                 irods::BLAKE2B512_NAME, "blake2b512:eGoC90IBWQPGxv2FJVLScpEvR0DhWEdhiobiF/cfVBnSXhAxr+5YUxOJZESTTrBLkDpoWxRIt1XVb3Aa/pvizg=="),
        std::make_tuple("",                 irods::CRC32_NAME,      "crc32:00000000"),
        std::make_tuple("",                 irods::CRC32C_NAME,     "crc32c:00000000"),
        std::make_tuple("",                 irods::CRC64NVME_NAME,  "crc64nvme:AAAAAAAAAAA=")
    );
    // clang-format on

    const std::string& str_to_hash = std::get<0>(hash_test_vals);
    const std::string& hash_type_str = std::get<1>(hash_test_vals);
    const std::string& expected_hash_value = std::get<2>(hash_test_vals);

    SECTION(fmt::format("{} hash [{}]", hash_type_str, str_to_hash))
    {
        irods::Hasher hasher;
        REQUIRE(irods::getHasher(hash_type_str, hasher).ok());

        SECTION("all-at-once")
        {
            REQUIRE(hasher.update(str_to_hash).ok());

            std::string hash_value;
            REQUIRE(hasher.digest(hash_value).ok());

            REQUIRE(hash_value == expected_hash_value);
        }

        SECTION("byte-by-byte")
        {
            for (const char& byte : str_to_hash) {
                const std::string byte_s(1, byte);
                REQUIRE(hasher.update(byte_s).ok());
            }

            std::string hash_value;
            REQUIRE(hasher.digest(hash_value).ok());

            REQUIRE(hash_value == expected_hash_value);
        }
    }
}

const std::vector<unsigned char> test_nonstring_1{0x00, 0x00, 0x00, 0x00, 0xF0, 0xA4, 0xAD, 0xA2, 0xC3, 0x28, 0xA0,
                                                  0xA1, 0xF0, 0x90, 0x28, 0xBC, 0xF0, 0x28, 0x8C, 0x28, 0xFF};

TEST_CASE("checksum hashers", "[nonstring]")
{
    // clang-format off
    const std::tuple<const std::vector<unsigned char>, const std::string, const std::string> hash_test_vals = GENERATE(
        std::make_tuple(test_nonstring_1, irods::MD5_NAME,        "47146e5abac11e150e8f5b0c64574e51"),
        std::make_tuple(test_nonstring_1, irods::RIPEMD160_NAME,  "ripemd160:MeAxdcWTqFhEXHlqmTHpjfFCvwc="),
        std::make_tuple(test_nonstring_1, irods::SHA1_NAME,       "sha1:6VDLSmD0EtUrxhB5PLpAH1jAAxs="),
        std::make_tuple(test_nonstring_1, irods::SHA224_NAME,     "sha224:itXKJLt8VONkX0tENAMGUTqT49tEJ3A5v0apMg=="),
        std::make_tuple(test_nonstring_1, irods::SHA256_NAME,     "sha2:Vv+8sjSakZcB2PPoVQZUaVIU5EDD4DjA3mNSB4bB7u4="),
        std::make_tuple(test_nonstring_1, irods::SHA384_NAME,     "sha384:Haq2JtwsY+mlA4EfzIGV1N0G35l2wlXyrX150BJZ6sCcYFRoZ6+Ki+890t+3YLao"),
        std::make_tuple(test_nonstring_1, irods::SHA512_NAME,     "sha512:dnPmYhU+7FrWj8hdMRjFEiXu11cQYVItfjGMweCcETgY8NWX5VhIoEk4Stka7RDifH0mz/L7tpIF2H7+YcKkxA=="),
        std::make_tuple(test_nonstring_1, irods::SHA3_224_NAME,   "sha3-224:vpfTGzACu3JAlERVJ8dxoJmtCYhDfHb9aZE1mQ=="),
        std::make_tuple(test_nonstring_1, irods::SHA3_256_NAME,   "sha3-256:hrDPA0us+I6bAICCQjBw5seH7jzox7y4c6mWDOsaCNI="),
        std::make_tuple(test_nonstring_1, irods::SHA3_384_NAME,   "sha3-384:pXepoTgranzq2ze09XpzcCzsSQYMKENyMEargaeyvY8WUZO4u9Ou+To1MG4SXthS"),
        std::make_tuple(test_nonstring_1, irods::SHA3_512_NAME,   "sha3-512:Z2ABAeHlDmI7PfO1p46C1Wzue1rpO8Cj1obZomifmZhHCgUQgI0/15dHQFIn0x/oTHPLco53u1hrt9q3ONzE0w=="),
        std::make_tuple(test_nonstring_1, irods::ADLER32_NAME,    "adler32:60e80a3f"),
        std::make_tuple(test_nonstring_1, irods::BLAKE2S256_NAME, "blake2s256:4ZTQECDEcBy1qEB1PqPW0IKC51n8KI4AMsooy65VHNo="),
        std::make_tuple(test_nonstring_1, irods::BLAKE2B512_NAME, "blake2b512:yNpst5ywvFV0Djy4yUYK9eY65DsByyOHq/7yViBwzT2YTyD6tYP7qB2C/7EjdpfNln1B6n60qAmBeVxJkHtRMA=="),
        std::make_tuple(test_nonstring_1, irods::CRC32_NAME,      "crc32:01e3fca2"),
        std::make_tuple(test_nonstring_1, irods::CRC32C_NAME,     "crc32c:fcaf6f72"),
        std::make_tuple(test_nonstring_1, irods::CRC64NVME_NAME,  "crc64nvme:LmR4qrQyCGM=")
    );
    // clang-format on

    const std::vector<unsigned char>& data_to_hash_v = std::get<0>(hash_test_vals);
    const std::string& hash_type_str = std::get<1>(hash_test_vals);
    const std::string& expected_hash_value = std::get<2>(hash_test_vals);

    const std::string data_to_hash(reinterpret_cast<const char*>(data_to_hash_v.data()), data_to_hash_v.size());

    SECTION(fmt::format("{} hash nonstring", hash_type_str))
    {
        irods::Hasher hasher;
        REQUIRE(irods::getHasher(hash_type_str, hasher).ok());

        SECTION("all-at-once")
        {
            REQUIRE(hasher.update(data_to_hash).ok());

            std::string hash_value;
            REQUIRE(hasher.digest(hash_value).ok());

            REQUIRE(hash_value == expected_hash_value);
        }

        SECTION("byte-by-byte")
        {
            for (const unsigned char& byte : data_to_hash_v) {
                const std::string byte_s(1, static_cast<char>(byte));
                REQUIRE(hasher.update(byte_s).ok());
            }

            std::string hash_value;
            REQUIRE(hasher.digest(hash_value).ok());

            REQUIRE(hash_value == expected_hash_value);
        }
    }
}
