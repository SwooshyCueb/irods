#ifndef IRODS_CHECKSUM_H
#define IRODS_CHECKSUM_H

struct KeyValPair;

#ifdef __cplusplus
extern "C" {
#endif

#define SHA224_CHKSUM_PREFIX    "sha224:"
#define SHA256_CHKSUM_PREFIX    "sha2:"
#define SHA384_CHKSUM_PREFIX    "sha384:"
#define SHA512_CHKSUM_PREFIX    "sha512:"
#define SHA3_224_CHKSUM_PREFIX  "sha3-224:"
#define SHA3_256_CHKSUM_PREFIX  "sha3-256:"
#define SHA3_384_CHKSUM_PREFIX  "sha3-384:"
#define SHA3_512_CHKSUM_PREFIX  "sha3-512:"
#define ADLER32_CHKSUM_PREFIX   "adler32:"
#define SHA1_CHKSUM_PREFIX      "sha1:"
#define CRC64NVME_CHKSUM_PREFIX "crc64nvme:"

int verifyChksumLocFile(char *fileName, const char *myChksum, char *chksumStr);

int chksumLocFile(const char *fileName, char *chksumStr, const char* hashScheme);

int hashToStr(unsigned char *digest, char *digestStr);

int rcChksumLocFile(char *fileName,
                    char *chksumFlag,
                    struct KeyValPair *condInput,
                    const char *hashScheme);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // IRODS_CHECKSUM_H
