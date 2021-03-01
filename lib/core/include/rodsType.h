#ifndef RODS_TYPE_H__
#define RODS_TYPE_H__

#include <sys/types.h>

#if defined(solaris_platform) || defined(aix_platform)
    #include <strings.h>
#endif

#include "rodsDef.h"

// clang-format off

#if defined(osx_platform)
typedef int64_t rodsLong_t;
typedef u_int64_t rodsULong_t;
#elif defined(sgi_platform)
typedef __int64_t rodsLong_t;
typedef int64_t u_longlong_t;
#elif defined(linux_platform) || defined(alpha_platform)
typedef long long rodsLong_t;
typedef unsigned long long rodsULong_t;
#elif defined(windows_platform)
typedef unsigned int uint;
typedef __int64 rodsLong_t;
typedef unsigned __int64 rodsULong_t;
#else	/* windows_platform */
typedef long long rodsLong_t;
typedef unsigned long long rodsULong_t;
#endif	/* windows_platform */

/**
 * \enum objType_t
 *
 * \var objType_t::UNKNOWN_OBJ_T
 *   unknown object type, probably a rods type
 * \var objType_t::DATA_OBJ_T
 *   data object
 * \var objType_t::COLL_OBJ_T
 *   collection
 * \var objType_t::UNKNOWN_FILE_T
 *   unknown local type (also used for stdout)
 * \var objType_t::LOCAL_FILE_T
 *   local file
 * \var objType_t::LOCAL_DIR_T
 *   local directory
 * \var objType_t::NO_INPUT_T
 */
typedef enum ObjectType {
    UNKNOWN_OBJ_T,
    DATA_OBJ_T,
    COLL_OBJ_T,
    UNKNOWN_FILE_T,
    LOCAL_FILE_T,
    LOCAL_DIR_T,
    NO_INPUT_T
} objType_t;

/**
 * \enum objStat_t
 *
 * \var objStat_t::UNKNOWN_ST
 *   unknown status
 * \var objStat_t::NOT_EXIST_ST
 *   object does not exist
 * \var objStat_t::EXIST_ST
 *   object exists
 */
typedef enum ObjectStat {
    UNKNOWN_ST,
    NOT_EXIST_ST,
    EXIST_ST
} objStat_t;

/**
 * \struct rodsStat_t
 *
 * \ingroup capi_input_data_structures
 *
 * \var rodsStat_t::st_size
 *   file size
 * \var rodsStat_t::st_dev
 * \var rodsStat_t::st_ino
 * \var rodsStat_t::st_mode
 * \var rodsStat_t::st_nlink
 * \var rodsStat_t::st_uid
 * \var rodsStat_t::st_gid
 * \var rodsStat_t::st_rdev
 * \var rodsStat_t::st_atim
 *   time of last access
 * \var rodsStat_t::st_mtim
 *   time of last mod
 * \var rodsStat_t::st_ctim
 *   time of last status change
 * \var rodsStat_t::st_blksize
 *   Optimal blocksize of FS
 * \var rodsStat_t::st_blocks
 *   number of blocks
 */
typedef struct rodsStat {
    rodsLong_t   st_size;
    unsigned int st_dev;
    unsigned int st_ino;
    unsigned int st_mode;
    unsigned int st_nlink;
    unsigned int st_uid;
    unsigned int st_gid;
    unsigned int st_rdev;
    unsigned int st_atim;
    unsigned int st_mtim;
    unsigned int st_ctim;
    unsigned int st_blksize;
    unsigned int st_blocks;
} rodsStat_t;

#define DIR_LEN 256

/**
 * \enum rodsDirent_t
 *
 * \var rodsDirent_t::d_offset
 *   offset after this entry
 * \var rodsDirent_t::d_ino
 *   inode number
 * \var rodsDirent_t::d_reclen
 *   length of this record
 * \var rodsDirent_t::d_namlen
 *   length of d_name
 * \var rodsDirent_t::d_name
 */
typedef struct rodsDirent {
    unsigned int d_offset;
    unsigned int d_ino;
    unsigned int d_reclen;
    unsigned int d_namlen;
    char         d_name[DIR_LEN];
} rodsDirent_t;

// clang-format on

#endif	// RODS_TYPE_H__

