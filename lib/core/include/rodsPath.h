#ifndef RODS_PATH_H__
#define RODS_PATH_H__

#include "rodsDef.h"
#include "rods.h"
#include "getRodsEnv.h"
#include "rodsType.h"
#include "objStat.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * \def STDOUT_FILE_NAME
 * filename used for piping to stdout
 */
#define STDOUT_FILE_NAME "-"

/**
 * \struct rodsPath_t
 * \brief Struct containing a path and information about the object referenced by the path.
 *
 * \ingroup capi_input_data_structures
 *
 * \var rodsPath_t::objType
 *   Type of object referenced by the path
 * \var rodsPath_t::objState
 *   State of object referenced by the path (whether or not it exists)
 * \var rodsPath_t::size
 *   Size of the object referenced by the path. Not always used.
 * \var rodsPath_t::objMode
 * \var rodsPath_t::inPath
 *   Path string as provided on the command line
 * \var rodsPath_t::outPath
 *   Path string after parsting rodsPath_t::inPath
 * \var rodsPath_t::dataId
 * \var rodsPath_t::chksum
 * \var rodsPath_t::rodsObjStat
 */
typedef struct RodsPath {
    objType_t objType;
    objStat_t objState;
    rodsLong_t size;
    uint objMode;
    char inPath[MAX_NAME_LEN];
    char outPath[MAX_NAME_LEN];
    char dataId[NAME_LEN];
    char chksum[NAME_LEN];
    rodsObjStat_t *rodsObjStat;
} rodsPath_t;

/**
 * \struct rodsPathInp_t
 * \brief Struct containing paths for operations
 *
 * \ingroup capi_input_data_structures
 *
 * \var rodsPathInp_t::numSrc
 *   Number of array elements in rodsPathInp_t::srcPath.
 * \var rodsPathInp_t::srcPath
 *   Pointer to an array of rodsPath_t objects populated with input paths.
 * \var rodsPathInp_t::destPath
 *   Pointer to the rodsPath_t object containing the destination path.
 * \var rodsPathInp_t::targPath
 *   Pointer to an array of rodsPath_t objects populated with target paths.
 *   Each element cooresponst to the element of the same index in
 *   rodsPathInp_t::srcPath.
 * \var rodsPathInp_t::resolved
 *   Resolution status variable used by some utility funcitons.
 */
typedef struct RodsPathInp {
    int numSrc;
    rodsPath_t *srcPath;
    rodsPath_t *destPath;
    rodsPath_t *targPath;
    int resolved;
} rodsPathInp_t;

/**
 * \def ALLOW_NO_SRC_FLAG
 * definition for flag in parseCmdLinePath
 */
#define	ALLOW_NO_SRC_FLAG 0x1

int
parseRodsPath( rodsPath_t *rodsPath, rodsEnv *myRodsEnv );
int
parseRodsPathStr( const char *inPath, rodsEnv *myRodsEnv, char *outPath );
int
addSrcInPath( rodsPathInp_t *rodsPathInp, const char *inPath );
int
parseLocalPath( rodsPath_t *rodsPath );
int
parseCmdLinePath( int argc, char **argv, int optInd, rodsEnv *myRodsEnv,
                  int srcFileType, int destFileType, int flag, rodsPathInp_t *rodsPathInp );

int
getLastPathElement( char *inPath, char *lastElement );

int
getFileType( rodsPath_t *rodsPath );
void
clearRodsPath( rodsPath_t *rodsPath );

/**
 * \fn escape_path
 * Returns a new path with the following special characters escaped:
 * \li \c \\f
 *
 * \param[in] _path The path to escape
 * \return dynamically allocated character array contianing the escaped path
 *
 * \remark The returned character array is dynamically allocated. The caller
 * is expected to deallocate this memory using \c free.
 */
char* escape_path(const char* _path);

/**
 * \fn has_trailing_path_separator
 * Returns whether \p path enws with a trailing path separator.
 *
 * \param[in] path The path to check.

 * \return An interger value indicating whether \p path ends with a trailing path separator.
 * \retval non-zero If \p path ends with a trailing path separator.
 * \retval 0        If \p path does not end with a trailing path separator.
 */
int has_trailing_path_separator(const char* path);

/**
 * \fn remove_trailing_path_separators
 * Removes trailing slashes from \p path in-place.
 *
 * \param[in,out] path The path to trim
 */
void remove_trailing_path_separators(char* path);

/**
 * \fn has_prefix
 * Returns whether \p path starts with \p prefix
 *
 * \p path and \p prefix are expected to be null-terminated strings.
 * The behavior is undefined if either string is null or not null-terminated.
 *
 * \since 4.2.8
 *
 * \param[in] path   The path to search.
 * \param[in] prefix The path to look for. Trailing slashes are ignored.
 *
 * \return An interger value indicating whether \p path starts with \p prefix.
 * \retval non-zero If \p path starts with \p prefix.
 * \retval 0        If \p path does not start with \p prefix or \p prefix is an empty string.
 */
int has_prefix(const char* path, const char* prefix);
#ifdef __cplusplus
}
#endif

#endif	// RODS_PATH_H__
