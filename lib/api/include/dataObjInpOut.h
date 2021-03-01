/**
 * @file  dataObjInpOut.h
 *
 */

/*** Copyright (c), The Regents of the University of California            ***
 *** For more information please refer to files in the COPYRIGHT directory ***/


#ifndef DATA_OBJ_INP_OUT_H__
#define DATA_OBJ_INP_OUT_H__

#include "rodsDef.h"
#include "rodsType.h"
#include "objInfo.h"

#if defined(aix_platform)
#ifndef _AIX_PTHREADS_D7
#define pthread_mutexattr_default NULL
#define pthread_condattr_default NULL
#define pthread_attr_default NULL
#endif  /* _AIX_PTHREADS_D7 */
#else   /* aix_platform */
#define pthread_mutexattr_default NULL
#define pthread_condattr_default NULL
#define pthread_attr_default NULL
#endif  /* aix_platform */

/**
 * \struct portList_t
 *
 * \ingroup capi_input_data_structures
 *
 * \var portList_t::portNum;
 *   The port number.
 * \var portList_t::cookie;
 * \var portList_t::sock
 *   The server's sock number. No meaning for client.
 * \var portList_t::windowSize
 * \var portList_t::hostAddr
 */
typedef struct {
    int portNum;       /* the port number */
    int cookie;
    int sock;           /* The server's sock number. no meaning for client */
    int windowSize;
    char hostAddr[LONG_NAME_LEN];
} portList_t;

/**
 * \struct dataObjInp_t
 * \brief Input struct for Data object operation
 * \since 1.0
 *
 * \ingroup capi_input_data_structures
 *
 * \var dataObjInp_t::objPath
 *   Full path of the data object.
 * \var dataObjInp_t::createMode
 *   The file mode of the data object.
 * \var dataObjInp_t::openFlags
 *   The flags for the I/O operation. Valid flags are \c O_RDONLY,
 *   \c O_WRONLY, \c O_RDWR and \c O_TRUNC. Also used for \c specCollInx
 *   in rcQuerySpecColl.
 * \var dataObjInp_t::offset
 * \var dataObjInp_t::dataSize
 *   The size of the data object.
 * \var dataObjInp_t::numThreads
 *   The number of threads to use.
 * \var dataObjInp_t::oprType
 *   The type of operation.
 * \var dataObjInp_t::specColl
 *   T pointer to a specColl_t if this path is in a special collection
 *   (e.g. mounted collection).
 * \li dataObjInp_t::condInput
 *   Keyword/value pair input. Valid keywords depend on the API. Include
 *   \c cksum flag and value.
 */
typedef struct DataObjInp {
    char objPath[MAX_NAME_LEN];
    int createMode;
    int openFlags;
    rodsLong_t offset;
    rodsLong_t dataSize;
    int numThreads;
    int oprType;
    specColl_t *specColl;
    keyValPair_t condInput;
} dataObjInp_t;

/**
 * \struct openedDataObjInp_t
 * \brief Input struct for Opened data object operation
 * \since 1.0
 *
 * \ingroup capi_input_data_structures
 *
 * \var openedDataObjInp_t::l1descInx
 *   The opened data object descriptor from rcDataObjOpen or rcDataObjCreate.
 *   For read, write, and close.
 * \var openedDataObjInp_t::len
 *   The length (number of bytes) to read/write.
 * \var openedDataObjInp_t::whence
 *   Valid only for rcDataObjLseek (similar to \c lseek of UNIX). Valid values
 *   are \c SEEK_SET, \c SEEK_CUR and \c SEEK_END.
 * \var openedDataObjInp_t::oprType
 *   The operation type. Valid values are PUT_OPR, GET_OPR,  REPLICATE_OPR.
 *   See dataObjInpOut.h for more.
 * \var openedDataObjInp_t::offset
 * \var openedDataObjInp_t::bytesWritten
 *   Number of bytes written (valid for rcDataObjClose).
 * \var openedDataObjInp_t::condInput
 *   Keyword/value pair input. Valid keywords depend on the API. Include
 *   \c cksum flag and value.
 */
typedef struct OpenedDataObjInp {
    int l1descInx;
    int len;
    int whence;
    int oprType;
    rodsLong_t offset;
    rodsLong_t bytesWritten;
    keyValPair_t condInput;
} openedDataObjInp_t;

typedef struct portalOprOut {
    int status;
    int l1descInx;
    int numThreads;
    char chksum[NAME_LEN];
    portList_t portList;
} portalOprOut_t;

typedef struct DataOprInp {
    int oprType;
    int numThreads;
    int srcL3descInx;
    int destL3descInx;
    int srcRescTypeInx;
    int destRescTypeInx;
    /* XXXXXXX offset and dataSize moved to here because of problem with
     * 64 bit susue linux that condInput has pointer's in it which
     * cause condInput to be aligned at 64 the beginning and end of condInput */
    rodsLong_t offset;
    rodsLong_t dataSize;
    keyValPair_t condInput;
} dataOprInp_t;

/**
 * \struct collInp_t
 * \brief Input struct for collection operation
 * \since 1.0
 *
 * \ingroup capi_input_data_structures
 *
 * \note
 * Elements of collInp_t:
 * \var collInp_t::collName
 *   Full path of the collection.
 * \var collInp_t::flags
 *   Flags for rcOpenCollection.
 * \var collInp_t::oprType
 *   Operation type. Not used?
 * \var collInp_t::condInput
 *   Keyword/value pair input. Valid keywords depend on the API.
 */
typedef struct CollInp {
    char collName[MAX_NAME_LEN];
    int flags;
    int oprType;
    keyValPair_t condInput;
} collInp_t;

/**
 * \defgroup oprType_defs
 * Definitions for oprType in dataObjInp_t, portalOpr_t and l1desc_t.
 * \{
 */
#define DONE_OPR                9999
#define PUT_OPR                 1
#define GET_OPR                 2
#define SAME_HOST_COPY_OPR      3
#define COPY_TO_LOCAL_OPR       4
#define COPY_TO_REM_OPR         5
#define REPLICATE_OPR           6
#define REPLICATE_DEST          7
#define REPLICATE_SRC           8
#define COPY_DEST               9
#define COPY_SRC                10
#define RENAME_DATA_OBJ         11
#define RENAME_COLL             12
#define MOVE_OPR                13
#define RSYNC_OPR               14
#define PHYMV_OPR               15
#define PHYMV_SRC               16
#define PHYMV_DEST              17
#define QUERY_DATA_OBJ          18
#define QUERY_DATA_OBJ_RECUR    19
#define QUERY_COLL_OBJ          20
#define QUERY_COLL_OBJ_RECUR    21
#define RENAME_UNKNOWN_TYPE     22
#define REMOTE_ZONE_OPR         24
#define UNREG_OPR               26
/** \} */

/**
 * \defgroup openType_defs
 * Definitions for openType in l1desc_t.
 * \{
 */
#define CREATE_TYPE             1
#define OPEN_FOR_READ_TYPE      2
#define OPEN_FOR_WRITE_TYPE     3
/** \} */

/**
 * \struct portalOpr_t
 *
 * \ingroup capi_input_data_structures
 *
 * \var portalOpr_t::oprType
 * \var portalOpr_t::dataOprInp
 * \var portalOpr_t::portList
 * \var portalOpr_t::shared_secret
 *   Shared secret for encryption
 */
typedef struct PortalOpr {
    int oprType;
    dataOprInp_t dataOprInp;
    portList_t portList;
    char shared_secret[ NAME_LEN ];
} portalOpr_t;

/**
 * \defgroup transfer_flag_defs
 * \{
 */
#define STREAMING_FLAG          0x1
#define NO_CHK_COPY_LEN_FLAG    0x2
/** \} */

typedef struct TransferHeader {
    int oprType;
    int flags;
    rodsLong_t offset;
    rodsLong_t length;
} transferHeader_t;

#endif  // DATA_OBJ_INP_OUT_H__
