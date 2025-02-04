#[=======================================================================[.rst:
FindZeroMQ
-----------

Finds ZeroMQ. Does not (currently) support version matching.

IMPORTED Targets
^^^^^^^^^^^^^^^^

The following :prop_tgt:`IMPORTED` targets may be defined:

``ZeroMQ::libzmq``
  ZeroMQ library.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``ZeroMQ_FOUND``
  true if ZeroMQ headers and library were found
``ZeroMQ_INCLUDE_DIR``
  the directory containing ZeroMQ headers
``ZeroMQ_LIBRARY``
  ZeroMQ library to be linked

#]=======================================================================]

cmake_policy(PUSH)
# if interprets arguments as variables or keywords only when they are unquoted
if (POLICY CMP0054)
	cmake_policy(SET CMP0054 NEW)
endif()
# find_(path|file|library|program) have consistent behavior for cache variables
if (POLICY CMP0125)
	cmake_policy(SET CMP0125 NEW)
endif()

find_path(ZeroMQ_INCLUDE_DIR NAMES zmq.h)
find_library(ZeroMQ_LIBRARY NAMES zmq)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(ZeroMQ
	REQUIRED_VARS ZeroMQ_INCLUDE_DIR ZeroMQ_LIBRARY)

if (ZeroMQ_FOUND)
	if (NOT TARGET ZeroMQ::libzmq)
		add_library(ZeroMQ::libzmq UNKNOWN IMPORTED)
		set_target_properties(ZeroMQ::libzmq PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${ZeroMQ_INCLUDE_DIR}")
		set_target_properties(ZeroMQ::libzmq PROPERTIES IMPORTED_LOCATION "${ZeroMQ_LIBRARY}")
	endif()
endif()

cmake_policy(POP)
