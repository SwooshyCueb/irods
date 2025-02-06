#[=======================================================================[.rst:
FindAvroCpp
-----------

Finds Avro C++.

IMPORTED Targets
^^^^^^^^^^^^^^^^

The following :prop_tgt:`IMPORTED` targets may be defined:

``Avro::AvroCpp``
  AvroCpp library.
``Avro::avrogencpp``
  avrogencpp command-line executable.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``AvroCpp_FOUND``
  true if AvroCpp headers, library, and executable were found
``AVROCPP_INCLUDE_DIR``
  the directory containing AvroCpp headers
``AVROCPP_LIBRARY``
  AvroCpp library to be linked
``AVROCPP_AVROGEN_EXECUTABLE``
  path to avrogencpp tool

#]=======================================================================]

cmake_policy(PUSH)
cmake_minimum_required(VERSION 3.12...3.17 FATAL_ERROR)
# list command detects invalid indices
if (POLICY CMP0121)
	cmake_policy(SET CMP0121 NEW)
endif()
# find_(path|file|library|program) have consistent behavior for cache variables
if (POLICY CMP0125)
	cmake_policy(SET CMP0125 NEW)
endif()

# If IRODS_EXTERNALS path is set, use it, unless AvroCpp_ROOT is already defined
if (IRODS_EXTERNALS_FULLPATH_AVRO and NOT AvroCpp_ROOT)
	set(AvroCpp_ROOT "${IRODS_EXTERNALS_FULLPATH_AVRO}")
endif()

find_path(AVROCPP_INCLUDE_DIR NAMES avro/AvroParse.hh)
find_library(AVROCPP_LIBRARY NAMES avrocpp libavrocpp)
find_program(AVROCPP_AVROGEN_EXECUTABLE NAMES avrogencpp)

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(AvroCpp
	REQUIRED_VARS AVROCPP_INCLUDE_DIR AVROCPP_LIBRARY AVROCPP_AVROGEN_EXECUTABLE)

if (AvroCpp_FOUND)
	if (NOT TARGET Avro::AvroCpp)
		add_library(Avro::AvroCpp SHARED IMPORTED)
		set_target_properties(Avro::AvroCpp PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${AVROCPP_INCLUDE_DIR}")
		set_target_properties(Avro::AvroCpp PROPERTIES IMPORTED_LOCATION "${AVROCPP_LIBRARY}")
	endif()
	if (NOT TARGET Avro::avrogencpp)
		add_executable(Avro::avrogencpp IMPORTED)
		set_target_properties(Avro::avrogencpp PROPERTIES IMPORTED_LOCATION "${AVROCPP_AVROGEN_EXECUTABLE}")
	endif()
endif()

cmake_policy(POP)
