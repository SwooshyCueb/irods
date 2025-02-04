#[=======================================================================[.rst:
Findcppzmq
-----------

Finds cppzmq. Prioritizes irods-externals if a path is defined.

IMPORTED Targets
^^^^^^^^^^^^^^^^

The following :prop_tgt:`IMPORTED` targets may be defined:

``cppzmq::cppzmq``
  cppzmq.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``cppzmq_FOUND``
  true if cppzmq headers were found
``cppzmq_INCLUDE_DIRECTORIES``
  the directories containing cppzmq headers
``cppzmq_VERSION``
  the version of cppzmq found

#]=======================================================================]

cmake_policy(PUSH)
# if interprets arguments as variables or keywords only when they are unquoted
if (POLICY CMP0054)
	cmake_policy(SET CMP0054 NEW)
endif()
# if(IN_LIST)
if (POLICY CMP0057)
	cmake_policy(SET CMP0057 NEW)
endif()
# mark_as_advanced() does nothing if a cache entry does not exist
if (POLICY CMP0102)
	cmake_policy(SET CMP0102 NEW)
endif()
# list command detects invalid indices
if (POLICY CMP0121)
	cmake_policy(SET CMP0121 NEW)
endif()
# find_(path|file|library|program) have consistent behavior for cache variables
if (POLICY CMP0125)
	cmake_policy(SET CMP0125 NEW)
endif()

macro(_cppzmq_fix_includes)
	get_target_property(cppzmq_INCLUDE_DIRECTORIES cppzmq::cppzmq INTERFACE_INCLUDE_DIRECTORIES)  
	list(REMOVE_DUPLICATES cppzmq_INCLUDE_DIRECTORIES)
	set_target_properties(
		cppzmq::cppzmq PROPERTIES
		INTERFACE_INCLUDE_DIRECTORIES "${cppzmq_INCLUDE_DIRECTORIES}"
	)
endmacro()

function(_cppzmq_create_target include_dir_var_name)
	if (NOT "${${include_dir_var_name}}" STREQUAL "${include_dir_var_name}-NOTFOUND")

		set(cppzmq_INCLUDE_DIRECTORIES "${${include_dir_var_name}}")
		set(cppzmq_INCLUDE_DIRECTORIES "${cppzmq_INCLUDE_DIRECTORIES}" PARENT_SCOPE)

		## Get version number
		include(CheckCPPMacroDefinition)

		set(CMAKE_REQUIRED_INCLUDES "${cppzmq_INCLUDE_DIRECTORIES}")
		set(CMAKE_REQUIRED_QUIET "ON")
		set(CMAKE_EXTRA_INCLUDE_FILES "zmq.hpp")

		# clear cached results if hash of json.hpp has changed
		file(MD5 "${${include_dir_var_name}}/zmq.hpp" cppzmq_HPP_MD5)
		if (NOT DEFINED cppzmq_HPP_MD5_LASTRUN OR NOT cppzmq_HPP_MD5 STREQUAL cppzmq_HPP_MD5_LASTRUN)
			unset(HAVE_cppzmq_macro_VERSION_MAJOR CACHE)
			unset(HAVE_cppzmq_macro_VERSION_MINOR CACHE)
			unset(HAVE_cppzmq_macro_VERSION_PATCH CACHE)
			unset(cppzmq_macro_VERSION_MAJOR CACHE)
			unset(cppzmq_macro_VERSION_MINOR CACHE)
			unset(cppzmq_macro_VERSION_PATCH CACHE)
		endif()
		set(cppzmq_HPP_MD5_LASTRUN "${cppzmq_HPP_MD5}" CACHE INTERNAL "last value of cppzmq_HPP_MD5")

		CHECK_CPP_MACRO_DEFINITION(CPPZMQ_VERSION_MAJOR cppzmq_macro_VERSION_MAJOR LANGUAGE CXX)
		CHECK_CPP_MACRO_DEFINITION(CPPZMQ_VERSION_MINOR cppzmq_macro_VERSION_MINOR LANGUAGE CXX)
		CHECK_CPP_MACRO_DEFINITION(CPPZMQ_VERSION_PATCH cppzmq_macro_VERSION_PATCH LANGUAGE CXX)

		set(cppzmq_VERSION "${cppzmq_macro_VERSION_MAJOR}.${cppzmq_macro_VERSION_MINOR}.${cppzmq_macro_VERSION_PATCH}")
		set(cppzmq_VERSION "${cppzmq_VERSION}" PARENT_SCOPE)

		unset("${include_dir_var_name}" CACHE)

		if (DEFINED cppzmq_FIND_VERSION)
			if (cppzmq_FIND_VERSION_EXACT)
				if (
					cppzmq_FIND_VERSION_COUNT EQUAL 1 AND
					NOT cppzmq_FIND_VERSION VERSION_EQUAL "${cppzmq_macro_VERSION_MAJOR}"
				)
					return()
				elseif (
					cppzmq_FIND_VERSION_COUNT EQUAL 2 AND
					NOT cppzmq_FIND_VERSION VERSION_EQUAL "${cppzmq_macro_VERSION_MAJOR}.${cppzmq_macro_VERSION_MINOR}"
				)
					return()
				elseif (NOT cppzmq_FIND_VERSION VERSION_EQUAL cppzmq_VERSION)
					return()
				endif()
			elseif (cppzmq_FIND_VERSION VERSION_GREATER cppzmq_VERSION)
				return()
			endif()
		endif()

		add_library(cppzmq::cppzmq INTERFACE IMPORTED)
		set_target_properties(
			cppzmq::cppzmq
			PROPERTIES
			INTERFACE_INCLUDE_DIRECTORIES "${cppzmq_INCLUDE_DIRECTORIES}"
		)
		set_target_properties(
			cppzmq::cppzmq
			PROPERTIES
			INTERFACE_LINK_LIBRARIES ZeroMQ::libzmq
		)
	endif()
	unset("${include_dir_var_name}" CACHE)
endfunction()

find_dependency(ZeroMQ)

# Search in IRODS_EXTERNALS path first.
if (DEFINED IRODS_EXTERNALS_FULLPATH_CPPZMQ AND NOT TARGET cppzmq::cppzmq)
	find_path(
		_cppzmq_include_dir
		NAMES "zmq.hpp"
		PATHS "${IRODS_EXTERNALS_FULLPATH_CPPZMQ}/include"
		NO_DEFAULT_PATH
	)
	_cppzmq_create_target(_cppzmq_include_dir)
endif()

if (NOT TARGET cppzmq::cppzmq)
	find_path(_cppzmq_include_dir NAMES zmq.hpp)
	_cppzmq_create_target(_cppzmq_include_dir)
endif()

if (TARGET cppzmq::cppzmq)
	_cppzmq_fix_includes()
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
	cppzmq
	REQUIRED_VARS cppzmq_INCLUDE_DIRECTORIES ZeroMQ_FOUND
	VERSION_VAR cppzmq_VERSION
)

if (cppzmq_DIR STREQUAL "cppzmq_DIR-NOTFOUND")
	unset(cppzmq_DIR CACHE)
endif()

cmake_policy(POP)
