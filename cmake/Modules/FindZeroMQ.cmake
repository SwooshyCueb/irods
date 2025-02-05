#[=======================================================================[.rst:
FindZeroMQ
-----------

Finds ZeroMQ. Prioritizes irods-externals if a path is defined.

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
``ZeroMQ_INCLUDE_DIRS``
  the directories containing ZeroMQ headers
``ZeroMQ_LIBRARIES``
  ZeroMQ libraries to be linked
``ZeroMQ_COMPILE_OPTIONS``
  Compiler flags for ZeroMQ
``ZeroMQ_LINK_OPTIONS``
  Linker flags for ZeroMQ
``ZeroMQ_VERSION``
  the version of ZeroMQ found

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

macro(_ZeroMQ_fix_properties)
	get_target_property(ZeroMQ_INCLUDE_DIRS ZeroMQ::libzmq INTERFACE_INCLUDE_DIRECTORIES)
	list(REMOVE_DUPLICATES ZeroMQ_INCLUDE_DIRS)
	get_target_property(_ZeroMQ_ALIASED_TARGET ZeroMQ::libzmq ALIASED_TARGET)
	if (NOT _ZeroMQ_ALIASED_TARGET)
		set_target_properties(
			ZeroMQ::libzmq PROPERTIES
			INTERFACE_INCLUDE_DIRECTORIES "${ZeroMQ_INCLUDE_DIRS}"
		)
	endif()
	unset(_ZeroMQ_ALIASED_TARGET)
endmacro()

function(_ZeroMQ_create_target lib_var_name include_dir_var_name cflags_var_name ldflags_var_name version_var_name)
	if (
		(NOT "${${lib_var_name}}" STREQUAL "${lib_var_name}-NOTFOUND") AND
		(NOT "${${include_dir_var_name}}" STREQUAL "${include_dir_var_name}-NOTFOUND")
	)

		set(ZeroMQ_LIBRARIES "${${lib_var_name}}")
		set(ZeroMQ_LIBRARIES "${ZeroMQ_LIBRARIES}" PARENT_SCOPE)

		set(ZeroMQ_INCLUDE_DIRS "${${include_dir_var_name}}")
		set(ZeroMQ_INCLUDE_DIRS "${ZeroMQ_INCLUDE_DIRS}" PARENT_SCOPE)

		if ("${${cflags_var_name}}")
			set(ZeroMQ_COMPILE_OPTIONS "${${cflags_var_name}}")
			set(ZeroMQ_COMPILE_OPTIONS "${ZeroMQ_COMPILE_OPTIONS}" PARENT_SCOPE)
		endif()

		if ("${${ldflags_var_name}}")
			set(ZeroMQ_LINK_OPTIONS "${${ldflags_var_name}}")
			set(ZeroMQ_LINK_OPTIONS "${ZeroMQ_LINK_OPTIONS}" PARENT_SCOPE)
		endif()

		## Get version number
		if ("${${version_var_name}}")
			set(ZeroMQ_VERSION "${${version_var_name}}")
			set(ZeroMQ_VERSION "${ZeroMQ_VERSION}" PARENT_SCOPE)
			unset("${version_var_name}" CACHE)
		else()
			include(CheckCPPMacroDefinition)

			set(CMAKE_REQUIRED_INCLUDES "${ZeroMQ_INCLUDE_DIRS}")
			set(CMAKE_REQUIRED_QUIET "ON")
			set(CMAKE_EXTRA_INCLUDE_FILES "zmq.h")

			# clear cached results if hash of json.hpp has changed
			file(MD5 "${${include_dir_var_name}}/zmq.h" ZeroMQ_H_MD5)
			if (NOT DEFINED ZeroMQ_H_MD5_LASTRUN OR NOT ZeroMQ_H_MD5 STREQUAL ZeroMQ_H_MD5_LASTRUN)
				unset(HAVE_ZeroMQ_macro_VERSION_MAJOR CACHE)
				unset(HAVE_ZeroMQ_macro_VERSION_MINOR CACHE)
				unset(HAVE_ZeroMQ_macro_VERSION_PATCH CACHE)
				unset(ZeroMQ_macro_VERSION_MAJOR CACHE)
				unset(ZeroMQ_macro_VERSION_MINOR CACHE)
				unset(ZeroMQ_macro_VERSION_PATCH CACHE)
			endif()
			set(ZeroMQ_H_MD5_LASTRUN "${ZeroMQ_H_MD5}" CACHE INTERNAL "last value of ZeroMQ_H_MD5")

			CHECK_CPP_MACRO_DEFINITION(ZMQ_VERSION_MAJOR ZeroMQ_macro_VERSION_MAJOR LANGUAGE C)
			CHECK_CPP_MACRO_DEFINITION(ZMQ_VERSION_MINOR ZeroMQ_macro_VERSION_MINOR LANGUAGE C)
			CHECK_CPP_MACRO_DEFINITION(ZMQ_VERSION_PATCH ZeroMQ_macro_VERSION_PATCH LANGUAGE C)

			set(ZeroMQ_VERSION "${ZeroMQ_macro_VERSION_MAJOR}.${ZeroMQ_macro_VERSION_MINOR}.${ZeroMQ_macro_VERSION_PATCH}")
			set(ZeroMQ_VERSION "${ZeroMQ_VERSION}" PARENT_SCOPE)
		endif()

		unset("${lib_var_name}" CACHE)
		unset("${include_dir_var_name}" CACHE)
		unset("${cflags_var_name}" CACHE)
		unset("${ldflags_var_name}" CACHE)

		if (DEFINED ZeroMQ_FIND_VERSION)
			if (ZeroMQ_FIND_VERSION_EXACT)
				if (
					ZeroMQ_FIND_VERSION_COUNT EQUAL 1 AND
					NOT ZeroMQ_FIND_VERSION VERSION_EQUAL "${ZeroMQ_macro_VERSION_MAJOR}"
				)
					return()
				elseif (
					ZeroMQ_FIND_VERSION_COUNT EQUAL 2 AND
					NOT ZeroMQ_FIND_VERSION VERSION_EQUAL "${ZeroMQ_macro_VERSION_MAJOR}.${ZeroMQ_macro_VERSION_MINOR}"
				)
					return()
				elseif (NOT ZeroMQ_FIND_VERSION VERSION_EQUAL ZeroMQ_VERSION)
					return()
				endif()
			elseif (ZeroMQ_FIND_VERSION VERSION_GREATER ZeroMQ_VERSION)
				return()
			endif()
		endif()

		add_library(ZeroMQ::libzmq INTERFACE IMPORTED)
		set_target_properties(
			ZeroMQ::libzmq
			PROPERTIES
			INTERFACE_LINK_LIBRARIES "${ZeroMQ_LIBRARIES}"
			INTERFACE_INCLUDE_DIRECTORIES "${ZeroMQ_INCLUDE_DIRS}"
			INTERFACE_COMPILE_OPTIONS "${ZeroMQ_COMPILE_OPTIONS}"
			INTERFACE_LINK_OPTIONS "${ZeroMQ_LINK_OPTIONS}"
			VERSION "${ZeroMQ_VERSION}"
		)
	endif()
	unset("${lib_var_name}" CACHE)
	unset("${include_dir_var_name}" CACHE)
	unset("${cflags_var_name}" CACHE)
	unset("${ldflags_var_name}" CACHE)
	unset("${version_var_name}" CACHE)
endfunction()

# define a libzmq target for consumers expecting the zmq package configuration
function(_define_default_alias)
	add_library(libzmq ALIAS ZeroMQ::libzmq)
endfunction()

# First check for existing targets
if (TARGET ZeroMQ::libzmq)
	# us?
	if (NOT TARGET libzmq)
		add_library(libzmq ALIAS ZeroMQ::libzmq)
	endif()
elseif (TAGET libzmq)
	add_library(ZeroMQ::libzmq ALIAS libzmq)
endif()

# Search in IRODS_EXTERNALS path first.
if (DEFINED IRODS_EXTERNALS_FULLPATH_ZMQ AND NOT TARGET ZeroMQ::libzmq)

	# Normally we would search for ZeroMQ's cmake config here, but the cmake config for ZeroMQ puts
	# private libs in INTERFACE_LINK_LIBRARIES, and it's not worthwhile to try and clean up behind it.
	#	if (DEFINED ZeroMQ_FIND_VERSION)
	#	set(_ZeroMQ_ver_arg "${ZeroMQ_FIND_VERSION}")
	#	if (ZeroMQ_FIND_VERSION_EXACT)
	#		set(_ZeroMQ_ver_arg "${_ZeroMQ_ver_arg} EXACT")
	#	endif()
	#else()
	#	set(_ZeroMQ_ver_arg)
	#endif()
	#find_package(
	#	ZeroMQ ${_ZeroMQ_ver_arg}
	#	QUIET
	#	NO_MODULE
	#	PATHS "${IRODS_EXTERNALS_FULLPATH_ZMQ}"
	#	NO_DEFAULT_PATH
	#)
	
	# We also don't check pkg-config here, as there's no clean way to tell it where to look

	if (NOT TARGET libzmq)
		find_library(
			_ZeroMQ_library
			NAMES zmq libzmq
			PATHS "${IRODS_EXTERNALS_FULLPATH_ZMQ}/lib"
			NO_DEFAULT_PATH
		)
		find_path(
			_ZeroMQ_include_dir
			NAMES "zmq.h"
			PATHS "${IRODS_EXTERNALS_FULLPATH_ZMQ}/include"
			NO_DEFAULT_PATH
		)
		_ZeroMQ_create_target(_ZeroMQ_library _ZeroMQ_include_dir)
	endif()
endif()

if (NOT TARGET ZeroMQ::libzmq)

	# Normally we would search for ZeroMQ's cmake config here, but the cmake config for ZeroMQ puts
	# private libs in INTERFACE_LINK_LIBRARIES, and it's not worthwhile to try and clean up behind it.

	find_package(PkgConfig QUIET)
	if (PKG_CONFIG_FOUND)
		pkg_check_modules(PC_LIBZMQ QUIET libzmq)
	endif()
	_ZeroMQ_create_target(_ZeroMQ_library _ZeroMQ_include_dir)

	find_library(_ZeroMQ_library NAMES zmq libzmq)
	find_path(_ZeroMQ_include_dir NAMES zmq.h)
	_ZeroMQ_create_target(_ZeroMQ_library _ZeroMQ_include_dir)
endif()

if (TARGET ZeroMQ::libzmq)
	_ZeroMQ_fix_properties()
endif()

include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(
	ZeroMQ
	REQUIRED_VARS ZeroMQ_LIBRARIES ZeroMQ_INCLUDE_DIRS
	VERSION_VAR ZeroMQ_VERSION
)

if (ZeroMQ_DIR STREQUAL "ZeroMQ_DIR-NOTFOUND")
	unset(ZeroMQ_DIR CACHE)
endif()


############

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
