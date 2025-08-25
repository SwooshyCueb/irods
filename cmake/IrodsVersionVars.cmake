#[=======================================================================[.rst:
IrodsVersionVars
----------------

This file defines the following variables:

``IRODS_VERSION``
  Version of iRODS itself.
``IRODS_SOVERSION``
  soname version for iRODS libraries.
``IRODS_SOVERSION_MAJOR``
  Major version component of :variable:`IRODS_SOVERSION`.
``IRODS_SOVERSION_MINOR``
  Minor version component of :variable:`IRODS_SOVERSION`.
``IRODS_SOVERSION_PATCH``
  Patch version component of :variable:`IRODS_SOVERSION`. Undefined if not used.
``IRODS_IS_DEVELOPMENT_VERSION``
  Whether or not the version of iRODS is a development version for an upcoming
  release.
``IRODS_IS_DEVELOPMENT_VERSION_IMMINENT``
  Whether or not the version of iRODS is a development version for an imminent
  release.
``IRODS_IS_DEVELOPMENT_MAJOR_VERSION``
  Whether or not the version of iRODS is a development version for an upcoming
  major release.
``IRODS_IS_DEVELOPMENT_MAJOR_VERSION_IMMINENT``
  Whether or not the version of iRODS is a development version for an imminent
  major release.
``IRODS_IS_DEVELOPMENT_MINOR_VERSION``
  Whether or not the version of iRODS is a development version for an upcoming
  minor release.
``IRODS_IS_DEVELOPMENT_MINOR_VERSION_IMMINENT``
  Whether or not the version of iRODS is a development version for an imminent
  minor release.
``IRODS_DEVELOPMENT_VERSION``
  The upcoming release for which iRODS is a development version. Undefined if
  iRODS is not a development version.
``IRODS_DEVELOPMENT_VERSION_MAJOR``
  Major version component of :variable:`IRODS_DEVELOPMENT_VERSION`. Undefined
  if iRODS is not a development version.
``IRODS_DEVELOPMENT_VERSION_MINOR``
  Minor version component of :variable:`IRODS_DEVELOPMENT_VERSION`. Undefined
  if iRODS is not a development version.
``IRODS_DEVELOPMENT_VERSION_PATCH``
  Patch version component of :variable:`IRODS_DEVELOPMENT_VERSION`. Undefined
  if iRODS is not a development version.

These variables are stored in the CMake package configuration in case they are
needed by downstream projects.

#]=======================================================================]

# IRODS_VERSION is simple.
set(IRODS_VERSION "${IRODS_VERSION_MAJOR}.${IRODS_VERSION_MINOR}.${IRODS_VERSION_PATCH}")

# In general, soname versions are just the major and minor components of the
# full version number.
set(IRODS_SOVERSION_MAJOR "${IRODS_VERSION_MAJOR}")
set(IRODS_SOVERSION_MINOR "${IRODS_VERSION_MINOR}")

# Defaults for IRODS_IS_DEVELOPMENT_*
set(IRODS_IS_DEVELOPMENT_VERSION FALSE)
set(IRODS_IS_DEVELOPMENT_VERSION_IMMINENT FALSE)
set(IRODS_IS_DEVELOPMENT_MAJOR_VERSION FALSE)
set(IRODS_IS_DEVELOPMENT_MAJOR_VERSION_IMMINENT FALSE)
set(IRODS_IS_DEVELOPMENT_MINOR_VERSION FALSE)
set(IRODS_IS_DEVELOPMENT_MINOR_VERSION_IMMINENT FALSE)

if (IRODS_VERSION_MINOR GREATER_EQUAL 90)
  # A minor version component of 90 or greater is currently used to indicate
  # that iRODS is a development version for an upcoming major release.
  set(IRODS_IS_DEVELOPMENT_VERSION TRUE)
  set(IRODS_IS_DEVELOPMENT_MAJOR_VERSION TRUE)
  math(EXPR IRODS_DEVELOPMENT_VERSION_MAJOR "${IRODS_VERSION_MAJOR} + 1")
  set(IRODS_DEVELOPMENT_VERSION_MINOR "0")
  set(IRODS_DEVELOPMENT_VERSION_PATCH "0")

  if (IRODS_VERSION_MINOR GREATER_EQUAL 100)
    # While we don't often do this, a minor version component of 100 or greater
    # can be used to indicate that iRODS is a development version for an
    # imminent major release.
    set(IRODS_IS_DEVELOPMENT_VERSION_IMMINENT TRUE)
    set(IRODS_IS_DEVELOPMENT_MAJOR_VERSION_IMMINENT TRUE)
  endif()
endif()

if (IRODS_VERSION_PATCH GREATER_EQUAL 90)
  # Similar to the case with minor/major, a patch version component of 90 or
  # greater is currently used to indicate taht iRODS is a development version
  # for an upcoming minor release.
  set(IRODS_IS_DEVELOPMENT_VERSION TRUE)
  set(IRODS_IS_DEVELOPMENT_MINOR_VERSION TRUE)
  set(IRODS_DEVELOPMENT_VERSION_MAJOR "${IRODS_VERSION_MAJOR}")
  math(EXPR IRODS_DEVELOPMENT_VERSION_MINOR "${IRODS_VERSION_MINOR} + 1")
  set(IRODS_DEVELOPMENT_VERSION_PATCH "0")

  if (IRODS_VERSION_PATCH GREATER_EQUAL 100)
    # While we don't often do this, a patch version component of 100 or greater
    # can be used to indicate that iRODS is a development version for an
    # imminent minor release.
    set(IRODS_IS_DEVELOPMENT_VERSION_IMMINENT TRUE)
    set(IRODS_IS_DEVELOPMENT_MINOR_VERSION_IMMINENT TRUE)
  endif()
endif()

if (IRODS_IS_DEVELOPMENT_MAJOR_VERSION AND IRODS_IS_DEVELOPMENT_MINOR_VERSION)
  message(FATAL_ERROR "iRODS version \"${IRODS_VERSION}\" indicates a development version for both major and minor releases.")
endif()

if (IRODS_IS_DEVELOPMENT_VERSION_IMMINENT)
  # For development versions for imminent releases, the soname should match the
  # upcoming release.
  set(IRODS_SOVERSION_MAJOR "${IRODS_DEVELOPMENT_VERSION_MAJOR}")
  set(IRODS_SOVERSION_MINOR "${IRODS_DEVELOPMENT_VERSION_MINOR}")
elseif (IRODS_IS_DEVELOPMENT_MINOR_VERSION)
  # For development versions of non-imminent minor releases, the soname version
  # should contain the patch version component.
  set(IRODS_SOVERSION_PATCH "${IRODS_VERSION_PATCH}")
endif()

if (IRODS_IS_DEVELOPMENT_VERSION)
  set(IRODS_DEVELOPMENT_VERSION "${IRODS_DEVELOPMENT_VERSION_MAJOR}.${IRODS_DEVELOPMENT_VERSION_MINOR}.${IRODS_DEVELOPMENT_VERSION_PATCH}")
endif()

set(IRODS_SOVERSION "${IRODS_SOVERSION_MAJOR}.${IRODS_SOVERSION_MINOR}")
if (DEFINED IRODS_SOVERSION_PATCH)
  set(IRODS_SOVERSION "${IRODS_SOVERSION}.${IRODS_SOVERSION_PATCH}")
endif()
