add_executable(
  mytest
  "${CMAKE_CURRENT_SOURCE_DIR}/test.c"
)
set_property(TARGET mytest PROPERTY VERSION ${IRODS_VERSION})
target_link_libraries(
  mytest
  PRIVATE
  RodsAPIs
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_chrono.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_filesystem.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_regex.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_system.so"
  "${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_thread.so"
)
