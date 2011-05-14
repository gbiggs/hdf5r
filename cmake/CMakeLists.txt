set(cmake_modules hdf5r_utils.cmake)

install(FILES ${cmake_modules} DESTINATION share/cmake/Modules
    COMPONENT cmakestuff)

set(PKG_DESC "HDF5R logging library")
set(PKG_EXTERNAL_DEPS "")
set(pkg_conf_file ${CMAKE_CURRENT_BINARY_DIR}/hdf5r.pc)
configure_file(hdf5r.pc.in ${pkg_conf_file} @ONLY)
install(FILES ${pkg_conf_file}
    DESTINATION ${LIB_INSTALL_DIR}/pkgconfig/ COMPONENT pkgconfig)

