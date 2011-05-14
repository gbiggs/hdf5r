cmake_minimum_required(VERSION 2.6 FATAL_ERROR)

project(HDF5R)
string(TOLOWER ${PROJECT_NAME} PROJECT_NAME_LOWER)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
    "${CMAKE_SOURCE_DIR}/cmake/Modules/")
include(${PROJECT_SOURCE_DIR}/cmake/hdf5r_utils.cmake)

# Set version and OS-specific settings
set(HDF5R_VERSION 1.0.0 CACHE STRING "HDF5R version")
DISSECT_VERSION()
GET_OS_INFO()

# Add an "uninstall" target
CONFIGURE_FILE ("${PROJECT_SOURCE_DIR}/cmake/uninstall_target.cmake.in"
    "${PROJECT_BINARY_DIR}/uninstall_target.cmake" IMMEDIATE @ONLY)
ADD_CUSTOM_TARGET (uninstall "${CMAKE_COMMAND}" -P
    "${PROJECT_BINARY_DIR}/uninstall_target.cmake")

# Global options
option(BUILD_SHARED "Build shared libraries." ON)
if(BUILD_SHARED)
    set(HDF5R_SHARED "SHARED")
else(BUILD_SHARED)
    set(HDF5R_SHARED "STATIC")
endif(BUILD_SHARED)

# Global dependencies
if(NOT BUILD_SHARED)
    set(HDF5_USE_STATIC_LIBRARIES TRUE)
endif(NOT BUILD_SHARED)
find_package(HDF5 REQUIRED)
include_directories(${HDF5_INCLUDE_DIRS})
link_directories(${HDF5_LIBRARY_DIRS})
add_definitions(${HDF5_DEFINITIONS})

# Subdirectories
add_subdirectory(src)
add_subdirectory(examples)

# Package creation
include(InstallRequiredSystemLibraries)
set(HDF5R_DESCRIPTION "HDF5R logging library")
set(HDF5R_VENDOR "RETF")
configure_file("${PROJECT_SOURCE_DIR}/cmake/cpack_options.cmake.in"
    "${PROJECT_BINARY_DIR}/cpack_options.cmake" @ONLY)
set(CPACK_PROJECT_CONFIG_FILE "${PROJECT_BINARY_DIR}/cpack_options.cmake")
include(CPack)

