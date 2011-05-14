set(srcs hdf5r.cpp
    )
set(hdrs ${PROJECT_SOURCE_DIR}/include/hdf5r/hdf5r.h
    )

include_directories(${PROJECT_SOURCE_DIR}/include)

set(lib_name "hdf5r")
add_library(${lib_name} ${HDF5R_SHARED} ${srcs})
target_link_libraries(${lib_name} ${HDF5_LIBRARIES})
install(FILES ${hdrs} DESTINATION ${INCLUDE_INSTALL_DIR}
    COMPONENT headers)
install(TARGETS ${lib_name} LIBRARY DESTINATION ${LIB_INSTALL_DIR}
    COMPONENT libs)

