include_directories(${PROJECT_SOURCE_DIR}/include)

add_executable(hdf5r_sample example.cpp)
target_link_libraries(hdf5r_sample hdf5r ${HDF5_LIBRARIES} rt)
install(TARGETS hdf5r_sample RUNTIME DESTINATION ${BIN_INSTALL_DIR}
    COMPONENT examples)

