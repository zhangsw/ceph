include_directories(${CMAKE_SOURCE_DIR}/src/include})

add_subdirectory(rados)
add_subdirectory(cephfs)

add_custom_target(run ALL
    DEPENDS rados.py cephfs.py)
