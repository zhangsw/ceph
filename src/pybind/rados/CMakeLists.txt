

add_custom_target(rados.py
  COMMAND
  mkdir -p ../build &&
  LDFLAGS=-L${CMAKE_BINARY_DIR}/src
  CFLAGS=\"-I${CMAKE_SOURCE_DIR}/src/include -std=c++11\"
  python ${CMAKE_SOURCE_DIR}/src/pybind/rados/setup.py build --build-base ../build --verbose 
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/src/pybind/rados
  DEPENDS rados
  COMMENT "rados.py is being created")

set_property(TARGET
  rados.py
  PROPERTY ENVIRONMENT
  CYTHON_BUILD_DIR=${CMAKE_SOURCE_DIR}/src/pybind/build
  )

