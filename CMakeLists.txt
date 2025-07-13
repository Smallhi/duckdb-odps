cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME odps)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES 
    src/odps_extension.cpp
    src/odps_reader.cpp
    src/odps_storage_reader.cpp
    src/odps_tunnel_reader.cpp
    src/odps_sql_executor.cpp
    src/odps_table_function.cpp
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link dependencies in both the static library as the loadable extension
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

# Add Arrow support if available
find_package(Arrow QUIET)
if(Arrow_FOUND AND TARGET arrow::arrow_shared)
    target_link_libraries(${EXTENSION_NAME} arrow::arrow_shared)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} arrow::arrow_shared)
    message(STATUS "Arrow found, building with Arrow support")
elseif(Arrow_FOUND AND TARGET Arrow::Arrow)
    target_link_libraries(${EXTENSION_NAME} Arrow::Arrow)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} Arrow::Arrow)
    message(STATUS "Arrow found, building with Arrow support")
else()
    # Arrow not found, but we can still build without it
    message(STATUS "Arrow not found, building without Arrow support")
endif()

# Add httplib support
find_package(httplib QUIET)
if(httplib_FOUND)
    target_link_libraries(${EXTENSION_NAME} httplib::httplib)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} httplib::httplib)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
