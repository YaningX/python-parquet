cmake_minimum_required(VERSION 3.5)
project(python_parquet)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(SOURCE_FILES parquet-record-reader/cpp/main.cpp)
set(LIB_SOURCE_FILES parquet-record-reader/cpp/MaterializingFileReader.cpp parquet-record-reader/include/MaterializingFileReader.h parquet-record-reader/include/PrimitiveFieldConverter.h parquet-record-reader/include/PrimitiveFieldProcessor.h parquet-record-reader/include/RecordReader.h parquet-record-reader/include/Processor.h parquet-record-reader/include/Converter.h parquet-record-reader/include/RecordConverter.h parquet-record-reader/cpp/RecordReader.cpp parquet-record-reader/cpp/PrimitiveFieldProcessor.cpp parquet-record-reader/include/Delegates.h)
add_executable(python_parquet ${SOURCE_FILES})
add_library(parquet-record-reader ${LIB_SOURCE_FILES})
target_link_libraries(python_parquet parquet thrift parquet-record-reader)

if (DEFINED $ENV{CMAKE_OUTPUT_DIR})
    set(LIBRARY_OUTPUT_PATH  $ENV{CMAKE_OUTPUT_DIR})
    set(EXECUTABLE_OUTPUT_PATH   $ENV{CMAKE_OUTPUT_DIR})
endif()