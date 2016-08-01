//
// Created by roman on 7/31/16.
//

#ifndef PYTHON_PARQUET_DELEGATECONVERTERS_H
#define PYTHON_PARQUET_DELEGATECONVERTERS_H


#include "PrimitiveFieldConverter.h"
#include "RecordConverter.h"
#include "MaterializingFileReader.h"
#include <stdexcept>

namespace parquet {
    namespace python {
        namespace delegate {
            typedef void (*Int32ConverterDelegate)(int, bool);

            typedef void (*Int64ConverterDelegate)(long, bool);

            typedef void (*DoubleConverterDelegate)(double, bool);

            typedef void (*FloatConverterDelegate)(float, bool);

            typedef void (*BoolConverterDelegate)(bool, bool);

            typedef void (*BinaryConverterDelegate)(const uint8_t *, int, bool);

            class DelegatePrimitiveFieldConverter : public PrimitiveFieldConverter {
            private:
                Int32ConverterDelegate int32_converter;
                Int64ConverterDelegate int64_converter;
                DoubleConverterDelegate double_converter;
                FloatConverterDelegate float_converter;
                BoolConverterDelegate bool_converter;
                BinaryConverterDelegate binary_converter;
            public:
                DelegatePrimitiveFieldConverter(Int32ConverterDelegate int32_converter,
                                                Int64ConverterDelegate int64_converter,
                                                DoubleConverterDelegate double_converter,
                                                FloatConverterDelegate float_converter,
                                                BoolConverterDelegate bool_converter,
                                                BinaryConverterDelegate binary_converter)
                        : int32_converter(int32_converter), int64_converter(int64_converter),
                          double_converter(double_converter),
                          float_converter(float_converter), bool_converter(bool_converter),
                          binary_converter(binary_converter) {

                }

                inline virtual void add_int(int value, bool is_null) {
                    if (int32_converter != nullptr) int32_converter(value, is_null);
                }

                inline virtual void add_long(long value, bool is_null) {
                    if (int64_converter != nullptr) int64_converter(value, is_null);
                }

                inline virtual void add_binary(const uint8_t *value, int length, bool is_null) {
                    if (binary_converter != nullptr) binary_converter(value, length, is_null);
                }

                inline virtual void add_float(float value, bool is_null) {
                    if (float_converter != nullptr) float_converter(value, is_null);
                }

                inline virtual void add_double(double value, bool is_null) {
                    if (double_converter != nullptr) double_converter(value, is_null);
                }

                inline virtual void add_boolean(bool value, bool is_null) {
                    if (bool_converter != nullptr) bool_converter(value, is_null);
                }
            };

            typedef void (*GroupStartDelegate)();
            typedef void (*GroupEndDelegate)();
            typedef void* (*GetCurrentRecordDelegate)();
            typedef shared_ptr<FieldConverter> (*GetFieldConverterDelegate)(int);

            class DelegateGroupConverter : public TypedGroupConverter<void>{
            private:
                GroupStartDelegate  group_start;
                GroupEndDelegate group_end;
                GetCurrentRecordDelegate current_record;
                GetFieldConverterDelegate field_converter;

            public:
                DelegateGroupConverter(GroupStartDelegate  group_start,
                    GroupEndDelegate group_end,
                    GetCurrentRecordDelegate current_record,
                    GetFieldConverterDelegate field_converter)
                        : group_start(group_start), group_end(group_end), current_record(current_record),
                          field_converter(field_converter)
                {
                    if( group_start == nullptr)
                        throw new std::invalid_argument("group_start");

                    if( group_end == nullptr)
                        throw new std::invalid_argument("group_end");

                    if( current_record == nullptr)
                        throw new std::invalid_argument("current_record");

                    if( field_converter == nullptr)
                        throw new std::invalid_argument("field_converter");
                }

                virtual bool is_primitive() { return false; }

                inline virtual void* get_current_record_untyped(){
                    return current_record();
                }

                inline virtual void start() {
                    group_start();
                }

                inline virtual void end(){
                    group_end();
                }

                virtual shared_ptr<FieldConverter> get_converter(int i) {
                    return field_converter(i);
                }
                virtual void* get_current_record(){
                    return get_current_record_untyped();
                }
            };

            class DelegateFileReader : public MaterializingParquetFileReader<void>
            {

            private:
                static unique_ptr<RandomAccessSource> get_open_source(string uri){
                    LocalFileSource *lfs = new LocalFileSource();
                    lfs->Open(uri);
                    return move(unique_ptr<RandomAccessSource>(static_cast<RandomAccessSource*>(lfs)));
                }
            public:
                DelegateFileReader(string file_name)
                        : MaterializingParquetFileReader(move(DelegateFileReader::get_open_source(file_name)))
                { }

                void set_root_converter(shared_ptr<DelegateGroupConverter> root_converter){
                    this->set_root_conveter(static_pointer_cast<TypedGroupConverter<void>>(root_converter));
                }
            };
        }
    }
}
#endif //PYTHON_PARQUET_DELEGATECONVERTERS_H
