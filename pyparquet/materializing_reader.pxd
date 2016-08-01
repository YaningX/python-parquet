from libcpp.memory cimport shared_ptr, unique_ptr
from libc.stdint cimport uint8_t, int32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp cimport bool

cdef extern from "Delegates.h" namespace "parquet::python::delegate":

    cdef cppclass FieldConverter:
        bool is_primitive()

    ctypedef void (*Int32ConverterDelegate)(int, bool);
    ctypedef void (*Int64ConverterDelegate)(long, bool);
    ctypedef void (*DoubleConverterDelegate)(double, bool);
    ctypedef void (*FloatConverterDelegate)(float, bool);
    ctypedef void (*BoolConverterDelegate)(bool, bool);
    ctypedef void (*BinaryConverterDelegate)(const uint8_t *, int, bool);
    ctypedef void (*GroupStartDelegate)();
    ctypedef void (*GroupEndDelegate)();
    ctypedef void* (*GetCurrentRecordDelegate)();
    ctypedef shared_ptr[FieldConverter] (*GetFieldConverterDelegate)(int);

    cdef cppclass DelegatePrimitiveFieldConverter:
        DelegatePrimitiveFieldConverter(Int32ConverterDelegate int32_converter,
                                                Int64ConverterDelegate int64_converter,
                                                DoubleConverterDelegate double_converter,
                                                FloatConverterDelegate float_converter,
                                                BoolConverterDelegate bool_converter,
                                                BinaryConverterDelegate binary_converter)


    cdef cppclass DelegateGroupConverter:
        DelegateGroupConverter(GroupStartDelegate  group_start,
                    GroupEndDelegate group_end,
                    GetCurrentRecordDelegate current_record,
                    GetFieldConverterDelegate field_converter)

    cdef cppclass DelegateFileReader:
        DelegateFileReader(string file_name, shared_ptr[DelegateGroupConverter] root_converter)

cdef extern from "PythonSchemaGenerator.h" namespace "parquet::python::schema":

    cdef cppclass Field:
        string logical_type
        string name
        bool is_primitive
        bool is_optional
        bool is_repeated
        bool is_required

    cdef cppclass PrimitiveField(Field):
        string type
        int length
        int precision
        int scale

    cdef cppclass GroupField(Field):
        shared_ptr[vector[shared_ptr[Field]]] fields

    cdef cppclass SchemaConverter:
        unique_ptr[Field] convert(shared_ptr[DelegateFileReader] reader)


