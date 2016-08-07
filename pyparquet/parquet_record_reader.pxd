from libcpp.memory cimport shared_ptr, unique_ptr
from libc.stdint cimport uint8_t, int32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp cimport bool
from parquet cimport NodePtr, NodeVector

cdef extern from "memory" namespace "std":
    cdef shared_ptr[T] static_pointer_cast[T, U]( const shared_ptr[U]& r ) except +

cdef extern from "Delegates.h" namespace "parquet::python":

    cdef cppclass FieldConverter:
        bool is_primitive()

cdef extern from "Delegates.h" namespace "parquet::python::delegate":

    ctypedef void (*Int32ConverterDelegate)(void*, int, bool);
    ctypedef void (*Int64ConverterDelegate)(void*,long, bool);
    ctypedef void (*DoubleConverterDelegate)(void*,double, bool);
    ctypedef void (*FloatConverterDelegate)(void*,float, bool);
    ctypedef void (*BoolConverterDelegate)(void*,bool, bool);
    ctypedef void (*BinaryConverterDelegate)(void*,const uint8_t *, int, bool);
    ctypedef void (*GroupStartDelegate)(void*);
    ctypedef void (*GroupEndDelegate)(void*);
    ctypedef void* (*GetCurrentRecordDelegate)(void*);
    ctypedef shared_ptr[FieldConverter] (*GetFieldConverterDelegate)(void*,int);

    cdef cppclass DelegatePrimitiveFieldConverter:
        DelegatePrimitiveFieldConverter(void* context,Int32ConverterDelegate int32_converter,
                                                Int64ConverterDelegate int64_converter,
                                                DoubleConverterDelegate double_converter,
                                                FloatConverterDelegate float_converter,
                                                BoolConverterDelegate bool_converter,
                                                BinaryConverterDelegate binary_converter)


    cdef cppclass DelegateGroupConverter(FieldConverter):
        DelegateGroupConverter(void *context, GroupStartDelegate  group_start,
                    GroupEndDelegate group_end,
                    GetCurrentRecordDelegate current_record,
                    GetFieldConverterDelegate field_converter)

    cdef cppclass DelegateFileReader:
        DelegateFileReader(string file_name)
        void set_root_converter(shared_ptr[DelegateGroupConverter] root_converter)
        const NodePtr& schema_root()

        int num_rows()
        int num_row_groups()

        void* read_next()
        void close()

