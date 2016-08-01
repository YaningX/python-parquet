from materializing_reader cimport DelegateFileReader, Field as CPPField
from materializing_reader cimport PrimitiveField as CPPPrimitiveField, GroupField as CPPGroupField
from materializing_reader cimport shared_ptr, string, bool, vector
from cpython.ref cimport PyObject

class PhysicalType(object):
    BOOLEAN = 'BOOLEAN'
    INT32 = 'INT32'
    INT64 = 'INT64'
    INT96 = 'INT96'
    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE'
    BYTE_ARRAY = 'BYTE_ARRAY'
    FIXED_LEN_BYTE_ARRAY = 'FIXED_LEN_BYTE_ARRAY'

class LogicalType(object):
    NONE = 'NONE'
    UTF8 = 'UTF8'
    MAP = 'MAP'
    MAP_KEY_VALUE = 'MAP_KEY_VALUE'
    LIST = 'LIST'
    ENUM = 'ENUM'
    DECIMAL = 'DECIMAL'
    DATE = 'DATE'
    TIME_MILLIS = 'TIME_MILLIS'
    TIMESTAMP_MILLIS = 'TIMESTAMP_MILLIS'
    UINT_8 = 'UINT_8'
    UINT_16 = 'UINT_16'
    UINT_32 = 'UINT_32'
    UINT_64 = 'UINT_64'
    INT_8 = 'INT_8'
    INT_16 = 'INT_16'
    INT_32 = 'INT_32'
    INT_64 = 'INT_64'
    JSON = 'JSON'
    BSON = 'BSON'
    INTERVAL = 'INTERVAL'

cdef class Field:
    cdef shared_ptr[CPPField] __field

    cdef init_field(self, shared_ptr[CPPField] cpp_field, name=None, is_optional=True, is_repeated=False,
                  is_required=False, logical_type=None, process_args=True):

        flags = [is_required, is_optional, is_repeated]
        it = flags.__iter__()
        if any(it) and not any(it):
            raise RuntimeError('Only one of is_primitive, is_optional, is_repeated can be set')

        if cpp_field == NULL:
            raise RuntimeError("cpp_field must be provided")

        self.__field = cpp_field
        if process_args:
            if not name:
                raise RuntimeError('name can not be none or empty')
            self.logical_type = logical_type
            self.name = name
            self.is_repeated = is_repeated
            self.is_optional = is_optional
            self.is_repeated = is_repeated

    property logical_type:
        def __get__(self):
            cdef bytes b = self.__field.get().logical_type.c_str()
            return b
        def __set__(self, value):
            if not isinstance(value, (str,unicode)):
                raise RuntimeError('Value must be a string')
            self.__field.get().logical_type = string(<bytes>value)

    property name:
        def __get__(self):
            cdef bytes b = self.__field.get().logical_type.c_str()
            return b
        def __set__(self, value):
            if not isinstance(value, (str,unicode)):
                raise RuntimeError('Value must be a string')
            self.__field.get().name = string(<bytes>value)

    property is_primitive:
        def __get__(self):
            return self.__field.get().is_primitive


    property is_optional:
        def __get__(self):
            return self.__field.get().is_optional
        def __set__(self, value):
            self.__field.get().is_optional = value

    property is_repeated:
        def __get__(self):
            return self.__field.get().is_repeated
        def __set__(self, value):
            self.__field.get().is_repeated = value

    property is_required:
        def __get__(self):
            return self.__field.get().is_required
        def __set__(self, value):
            self.__field.get().is_required = value


