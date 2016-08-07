from parquet_record_reader cimport DelegateFileReader, DelegatePrimitiveFieldConverter, DelegateGroupConverter
from parquet_record_reader cimport static_pointer_cast, FieldConverter
from parquet cimport *
from parquet cimport ParquetLogicalType, ParquetPhysicalType, ParquetRepetition, ParquetNodeType, Node
from parquet cimport ParquetPrimitiveNode, ParquetGroupNode
from parquet cimport NodePtr, NodeVector
from parquet cimport make_primitive_node, make_group_node
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp cimport bool
from cpython.ref cimport PyObject
from libcpp.memory cimport shared_ptr, unique_ptr




cdef class REPETITION:
    REQUIRED = <int>REPETITION_REQUIRED
    OPTIONAL = <int>REPETITION_OPTIONAL
    REPEATED = <int>REPETITION_REPEATED

class LOGICAL_TYPE:
    NONE = <int>LOGICAL_TYPE_NONE
    UTF8 = <int>LOGICAL_TYPE_UTF8
    MAP = <int>LOGICAL_TYPE_MAP
    MAP_KEY_VALUE = <int>LOGICAL_TYPE_MAP_KEY_VALUE
    LIST = <int>LOGICAL_TYPE_LIST
    ENUM = <int>LOGICAL_TYPE_ENUM
    DECIMAL = <int>LOGICAL_TYPE_DECIMAL
    DATE = <int>LOGICAL_TYPE_DATE
    TIME_MILLIS = <int>LOGICAL_TYPE_TIME_MILLIS
    TIMESTAMP_MILLIS = <int>LOGICAL_TYPE_TIMESTAMP_MILLIS
    UINT_8 = <int>LOGICAL_TYPE_UINT_8
    UINT_16 = <int>LOGICAL_TYPE_UINT_16
    UINT_32 = <int>LOGICAL_TYPE_UINT_32
    UINT_64 = <int>LOGICAL_TYPE_UINT_64
    INT_8 = <int>LOGICAL_TYPE_INT_8
    INT_16 = <int>LOGICAL_TYPE_INT_16
    INT_32 = <int>LOGICAL_TYPE_INT_32
    INT_64 = <int>LOGICAL_TYPE_INT_64
    JSON = <int>LOGICAL_TYPE_JSON
    BSON = <int>LOGICAL_TYPE_BSON
    INTERVAL = <int>LOGICAL_TYPE_INTERVAL
    
class PHYSICAL_TYPE:
    BOOLEAN = <int>PHYSICAL_TYPE_BOOLEAN
    INT32 = <int>PHYSICAL_TYPE_INT32
    INT64 = <int>PHYSICAL_TYPE_INT64
    INT96 = <int>PHYSICAL_TYPE_INT96
    FLOAT = <int>PHYSICAL_TYPE_FLOAT
    DOUBLE = <int>PHYSICAL_TYPE_DOUBLE
    BYTE_ARRAY = <int>PHYSICAL_TYPE_BYTE_ARRAY
    FIXED_LEN_BYTE_ARRAY = <int>PHYSICAL_TYPE_FIXED_LEN_BYTE_ARRAY    

class NODE_TYPE:
    PRIMITIVE = <int>NODE_TYPE_PRIMITIVE
    GROUP = <int>NODE_TYPE_GROUP
    
cdef class PrimitiveNode:
    cdef NodePtr __field

    def __cinit__(self, name = None, repetition = REPETITION.OPTIONAL, type = PHYSICAL_TYPE.INT64,
                    logical_type = LOGICAL_TYPE.NONE, length = -1, precision = -1, scale = -1,
                    skip_init = False):
        if not skip_init:
            if not name:
                raise RuntimeError("Name must be specified.")
            self.__build_from_py(length, <int>logical_type, name, precision, <int>repetition, scale, <int>type)

    cdef __build_from_py(self, int length, int logical_type, char* name, int precision,
                         int repetition, int scale, int type):
        self.__field = make_primitive_node(string(name), <ParquetRepetition>repetition,
                                           <ParquetPhysicalType>type, <ParquetLogicalType>logical_type,
                                           length, precision, scale)

    @staticmethod
    cdef PrimitiveNode make_from_parquet(NodePtr field):
        value = PrimitiveNode(skip_init=True)
        value.__field = field

        return value

    cdef ParquetPrimitiveNode* __get_ptr(self):
        return <ParquetPrimitiveNode*> self.__field.get()

    cdef NodePtr get_ptr(self):
        return self.__field

    property logical_type:
        def __get__(self):
            return <int>self.__get_ptr().logical_type()

    property  name:
        def __get__(self):
            return self.__get_ptr().name().c_str()

    property node_type:
        def __get__(self):
            return <int>self.__get_ptr().node_type()

    property repetition:
        def __get__(self):
            return <int>self.__get_ptr().repetition()
        
    property is_primitive:
        def __get__(self):
            return self.__get_ptr().is_primitive()
    property is_group:
        def __get__(self):
            return self.__get_ptr().is_group()
    property is_optional:
        def __get__(self):
            return self.__get_ptr().is_optional()
    property is_repeated:
        def __get__(self):
            return self.__get_ptr().is_repeated()
    property is_required:
        def __get__(self):
            return self.__get_ptr().is_required()

    property physical_type:
        def __get__(self):
            return <int>self.__get_ptr().physical_type()

    property type_length:
        def __get__(self):
            return self.__get_ptr().type_length()

cdef class GroupNode:
    cdef NodePtr __field

    cdef public __fields

    def __cinit__(self, name = None, repetition = REPETITION.OPTIONAL,
                        fields = None, logical_type = LOGICAL_TYPE.NONE, skip_init = False):

        self.__fields = []
        if not skip_init:
            if not name:
                raise RuntimeError("Name must be specified.")

            if not fields:
                raise RuntimeError("Group must contain at least one field.")

            self.__build_from_py(fields, <int>logical_type, name, <int>repetition)

    cdef __build_from_py(self, fields, int logical_type, char* name, int repetition):
        cdef NodeVector vec
        for node in fields:
            if isinstance(node, PrimitiveNode):
                vec.push_back((<PrimitiveNode>node).get_ptr())
            elif isinstance(node, GroupNode):
                vec.push_back((<GroupNode>node).get_ptr())
        self.__fields = fields
        self.__field = make_group_node(string(name), <ParquetRepetition>repetition, vec,
                                       <ParquetLogicalType>logical_type)

    @staticmethod
    cdef GroupNode make_from_parquet(NodePtr field):
        value = GroupNode(skip_init=True)
        value.__field = field

        value.__fields = []

        cdef NodePtr node
        for i in xrange(value.field_count):
            node = (<ParquetGroupNode*>field.get()).field(i)
            value.__fields.append((PrimitiveNode.make_from_parquet(node) if node.get().is_primitive()
                                                                         else GroupNode.make_from_parquet(node)))

        return value

    cdef ParquetGroupNode* __get_ptr(self):
        return <ParquetGroupNode*> self.__field.get()

    cdef NodePtr get_ptr(self):
        return self.__field

    def field(self, i):
        return self.__fields[i]

    property fields:
        def __get__(self):
            for i in xrange(self.field_count):
                yield self.field(i)

    property field_count:
        def __get__(self):
            return self.__get_ptr().field_count()

    property logical_type:
        def __get__(self):
            return <int>self.__get_ptr().logical_type()

    property  name:
        def __get__(self):
            return self.__get_ptr().name().c_str()

    property node_type:
        def __get__(self):
            return <int>self.__get_ptr().node_type()

    property repetition:
        def __get__(self):
            return <int>self.__get_ptr().repetition()

    property is_primitive:
        def __get__(self):
            return self.__get_ptr().is_primitive()
    property is_group:
        def __get__(self):
            return self.__get_ptr().is_group()
    property is_optional:
        def __get__(self):
            return self.__get_ptr().is_optional()
    property is_repeated:
        def __get__(self):
            return self.__get_ptr().is_repeated()
    property is_required:
        def __get__(self):
            return self.__get_ptr().is_required()


cdef class RecordProvider:
    cdef void set_value(self, object value, int field_index):
        pass

cdef class ListConverter(RecordProvider):
    cdef RecordProvider __record_provider
    cdef int __field_index
    cdef shared_ptr[DelegateGroupConverter] __converter
    cdef list __record
    cdef int __field_count
    cdef list __converters
    cdef bool __properly_defined

    def __cinit__(self, RecordProvider record_provider, int field_index, GroupNode group_node):
        self.__converter = shared_ptr[DelegateGroupConverter](
            new DelegateGroupConverter(
                <void*>self, RecordConverter.group_start, RecordConverter.group_end,
                   RecordConverter.current_record,RecordConverter.field_converter))
        self.__record_provider = record_provider
        self.__field_index = field_index
        self.__field_count = group_node.field_count
        self.__record = None

        self.__converters = []
        cdef int i = 0

        self.__properly_defined = group_node.field_count == 1 and not group_node.field(0).is_primitive

        for field in group_node.fields: # type: GroupNode
            if field.is_primitive:
                self.__converters[i] = PrimitiveFieldConverter(self, i)
            else:
                if field.logical_type == LOGICAL_TYPE.LIST:
                    self.__converters[i] = ListConverter(self, i, <GroupNode>field)
                elif field.logical_type == LOGICAL_TYPE.MAP or field.logical_type == LOGICAL_TYPE.MAP_KEY_VALUE:
                    self.__converters[i] = MapConverter(self, i, <GroupNode>field)
                else:
                    self.__converters[i] = RecordConverter(self, i, <GroupNode>field)

            i += 1

    @staticmethod
    cdef void group_start(void* _self):
        self = <ListConverter>_self
        self.__record = []

    @staticmethod
    cdef void group_end(void* _self):
        self = <ListConverter>_self
        if self.__record_provider is not None:
            self.__record_provider.set_value(self.__record, self.__field_count)

    @staticmethod
    cdef void* current_record(void* _self):
        self = <ListConverter>_self
        return <void*><PyObject*>self.__record

    @staticmethod
    cdef shared_ptr[FieldConverter] field_converter(void* _self,int i):
        self = <ListConverter>_self
        conv = self.__converters[i]

        if isinstance(conv, PrimitiveFieldConverter):
            return static_pointer_cast[FieldConverter, DelegatePrimitiveFieldConverter]((<PrimitiveFieldConverter>conv).get_converter())
        elif isinstance(conv, MapConverter):
            return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<MapConverter>conv).get_converter())
        elif isinstance(conv, ListConverter):
            return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<ListConverter>conv).get_converter())

        return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<RecordConverter>conv).get_converter())

    cdef shared_ptr[DelegateGroupConverter] get_converter(self):
        return self.__converter

    cdef void set_value(self, object value, int field_index):
        if self.__properly_defined:
            self.__record.append(value[0])
        self.__record.append(value)

cdef class MapConverter(RecordProvider):
    cdef RecordProvider __record_provider
    cdef int __field_index
    cdef shared_ptr[DelegateGroupConverter] __converter
    cdef dict __record
    cdef int __field_count
    cdef list __converters

    def __cinit__(self, RecordProvider record_provider, int field_index, GroupNode group_node):
        self.__converter = shared_ptr[DelegateGroupConverter](
            new DelegateGroupConverter(
                <void*>self, RecordConverter.group_start, RecordConverter.group_end,
                   RecordConverter.current_record,RecordConverter.field_converter))
        self.__record_provider = record_provider
        self.__field_index = field_index
        self.__field_count = group_node.field_count
        self.__record = None

        self.__converters = []
        cdef int i = 0
        for field in group_node.fields: # type: GroupNode
            if field.is_primitive:
                self.__converters[i] = PrimitiveFieldConverter(self, i)
            else:
                self.__converters[i] = RecordConverter(self, i, <GroupNode>field)

            i += 1

    @staticmethod
    cdef void group_start(void* _self):
        self = <MapConverter>_self
        self.__record = {}

    @staticmethod
    cdef void group_end(void* _self):
        self = <MapConverter>_self
        if self.__record_provider is not None:
            self.__record_provider.set_value(self.__record, self.__field_count)

    @staticmethod
    cdef void* current_record(void* _self):
        self = <MapConverter>_self
        return <void*><PyObject*>self.__record

    @staticmethod
    cdef shared_ptr[FieldConverter] field_converter(void* _self,int i):
        self = <MapConverter>_self
        conv = self.__converters[i]

        if not isinstance(conv, RecordConverter):
            raise RuntimeError("Invalid file format: map's direct child is a primitive")

        return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<RecordConverter>conv).get_converter())

    cdef shared_ptr[DelegateGroupConverter] get_converter(self):
        return self.__converter

    cdef void set_value(self, object value, int field_index):
        self.__record[value[0]] = value[1]

cdef class RecordConverter(RecordProvider):

    cdef RecordProvider __record_provider
    cdef int __field_index
    cdef shared_ptr[DelegateGroupConverter] __converter
    cdef list __record
    cdef int __field_count
    cdef list __converters

    def __cinit__(self, RecordProvider record_provider, int field_index, GroupNode _group_node):

        group_node = _group_node.get()

        self.__converter = shared_ptr[DelegateGroupConverter](
            new DelegateGroupConverter(
                <void*>self, RecordConverter.group_start, RecordConverter.group_end,
                   RecordConverter.current_record,RecordConverter.field_converter))
        self.__record_provider = record_provider
        self.__field_index = field_index
        self.__field_count = group_node.field_count
        self.__record = None

        self.__converters = []
        cdef int i = 0
        for field in group_node.fields: # type: GroupNode
            if field.is_primitive:
                self.__converters[i] = PrimitiveFieldConverter(self, i)
            else:
                if field.logical_type == LOGICAL_TYPE.LIST:
                    self.__converters[i] = ListConverter(self, i, <GroupNode>field)
                elif field.logical_type == LOGICAL_TYPE.MAP or field.logical_type == LOGICAL_TYPE.MAP_KEY_VALUE:
                    self.__converters[i] = MapConverter(self, i, <GroupNode>field)
                else:
                    self.__converters[i] = RecordConverter(self, i, <GroupNode>field)

            i += 1

    @staticmethod
    cdef void group_start(void* _self):
        self = <RecordConverter>_self
        self.__record = [None] * self.__field_count

    @staticmethod
    cdef void group_end(void* _self):
        self = <RecordConverter>_self
        if self.__record_provider is not None:
            self.__record_provider.set_value(self.__record, self.__field_count)

    @staticmethod
    cdef void* current_record(void* _self):
        self = <RecordConverter>_self
        return <void*><PyObject*>self.__record

    @staticmethod
    cdef shared_ptr[FieldConverter] field_converter(void* _self,int i):
        self = <RecordConverter>_self
        conv = self.__converters[i]

        if isinstance(conv, PrimitiveFieldConverter):
            return static_pointer_cast[FieldConverter, DelegatePrimitiveFieldConverter]((<PrimitiveFieldConverter>conv).get_converter())
        elif isinstance(conv, MapConverter):
            return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<MapConverter>conv).get_converter())
        elif isinstance(conv, ListConverter):
            return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<ListConverter>conv).get_converter())

        return static_pointer_cast[FieldConverter, DelegateGroupConverter]((<RecordConverter>conv).get_converter())

    cdef shared_ptr[DelegateGroupConverter] get_converter(self):
        return self.__converter

    cdef void set_value(self, object value, int field_index):
        self.__record[field_index] = value

cdef class PrimitiveFieldConverter:

    cdef shared_ptr[DelegatePrimitiveFieldConverter] __converter
    cdef RecordProvider __record_provider
    cdef int __field_index

    @staticmethod
    cdef void __int32ConverterDelegate(void *_self, int value, bool is_null):
        self = <PrimitiveFieldConverter>_self
        self.__record_provider.set_value(value if is_null else None, self.__field_index)

    @staticmethod
    cdef void __int64ConverterDelegate(void *_self, long value, bool is_null):
        self = <PrimitiveFieldConverter>_self
        self.__record_provider.set_value(value if is_null else None, self.__field_index)

    @staticmethod
    cdef void __doubleConverterDelegate(void *_self, double value, bool is_null):
        self = <PrimitiveFieldConverter>_self
        self.__record_provider.set_value(value if is_null else None, self.__field_index)

    @staticmethod
    cdef void __floatConverterDelegate(void *_self, float value, bool is_null):
        self = <PrimitiveFieldConverter>_self
        self.__record_provider.set_value(value if is_null else None, self.__field_index)

    @staticmethod
    cdef void __boolConverterDelegate(void *_self, bool value, bool is_null):
        self = <PrimitiveFieldConverter>_self
        self.__record_provider.set_value(value if is_null else None, self.__field_index)

    @staticmethod
    cdef void __binaryConverterDelegate(void *_self, const uint8_t *value, int length, bool is_null):
        self = <PrimitiveFieldConverter>_self
        if is_null:
            self.__record_provider(None, self.__field_index)
        else:
            self.__record_provider((<const char*>value)[:length], self.__field_index)

    def __cinit__(self, RecordProvider record_provider, int field_index):
        self.__converter = shared_ptr[DelegatePrimitiveFieldConverter](
            new DelegatePrimitiveFieldConverter(<void*><PyObject*>self,
                                            PrimitiveFieldConverter.__int32ConverterDelegate,
                                            PrimitiveFieldConverter.__int64ConverterDelegate,
                                            PrimitiveFieldConverter.__doubleConverterDelegate,
                                            PrimitiveFieldConverter.__floatConverterDelegate,
                                            PrimitiveFieldConverter.__boolConverterDelegate,
                                            PrimitiveFieldConverter.__binaryConverterDelegate))

        self.__record_provider = record_provider
        self.__field_index = field_index

    cdef shared_ptr[DelegatePrimitiveFieldConverter] get_converter(self):
        return self.__converter

cdef class ParquetReader:
    cdef shared_ptr[DelegateFileReader] __reader
    cdef str __path
    cdef GroupNode __schema
    cdef RecordConverter __root_converter

    def __cinit__(self, path):
        self.__path = path
        self.__reader = shared_ptr[DelegateFileReader](NULL)
        self.__schema = None

    def __enter__(self):

        bytes_path = self.__path.encode('utf-8')
        self.__reader = shared_ptr[DelegateFileReader](new DelegateFileReader(string(<char*>bytes_path)))
        self.__schema = GroupNode.make_from_parquet(self.__reader.get().schema_root())
        self.__root_converter = RecordConverter(None, 0, self.__schema)
        self.__reader.get().set_root_converter(self.__root_converter.get_converter())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__reader.get() == NULL:
            self.__reader.get().close()

        self.__reader = shared_ptr[DelegateFileReader](NULL)
        self.__schema = None

    def next(self):
        cdef void *ptr
        ptr = self.__reader.get().read_next()
        if ptr == NULL:
            return None

        return_val = <object>ptr

        return return_val

    property path:
        def __get__(self):
            return self.__path

    property schema:
        def __get__(self):
            return self.__schema