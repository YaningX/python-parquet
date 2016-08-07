from libcpp.memory cimport shared_ptr, unique_ptr
from libc.stdint cimport uint8_t, int32_t
from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp cimport bool


cdef extern from "parquet/api/schema.h" namespace "parquet::schema":

    cdef cppclass DecimalMetadata:
      bool isset
      int32_t scale
      int32_t precision

    cdef cppclass ParquetRepetition "parquet::Repetition::type":
        pass
    
    cdef ParquetRepetition REPETITION_REQUIRED "parquet::Repetition::type::REQUIRED"
    cdef ParquetRepetition REPETITION_OPTIONAL "parquet::Repetition::type::OPTIONAL"
    cdef ParquetRepetition REPETITION_REPEATED "parquet::Repetition::type::REPEATED"

    
    cdef cppclass ParquetLogicalType "parquet::LogicalType::type":
        pass
        
    cdef ParquetLogicalType LOGICAL_TYPE_NONE "parquet::LogicalType::type::NONE"
    cdef ParquetLogicalType LOGICAL_TYPE_UTF8 "parquet::LogicalType::type::UTF8"
    cdef ParquetLogicalType LOGICAL_TYPE_MAP "parquet::LogicalType::type::MAP"
    cdef ParquetLogicalType LOGICAL_TYPE_MAP_KEY_VALUE "parquet::LogicalType::type::MAP_KEY_VALUE"
    cdef ParquetLogicalType LOGICAL_TYPE_LIST "parquet::LogicalType::type::LIST"
    cdef ParquetLogicalType LOGICAL_TYPE_ENUM "parquet::LogicalType::type::ENUM"
    cdef ParquetLogicalType LOGICAL_TYPE_DECIMAL "parquet::LogicalType::type::DECIMAL"
    cdef ParquetLogicalType LOGICAL_TYPE_DATE "parquet::LogicalType::type::DATE"
    cdef ParquetLogicalType LOGICAL_TYPE_TIME_MILLIS "parquet::LogicalType::type::TIME_MILLIS"
    cdef ParquetLogicalType LOGICAL_TYPE_TIMESTAMP_MILLIS "parquet::LogicalType::type::TIMESTAMP_MILLIS"
    cdef ParquetLogicalType LOGICAL_TYPE_UINT_8 "parquet::LogicalType::type::UINT_8"
    cdef ParquetLogicalType LOGICAL_TYPE_UINT_16 "parquet::LogicalType::type::UINT_16"
    cdef ParquetLogicalType LOGICAL_TYPE_UINT_32 "parquet::LogicalType::type::UINT_32"
    cdef ParquetLogicalType LOGICAL_TYPE_UINT_64 "parquet::LogicalType::type::UINT_64"
    cdef ParquetLogicalType LOGICAL_TYPE_INT_8 "parquet::LogicalType::type::INT_8"
    cdef ParquetLogicalType LOGICAL_TYPE_INT_16 "parquet::LogicalType::type::INT_16"
    cdef ParquetLogicalType LOGICAL_TYPE_INT_32 "parquet::LogicalType::type::INT_32"
    cdef ParquetLogicalType LOGICAL_TYPE_INT_64 "parquet::LogicalType::type::INT_64"
    cdef ParquetLogicalType LOGICAL_TYPE_JSON "parquet::LogicalType::type::JSON"
    cdef ParquetLogicalType LOGICAL_TYPE_BSON "parquet::LogicalType::type::BSON"
    cdef ParquetLogicalType LOGICAL_TYPE_INTERVAL "parquet::LogicalType::type::INTERVAL"

    cdef cppclass ParquetPhysicalType "parquet::Type::type":
        pass

    cdef ParquetPhysicalType PHYSICAL_TYPE_BOOLEAN "parquet::Type::type::BOOLEAN"
    cdef ParquetPhysicalType PHYSICAL_TYPE_INT32 "parquet::Type::type::INT32"
    cdef ParquetPhysicalType PHYSICAL_TYPE_INT64 "parquet::Type::type::INT64"
    cdef ParquetPhysicalType PHYSICAL_TYPE_INT96 "parquet::Type::type::INT96"
    cdef ParquetPhysicalType PHYSICAL_TYPE_FLOAT "parquet::Type::type::FLOAT"
    cdef ParquetPhysicalType PHYSICAL_TYPE_DOUBLE "parquet::Type::type::DOUBLE"
    cdef ParquetPhysicalType PHYSICAL_TYPE_BYTE_ARRAY "parquet::Type::type::BYTE_ARRAY"
    cdef ParquetPhysicalType PHYSICAL_TYPE_FIXED_LEN_BYTE_ARRAY "parquet::Type::type::FIXED_LEN_BYTE_ARRAY"

    cdef cppclass ParquetNodeType "parquet::schema::Node::type":
        pass
        
    cdef ParquetNodeType NODE_TYPE_PRIMITIVE "parquet::schema::Node::type::PRIMITIVE"
    cdef ParquetNodeType NODE_TYPE_GROUP "parquet::schema::Node::type::GROUP"

    cdef cppclass Node:
        bool is_primitive()
        bool is_group()
        bool is_optional()
        bool is_repeated()
        bool is_required()
        const string& name() const
        ParquetNodeType node_type() const
        ParquetRepetition repetition() const
        ParquetLogicalType logical_type() const
        int id() const
        const Node* parent() const

    cdef cppclass ParquetPrimitiveNode "parquet::schema::PrimitiveNode" (Node):
        ParquetPhysicalType physical_type() const
        int32_t type_length() const
        const DecimalMetadata& decimal_metadata() const

    ctypedef shared_ptr[Node] NodePtr;
    ctypedef vector[NodePtr] NodeVector;

    cdef cppclass ParquetGroupNode "parquet::schema::GroupNode"(Node):
        NodePtr& field(int i) const
        int field_count() const

    cdef NodePtr make_primitive_node "parquet::schema::PrimitiveNode::Make"(const string& name,
                                                                  ParquetRepetition repetition,
                                                                  ParquetPhysicalType type,
                                                                  ParquetLogicalType logical_type,
                                                                  int length, int precision, int scale)

    cdef NodePtr make_group_node "parquet::schema::GroupNode::Make"(const string& name, ParquetRepetition repetition,
                                                                  const NodeVector& fields, ParquetLogicalType logical_type)