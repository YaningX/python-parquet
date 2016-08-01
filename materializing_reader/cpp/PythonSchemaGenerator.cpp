//
// Created by roman on 7/31/16.
//

#include "../include/PythonSchemaGenerator.h"

namespace parquet {
    namespace python {
        namespace schema {
            string SchemaConverter::get_logical_type_name(LogicalType::type node_type){
                switch(node_type){
                    case LogicalType::type::NONE: return "NONE";
                    case LogicalType::type::UTF8: return "UTF8";
                    case LogicalType::type::MAP: return "MAP";
                    case LogicalType::type::MAP_KEY_VALUE: return "MAP_KEY_VALUE";
                    case LogicalType::type::LIST: return "LIST";
                    case LogicalType::type::ENUM: return "ENUM";
                    case LogicalType::type::DECIMAL: return "DECIMAL";
                    case LogicalType::type::DATE: return "DATE";
                    case LogicalType::type::TIME_MILLIS: return "TIME_MILLIS";
                    case LogicalType::type::TIMESTAMP_MILLIS: return "TIMESTAMP_MILLIS";
                    case LogicalType::type::UINT_8: return "UINT_8";
                    case LogicalType::type::UINT_16: return "UINT_16";
                    case LogicalType::type::UINT_32: return "UINT_32";
                    case LogicalType::type::UINT_64: return "UINT_64";
                    case LogicalType::type::INT_8: return "INT_8";
                    case LogicalType::type::INT_16: return "INT_16";
                    case LogicalType::type::INT_32: return "INT_32";
                    case LogicalType::type::INT_64: return "INT_64";
                    case LogicalType::type::JSON: return "JSON";
                    case LogicalType::type::BSON: return "BSON";
                    case LogicalType::type::INTERVAL: return "INTERVAL";
                }
            }

            shared_ptr<Field> SchemaConverter::convert_group(const parquet::schema::GroupNode* group_node){
                GroupField* group = new GroupField();
                group->name = group_node->name();
                group->is_primitive = false;
                group->logical_type = get_logical_type_name(group_node->logical_type());
                group->is_optional = group_node->is_optional();
                group->is_repeated = group_node->is_repeated();
                group->is_required = group_node->is_required();
                group->fields = shared_ptr<vector<shared_ptr<Field>>>(new vector<shared_ptr<Field>>());
                for(int i =0; i<group_node->field_count(); i++ )
                {
                    if(group_node->field(i)->is_primitive())
                        group->fields->push_back(move(convert_primitive((const parquet::schema::PrimitiveNode*)group_node->field(i).get())));
                    else
                        group->fields->push_back(move(convert_group((const parquet::schema::GroupNode*)group_node->field(i).get())));
                }

                return shared_ptr<Field>((Field*)group);
            }

            string SchemaConverter::get_physical_type_name(Type::type node_type){
                switch(node_type) {
                    case Type::type::BOOLEAN: return "BOOLEAN";
                    case Type::type::INT32: return "INT32";
                    case Type::type::INT64: return "INT64";
                    case Type::type::INT96: return "INT96";
                    case Type::type::FLOAT: return "FLOAT";
                    case Type::type::DOUBLE: return "DOUBLE";
                    case Type::type::BYTE_ARRAY: return "BYTE_ARRAY";
                    case Type::type::FIXED_LEN_BYTE_ARRAY: return "FIXED_LEN_BYTE_ARRAY";
                }
            }

            shared_ptr<Field> SchemaConverter::convert_primitive(const parquet::schema::PrimitiveNode* node){
                PrimitiveField *field = new PrimitiveField();
                field->name = node->name();
                field->is_primitive = true;
                field->logical_type = get_logical_type_name(node->logical_type());
                field->is_optional = node->is_optional();
                field->is_repeated = node->is_repeated();
                field->is_required = node->is_required();
                field->type = get_physical_type_name(node->physical_type());
                field->length = node->type_length();
                field->scale= node->decimal_metadata().scale;
                field->precision=node->decimal_metadata().precision;

                return shared_ptr<Field>((Field*)field);
            }

        }
    }
}