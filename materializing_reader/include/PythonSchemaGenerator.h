//
// Created by roman on 7/31/16.
//

#ifndef PYTHON_PARQUET_PYTHONSCHEMAGENERATOR_H
#define PYTHON_PARQUET_PYTHONSCHEMAGENERATOR_H
#include <string>
#include <vector>
#include <memory>
#include "Delegates.h"
#include "parquet/types.h"

using namespace std;
namespace parquet{
    namespace python{
        namespace schema {
            struct Field {
                string logical_type;
                string name;
                bool is_primitive;
                bool is_optional;
                bool is_repeated;
                bool is_required;
            };
            struct PrimitiveField : public Field {
                string type;
                int length;
                int precision;
                int scale;
            };

            struct GroupField : public  Field {
                shared_ptr<vector<shared_ptr<Field>>> fields;
            };

            class SchemaConverter{
            private:
                string get_logical_type_name(LogicalType::type node_type);
                shared_ptr<Field> convert_group(const parquet::schema::GroupNode* group_node);

                string get_physical_type_name(Type::type node_type);
                shared_ptr<Field> convert_primitive(const parquet::schema::PrimitiveNode* node);


            public:
                shared_ptr<Field> convert(shared_ptr<parquet::python::delegate::DelegateFileReader> reader){
                    auto schema_root = reader->schema_root();
                    return convert_group(schema_root);
                }
            };
        }
    }
}
#endif //PYTHON_PARQUET_PYTHONSCHEMAGENERATOR_H
