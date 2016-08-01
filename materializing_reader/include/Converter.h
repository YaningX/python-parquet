//
// Created by roman on 7/31/16.
//

#ifndef PYTHON_PARQUET_CONVERTER_H
#define PYTHON_PARQUET_CONVERTER_H
namespace parquet
{
    namespace python{
        class FieldConverter {
        public:
            virtual bool is_primitive() = 0;
        };
    }
}
#endif //PYTHON_PARQUET_CONVERTER_H
