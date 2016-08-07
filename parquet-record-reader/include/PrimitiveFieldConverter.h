//
// Created by roman on 7/30/16.
//

#ifndef PYTHON_PARQUET_COLUMNCONVERTER_H
#define PYTHON_PARQUET_COLUMNCONVERTER_H

#include <memory>
#include <cstdint>
#include "Converter.h"

namespace parquet {
    namespace python {

        class PrimitiveFieldConverter : public FieldConverter {
        public:
            virtual bool is_primitive() {
                return true;
            }

            virtual void add_int(int value, bool is_null) {

            }

            virtual void add_long(long value, bool is_null) {

            }

            virtual void add_binary(const uint8_t *value, int length, bool is_null) {

            }

            virtual void add_float(float value, bool is_null){

            }

            virtual void add_double(double value, bool is_null) {

            }

            virtual void add_boolean(bool value, bool is_null){

            }
        };
    }
}
#endif //PYTHON_PARQUET_COLUMNCONVERTER_H
