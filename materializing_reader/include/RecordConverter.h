//
// Created by roman on 7/31/16.
//

#ifndef PYTHON_PARQUET_RECORDCONVERTER_H
#define PYTHON_PARQUET_RECORDCONVERTER_H
#include "Converter.h"
#include <bits/shared_ptr.h>
using namespace std;
namespace parquet{
    namespace python{
        class GroupConverter : public FieldConverter{
        public:
            virtual bool is_primitive() { return false; }
            virtual void* get_current_record_untyped() const = 0;
            virtual void start() = 0;
            virtual void end() = 0;
            virtual shared_ptr<FieldConverter> get_converter(int i) = 0;
        };

        template<typename T> class TypedGroupConverter : public GroupConverter
        {
            virtual T* get_current_record(){
                return static_cast<T*>(GroupConverter::get_current_record_untyped());
            }
        };
    }
}
#endif //PYTHON_PARQUET_RECORDCONVERTER_H
