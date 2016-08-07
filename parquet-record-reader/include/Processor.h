//
// Created by roman on 7/31/16.
//

#ifndef PYTHON_PARQUET_PROCESSOR_H
#define PYTHON_PARQUET_PROCESSOR_H
#include <parquet/api/reader.h>
#include "parquet/column/scanner.h"

using namespace std;
namespace parquet{
    namespace python{
        class ProcessorContext {

        };

        class Processor {
        private:
            unique_ptr<ProcessorContext> context;
        protected:
            Processor(ProcessorContext *context)
                    : context(context) {

            }

            Processor(unique_ptr<ProcessorContext> context)
                    : Processor(context.release()) {

            }

            inline ProcessorContext* get_context(){
                return context.get();
            }
        public:

            virtual bool has_value() = 0;
            virtual void advance_next() = 0;
            virtual void start_row_group(shared_ptr<parquet::RowGroupReader> row_group_reader) = 0;
            virtual void end_row_group() = 0;
        };

        class LevelProcessorContext {
        protected:
            parquet::ColumnDescriptor *column;
            int16_t def_level;
            int16_t rep_level;
            bool _is_null;

        public:
            LevelProcessorContext(parquet::ColumnDescriptor *column)
                : column(column), def_level(0), rep_level(0), _is_null(false){

            }

            inline int16_t get_def_level() const { return def_level; }

            inline int16_t get_rep_level() const { return rep_level; }

            inline bool is_null() const { return _is_null; }

            inline parquet::ColumnDescriptor *get_column() const { return column; }


        };

        class LevelProcessor {
        protected:
            LevelProcessor() { }

        public:
            virtual bool process_levels(LevelProcessorContext *context) = 0;

        };
    }
}
#endif //PYTHON_PARQUET_PROCESSOR_H
