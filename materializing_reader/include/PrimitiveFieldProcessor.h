//
// Created by roman on 7/30/16.
//

#ifndef PYTHON_PARQUET_COLUMNPROCESSOR_H
#define PYTHON_PARQUET_COLUMNPROCESSOR_H


#include "PrimitiveFieldConverter.h"
#include <string>
#include <sstream>
#include "Processor.h"

using namespace std;
namespace parquet {
    namespace python {

        class PrimitiveProcessorContext : public ProcessorContext {
        private:
            int leaf_index;
            shared_ptr<PrimitiveFieldConverter> column_converter;
            parquet::ColumnDescriptor* column;
            // TODO: check if can be made into shared_ptr
            LevelProcessor *level_processor;
        public:
            PrimitiveProcessorContext(int leaf_index,
                                   parquet::ColumnDescriptor* column,
                                   shared_ptr<PrimitiveFieldConverter> column_converter,
                                   LevelProcessor *level_processor)
                    : column_converter(column_converter), column(column), level_processor(level_processor),
                      leaf_index(leaf_index)
            { }

            friend class PrimitiveFieldProcessor;
        };

        class PrimitiveLevelProcessorContext : public LevelProcessorContext{
        protected:
            inline int16_t* def_level_addr(){ return &def_level; }
            inline int16_t* rep_level_addr(){ return &rep_level; }
            inline bool*    is_null_addr() {return &_is_null; }
        public:
            PrimitiveLevelProcessorContext(parquet::ColumnDescriptor *column)
                    : LevelProcessorContext(column){

            }

            friend class Int32FieldProcessor;
            friend class Int64FieldProcessor;
            friend class BooleanFieldProcessor;
            friend class FloatFieldProcessor;
            friend class DoubleFieldProcessor;
            friend class ByteArrayFieldProcessor;
            friend class FLBAFieldProcessor;
        };

        class PrimitiveFieldProcessor : public Processor {
        private:
            PrimitiveFieldProcessor(PrimitiveProcessorContext* context)
                    : Processor(unique_ptr<PrimitiveProcessorContext>(context)), scanner(nullptr),
                      prior_level(PrimitiveLevelProcessorContext(context->column)) {
            }
        protected:
            shared_ptr<parquet::Scanner> scanner;
            PrimitiveLevelProcessorContext prior_level;
            bool use_prior_data;

            PrimitiveFieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                : PrimitiveFieldProcessor(context.release()) {
            }

            inline PrimitiveProcessorContext* get_context(){
                return static_cast<PrimitiveProcessorContext*>(Processor::get_context());
            }

            bool process_levels(LevelProcessorContext *level_context) {
                return get_context()->level_processor->process_levels(level_context);
            }

            inline parquet::ColumnDescriptor *get_column() {
                return get_context()->column;
            }

            inline PrimitiveFieldConverter *get_converter() {
                return get_context()->column_converter.get();
            }
        public:
            virtual void start_row_group(shared_ptr<parquet::RowGroupReader> row_group_reader) {
                scanner = parquet::Scanner::Make(row_group_reader->Column(get_context()->leaf_index));
            }

            virtual void end_row_group(){
                scanner = nullptr;
            }

            virtual bool has_value(){
                if (use_prior_data) return true;

                return scanner->HasNext();
            }

            static PrimitiveFieldProcessor *create(unique_ptr<PrimitiveProcessorContext> context);
        };

        class FLBAFieldProcessor : public PrimitiveFieldProcessor {
        private:
            parquet::FixedLenByteArray *prior_val;
        public:
            FLBAFieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::FixedLenByteArrayScanner> scanner
                        = static_pointer_cast<parquet::FixedLenByteArrayScanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_binary(prior_level.is_null()? nullptr : prior_val->ptr,
                                                get_column()->type_length(),
                                                prior_level.is_null());
                }
            }
        };

        class ByteArrayFieldProcessor : public PrimitiveFieldProcessor {
        private:
            parquet::ByteArray *prior_val;
        public:
            ByteArrayFieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::ByteArrayScanner> scanner
                        = static_pointer_cast<parquet::ByteArrayScanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_binary(prior_level.is_null() ? nullptr : prior_val->ptr,
                                                prior_level.is_null() ? 0 : prior_val->len,
                                                prior_level.is_null());
                }
            }
        };

        class DoubleFieldProcessor : public PrimitiveFieldProcessor {
        private:
            double *prior_val;
        public:
            DoubleFieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::DoubleScanner> scanner
                        = static_pointer_cast<parquet::DoubleScanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_double(*prior_val, prior_level.is_null());
                }
            }
        };

        class FloatFieldProcessor : public PrimitiveFieldProcessor {
        private:
            float *prior_val;
        public:
            FloatFieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::FloatScanner> scanner
                        = static_pointer_cast<parquet::FloatScanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_float(*prior_val, prior_level.is_null());
                }
            }
        };

        class Int64FieldProcessor : public PrimitiveFieldProcessor {
        private:
            int64_t *prior_val;
        public:
            Int64FieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::Int64Scanner> scanner
                        = static_pointer_cast<parquet::Int64Scanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_long(*prior_val, prior_level.is_null());
                }
            }
        };

        class Int32FieldProcessor : public PrimitiveFieldProcessor {
        private:
            int *prior_val;
        public:
            Int32FieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::Int32Scanner> scanner
                        = static_pointer_cast<parquet::Int32Scanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_int(*prior_val, prior_level.is_null());
                }
            }
        };

        class BooleanFieldProcessor : public PrimitiveFieldProcessor {
        private:
            bool *prior_val;
        public:
            BooleanFieldProcessor(unique_ptr<PrimitiveProcessorContext> context)
                    : PrimitiveFieldProcessor(move(context)) {
            }

            virtual void advance_next() {
                shared_ptr<parquet::BoolScanner> scanner
                        = static_pointer_cast<parquet::BoolScanner>(PrimitiveFieldProcessor::scanner);

                if (!use_prior_data)
                    scanner->Next(prior_val, prior_level.def_level_addr(), prior_level.rep_level_addr(), prior_level.is_null_addr());

                use_prior_data = !process_levels(&prior_level);
                if (!use_prior_data) {
                    get_converter()->add_boolean(*prior_val, prior_level.is_null());
                }
            }
        };
    }
}

#endif //PYTHON_PARQUET_COLUMNPROCESSOR_H
