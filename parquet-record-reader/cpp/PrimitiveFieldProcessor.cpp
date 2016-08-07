//
// Created by roman on 7/31/16.
//

#include "../include/PrimitiveFieldProcessor.h"
namespace parquet{
    namespace python{
        PrimitiveFieldProcessor* PrimitiveFieldProcessor::create(unique_ptr<PrimitiveProcessorContext> context) {
            switch (context->column->physical_type()) {
                case parquet::Type::BOOLEAN:
                    return new BooleanFieldProcessor(move(context));
                case parquet::Type::BYTE_ARRAY:
                    return new ByteArrayFieldProcessor(move(context));
                case parquet::Type::DOUBLE:
                    return new DoubleFieldProcessor(move(context));
                case parquet::Type::FIXED_LEN_BYTE_ARRAY:
                    return new FLBAFieldProcessor(move(context));
                case parquet::Type::FLOAT:
                    return new FloatFieldProcessor(move(context));
                case parquet::Type::INT32:
                    return new Int32FieldProcessor(move(context));
                case parquet::Type::INT64:
                    return new Int64FieldProcessor(move(context));
                default:
                    ostringstream ss;
                    ss << "Unsupported type: " << context->column->physical_type();
                    throw new runtime_error(ss.str());
            }
        }
    }
}