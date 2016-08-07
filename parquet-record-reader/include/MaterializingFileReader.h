//
// Created by roman on 7/30/16.
//

#ifndef PYTHON_PARQUET_READER_H
#define PYTHON_PARQUET_READER_H

#include "parquet/api/reader.h"
#include "parquet/util/input.h"
#include "RecordConverter.h"
#include "RecordReader.h"

using namespace std;

namespace parquet {
    namespace python {
        template <typename T> class MaterializingParquetFileReader {
        private:
            unique_ptr<parquet::ParquetFileReader> file_reader;
            unique_ptr<RecordReader>            record_processor;
            shared_ptr<parquet::RowGroupReader> row_group_reader;

            int current_row_group;
            int max_row_group;
            bool row_group_initialized;
        public:
            MaterializingParquetFileReader(unique_ptr<parquet::RandomAccessSource> source);

            void set_root_conveter(shared_ptr<TypedGroupConverter<T>> root_converter);


            int limit_read(int start_row_group, int num_row_groups){

                if (start_row_group >= num_row_groups)
                    throw new runtime_error("start_row_group exceeds number of row groups in the file");
                current_row_group = start_row_group;
                num_row_groups = current_row_group + num_row_groups > this->num_row_groups() ?
                                 this->num_row_groups() - current_row_group : num_row_groups;

                max_row_group = current_row_group + num_row_groups;
                row_group_reader = NULL;
            }

            int num_rows() {
                return file_reader->num_rows();
            }

            int num_row_groups() {
                return file_reader->num_row_groups();
            }

            const parquet::schema::NodePtr& schema_root() {
                return file_reader->descr()->schema();
            }

            T* read_next();

            void close() {
                file_reader->Close();
            }
        };
    }
}
#endif //PYTHON_PARQUET_READER_H
