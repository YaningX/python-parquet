//
// Created by roman on 7/30/16.
//

#include "../include/MaterializingFileReader.h"
#include "cstdint"

#include "../include/RecordConverter.h"
using namespace std;

namespace parquet {

    namespace python {
        template <class T>
        MaterializingParquetFileReader<T>::MaterializingParquetFileReader(unique_ptr<parquet::RandomAccessSource> source) :
                file_reader(new parquet::ParquetFileReader()),
                record_processor(nullptr)
        {
            file_reader->Open(move(source));

            current_row_group = 0;
            max_row_group = num_row_groups();

        }

        template <class T>
        void MaterializingParquetFileReader<T>::set_root_conveter(shared_ptr<TypedGroupConverter<T>> root_converter){
            if( this->record_processor.get() != nullptr)
                throw new runtime_error("Root converter can only be set once!");

            const SchemaDescriptor* descr = file_reader->descr();
            shared_ptr<map<string, int>> column_map = shared_ptr<map<string,int>>(new map<string,int>());
            for(int i =0; i<descr->num_columns(); i++)
            {
                column_map->insert(make_pair(descr->Column(i)->path()->ToDotString(), i));
            }

            this->record_processor = new RecordReader(move(unique_ptr<GroupReaderContext>(new GroupReaderContext(
                    descr, static_pointer_cast<parquet::schema::GroupNode>(descr->schema()),
                    static_pointer_cast<GroupConverter>(root_converter),column_map,""
            ))));
        }

        template <class T>
        T* MaterializingParquetFileReader<T>::read_next() {
            if (!row_group_initialized)
                this->record_processor->start_row_group(this->file_reader->RowGroup(current_row_group));
            T* result = static_cast<T*>(this->record_processor->read_next());
            while(result == nullptr ) {
                this->record_processor->end_row_group();
                current_row_group ++;
                if(current_row_group == max_row_group)
                    return nullptr;
                this->record_processor->start_row_group(this->file_reader->RowGroup(current_row_group));
                result = static_cast<T*>(this->record_processor->read_next());
            }
            return result;
        }


    }
}