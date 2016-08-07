//
// Created by roman on 7/30/16.
//

#ifndef PYTHON_PARQUET_RECORDREADER_H
#define PYTHON_PARQUET_RECORDREADER_H

#include "PrimitiveFieldConverter.h"
#include "Processor.h"
#include "parquet/schema/types.h"
#include "parquet/schema/descriptor.h"
#include "PrimitiveFieldProcessor.h"
#include "RecordConverter.h"

#include <string>
#include <map>

using namespace std;
namespace parquet {
    namespace python {

        class GroupReaderContext : ProcessorContext {
        private:
            shared_ptr<GroupConverter> group_converter;
            shared_ptr<parquet::schema::GroupNode> group;
            string node_path;
            shared_ptr<map<string, int>> source_index_map;
            const parquet::SchemaDescriptor *schema_descriptor;

        public:
            GroupReaderContext(const parquet::SchemaDescriptor *schema_descriptor,
                               shared_ptr<parquet::schema::GroupNode> group,
                               shared_ptr<GroupConverter> group_converter,
                               shared_ptr<map<string, int>> source_index_map,
                               string node_path
            ) : group_converter(group_converter), group(group), schema_descriptor(schema_descriptor),
                node_path(node_path), source_index_map(source_index_map) {


            }

            const GroupConverter* get_group_converter(){
                return group_converter.get();
            }
            friend class GroupReader;

        };

        class GroupReader : public LevelProcessor, public Processor {
        private:
            bool process_more_records;
            int field_count;
            int has_next_delegate;
            vector<unique_ptr<Processor>> processors;
            vector<shared_ptr<FieldConverter>> converters;
            int advancing_column;

            void init_fields();

            inline void start_record() {
                if (!record_started) {
                    record_started = true;
                    get_context()->group_converter->start();
                }
            }
            bool process_parent_and_nullity(LevelProcessorContext *level_context);

        protected:
            struct LevelData {
                int rep_level;
                int def_level;

                LevelData() : rep_level(0), def_level(0) { }
            };

            LevelData level_data;
            GroupReader *parent;
            bool read_column_once;
            bool record_started;

            GroupReader(GroupReader *parent, unique_ptr<GroupReaderContext> context)
                    : Processor(unique_ptr<ProcessorContext>(context.release())),
                      level_data(), parent(parent), record_started(false),
                      has_next_delegate(-1), advancing_column(0), process_more_records(false),
                        read_column_once(false){
                level_data.def_level = this->parent->level_data.def_level;
                level_data.rep_level = this->parent->level_data.rep_level;

                if (!get_context()->group->is_required())
                    level_data.def_level++;
                if (get_context()->group->is_repeated())
                    level_data.rep_level++;

                init_fields();

            }


            inline virtual GroupReaderContext *get_context() {
                return static_cast<GroupReaderContext *>(Processor::get_context());
            }
            void read_single_group();

        public:
            virtual bool process_levels(LevelProcessorContext *level_context);
            inline virtual void advance_next();

            inline virtual bool has_value() {
                return processors[has_next_delegate]->has_value();
            }


            inline virtual void start_row_group(shared_ptr<parquet::RowGroupReader> row_group_reader) {
                for (int i = 0; i < processors.size(); i++)
                    processors[i]->start_row_group(row_group_reader);
            }

            inline virtual void end_row_group() {
                for (int i = 0; i < processors.size(); i++)
                    processors[i]->end_row_group();
            }


        };

        class RecordReader : public GroupReader {
        private:

        public:
            RecordReader(unique_ptr<GroupReaderContext> context)
            :  GroupReader(this, move(context)) {

            }


            virtual bool process_levels(LevelProcessorContext *level_context);
            inline virtual void advance_next();
            void*  read_next();
        };
    }
}
#endif //PYTHON_PARQUET_RECORDREADER_H
