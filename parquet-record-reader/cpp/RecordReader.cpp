//
// Created by roman on 7/31/16.
//

#include "../include/RecordReader.h"

namespace parquet {
    namespace python {

        void GroupReader::init_fields() {
            field_count = get_context()->group->field_count();

            processors = vector<unique_ptr<Processor>>((size_t) field_count);
            converters = vector<shared_ptr<FieldConverter>>((size_t) field_count);
            for (int i = 0; i < field_count; i++) {
                schema::NodePtr field_schema = get_context()->group->field(i);

                converters[i] = get_context()->group_converter->get_converter(i);
                string field_path = get_context()->node_path + "." + field_schema->name();

                if (get_context()->group->field(i)->is_primitive()) {
                    int column_index = get_context()->source_index_map->at(field_path);
                    ColumnDescriptor *column_descriptor
                            = const_cast<ColumnDescriptor *>(get_context()->schema_descriptor->Column(column_index));
                    PrimitiveProcessorContext *column_context = new PrimitiveProcessorContext(
                            column_index, column_descriptor,
                            static_pointer_cast<PrimitiveFieldConverter>(converters[i]),
                            this);
                    processors[i] = move(unique_ptr<Processor>((Processor*)PrimitiveFieldProcessor::create(unique_ptr<PrimitiveProcessorContext>(column_context))));
                    if (has_next_delegate == -1)
                        has_next_delegate = i;
                } else {
                    processors[i] = move(unique_ptr<Processor>((Processor*)new GroupReader(this,
                                                    move(unique_ptr<GroupReaderContext>(new GroupReaderContext(
                                                            get_context()->schema_descriptor,
                                                            static_pointer_cast<schema::GroupNode>(field_schema),
                                                            static_pointer_cast<GroupConverter>(converters[i]),
                                                            get_context()->source_index_map, field_path
                                                    ))))));
                }
            }

            if (has_next_delegate < 0)
                has_next_delegate = 0;
        }

        inline bool GroupReader::process_parent_and_nullity(LevelProcessorContext *level_context) {
            if( parent->process_levels(level_context)) {
                if (!level_context->is_null() || level_data.def_level > level_context->get_def_level())
                    start_record();
                return true;
            }
            return false;
        }

        inline bool GroupReader::process_levels(LevelProcessorContext *level_context) {
            if (level_context->get_rep_level() == 0) {
                return process_parent_and_nullity(level_context);
            }
            else if (level_context->get_rep_level() > level_data.rep_level) {
                return true; // there is some repetition below our node, we can safely allow to read.
            } else if (level_context->get_rep_level() < level_data.rep_level)
            {
                // there is repetition above us, will proceed creating new record if parent OK's
                return process_parent_and_nullity(level_context);
            }
            else {
                // same repetition level as the current node
                if (level_data.rep_level == parent->level_data.rep_level) {
                    // if node is not repetitive, i.e. same rep level as parent, then ask parent what to do
                    return process_parent_and_nullity(level_context);
                } else {
                    // ok, we're a node at which things are being repeated
                    if(!read_column_once) {
                        process_more_records = true;
                        return false;
                    } else {
                        start_record();
                    }
                }
            }
        }

        inline void GroupReader::advance_next() {


            // if we are a non-repetitive group,
            // we are only going to read each column once.

            // if we are a repetitive group,
            // then each time we encounter repetition at our level in some column value,
            // we are going to set this flag.
            do {
                process_more_records = false;
                read_single_group();

            } while(process_more_records);
        }

        void GroupReader::read_single_group() {
            record_started = false;

            for (int i = 0; i < processors.size(); i++) {
                    read_column_once = false;
                    advancing_column = i;
                    processors[i]->advance_next();
                }
            if (record_started)
                    get_context()->group_converter->end();
        }

        inline bool RecordReader::process_levels(LevelProcessorContext *level_context) {
            return !read_column_once;
        }

        inline void RecordReader::advance_next() {
            this->read_single_group();
        }

        inline void* RecordReader::read_next() {
            if(has_value()) {
                this->advance_next();
                return GroupReader::get_context()->get_group_converter()->get_current_record_untyped();
            }

            return nullptr;
        }
    }
}