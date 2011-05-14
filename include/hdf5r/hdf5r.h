/* HDF5R
 *
 * Copyright (C) 2011
 *     Geoffrey Biggs and contributors
 *     RT-Synthesis Research Group
 *     Intelligent Systems Research Institute,
 *     National Institute of Advanced Industrial Science and Technology (AIST),
 *     Japan
 *     All rights reserved.
 * Licensed under the Eclipse Public License -v 1.0 (EPL)
 * http://www.opensource.org/licenses/eclipse-1.0.txt
 *
 * Robotics-oriented interface to HDF5 files.
 */


#if !defined(HDF5R_H__)
#define HDF5R_H__


#include <hdf5.h>
#include <map>
#include <string>
#include <vector>


namespace hdf5r
{
    class ChannelInfo
    {
        public:
            ChannelInfo();
            ChannelInfo(std::string name, std::string type_name,
                    std::string source_name, hid_t mem_type, size_t size,
                    uint64_t start_time, uint64_t end_time);
            ChannelInfo(ChannelInfo const& rhs);
            ~ChannelInfo();

            ChannelInfo& operator=(ChannelInfo const& rhs);

            void name(std::string name) { name_ = name; }
            std::string name() const { return name_; }
            void type_name(std::string type_name) { type_name_ = type_name; }
            std::string type_name() const { return type_name_; }
            void source_name(std::string source_name)
                { source_name_ = source_name; }
            std::string source_name() const { return source_name_; }
            void mem_type(hid_t mem_type);
            hid_t const mem_type() const { return mem_type_; }
            void size(size_t size) { size_ = size; }
            size_t size() const { return size_; }
            void start_time(uint64_t start_time) { start_time_ = start_time; }
            uint64_t start_time() const { return start_time_; }
            void end_time(uint64_t end_time) { end_time_ = end_time; }
            uint64_t end_time() const { return end_time_; }

        private:
            std::string name_;
            std::string type_name_;
            std::string source_name_;
            hid_t mem_type_;
            size_t size_;
            uint64_t start_time_, end_time_;
    };


    class Channel
    {
        public:
            Channel(std::string name="", hid_t group=-1, hid_t rec_space=-1,
                    hid_t rec_set=-1, hid_t ts_space=-1, hid_t ts_set=-1,
                    hid_t mem_type=-1, size_t size=0);
            Channel(Channel const& rhs);
            ~Channel();

            std::string name() const { return name_; }
            hid_t group() const { return group_; }
            hid_t rec_space() const { return rec_space_; }
            hid_t rec_set() const { return rec_set_; }
            hid_t ts_space() const { return ts_space_; }
            hid_t ts_set() const { return ts_set_; }
            hid_t mem_type() const { return mem_type_; }
            void size(size_t size) { size_ = size; }
            size_t size() const { return size_; }

        private:
            std::string name_;
            hid_t group_;
            hid_t rec_space_;
            hid_t rec_set_;
            hid_t ts_space_;
            hid_t ts_set_;
            hid_t mem_type_;
            size_t size_; // Current number of records
    };


    typedef uint64_t ChannelID;
    typedef enum { RDONLY, RDWR, NEW, TRUNCATE } Mode;
    typedef enum { STRING_TAG, BINARY_TAG} TagType;
    typedef std::pair<ChannelID, uint64_t> IndexPointer;
    typedef std::vector<IndexPointer> IndexPointerList;
    typedef std::map<uint64_t, IndexPointerList> Index;


    class HDF5R
    {
        public:
            HDF5R(std::string filename, Mode mode);
            HDF5R(HDF5R const& rhs);
            virtual ~HDF5R();

            Mode mode() const { return mode_; }

            ChannelID add_channel(std::string name, std::string type_name,
                    std::string source_name, hid_t mem_type, hid_t file_type);
            std::vector<ChannelID> channels() const;
            ChannelInfo get_channel_info(ChannelID chan_id);
            bool have_channel(std::string name) const;

            void add_entry(ChannelID chan_id, uint64_t timestamp,
                    void const* const buf);
            size_t get_entry_size(ChannelID chan_id, hsize_t index);
            uint64_t get_entry(ChannelID chan_id, hsize_t index,
                    void* const buf);

            Index index() const { return index_; }

            std::string get_text_tag(std::string tag);
            size_t get_binary_tag(std::string tag, void* const buf);
            std::map<std::string, TagType> get_tags() const;
            void set_text_tag(std::string tag, std::string value);
            void set_binary_tag(std::string tag, void const* const buf,
                    size_t size);

        private:
            std::string fn_;
            Mode mode_;
            hid_t file_;
            hid_t channels_grp_;
            hid_t tags_grp_;

            std::map<ChannelID, Channel> channels_;
            ChannelID next_id_;

            void prepare();
            void prepare_tags_group();
            ChannelInfo read_channel_info(Channel const& chan) const;
            std::string read_string(hid_t group, std::string set) const;
            hid_t read_type(hid_t group, std::string set) const;
            unsigned int read_uint(hid_t group, std::string set) const;
            void write_string(hid_t group, std::string set, std::string str);
            void write_type(hid_t group, std::string set, hid_t type);
            void write_uint(hid_t group, std::string set, unsigned int value);

            Index index_;

            hid_t make_index_ftype() const;
            hid_t make_index_mtype() const;
            void read_index();
            void write_index();

            struct close_group_fun
            {
                void operator()(std::pair<const ChannelID, Channel>& chan)
                {
                    H5Dclose(chan.second.rec_set());
                    H5Sclose(chan.second.rec_space());
                    H5Dclose(chan.second.ts_set());
                    H5Sclose(chan.second.ts_space());
                    H5Gclose(chan.second.group());
                }
            };

            static char const* const CHANNELS_GROUP;
            static char const* const TAGS_GROUP;
            static char const* const INDEX_SET;
            static char const* const RECORDS_SET;
            static char const* const TIMESTAMPS_SET;
    };
};

#endif // !defined(HDF5R_H__)

