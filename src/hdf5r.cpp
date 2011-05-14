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

#include <hdf5r/hdf5r.h>

#include <algorithm>
#include <stdexcept>
#include <vector>

using namespace hdf5r;


///////////////////////////////////////////////////////////////////////////////
// ChannelInfo class
///////////////////////////////////////////////////////////////////////////////


ChannelInfo::ChannelInfo()
    : mem_type_(-1)
{
}


ChannelInfo::ChannelInfo(std::string name, std::string type_name,
        std::string source_name, hid_t mem_type, size_t size,
        uint64_t start_time, uint64_t end_time)
    : name_(name), type_name_(type_name),
    source_name_(source_name), mem_type_(-1), size_(size),
    start_time_(start_time), end_time_(end_time)
{
    if (mem_type >= 0)
    {
        mem_type_ = H5Tcopy(mem_type);
    }
}

ChannelInfo::ChannelInfo(ChannelInfo const& rhs)
    : name_(rhs.name_), type_name_(rhs.type_name_),
    source_name_(rhs.source_name_), mem_type_(-1),
    size_(rhs.size_), start_time_(rhs.start_time_), end_time_(rhs.end_time_)
{
    if (rhs.mem_type_ >= 0)
    {
        mem_type_ = H5Tcopy(rhs.mem_type_);
    }
}

ChannelInfo::~ChannelInfo()
{
    if (mem_type_ >= 0)
    {
        H5Tclose(mem_type_);
    }
}


ChannelInfo& ChannelInfo::operator=(ChannelInfo const& rhs)
{
    name_ = rhs.name_;
    type_name_ = rhs.type_name_;
    source_name_ = rhs.source_name_;
    if (mem_type_ >= 0)
    {
        H5Tclose(mem_type_);
    }
    if (rhs.mem_type_ >= 0)
    {
        mem_type_ = H5Tcopy(rhs.mem_type_);
    }
    else
    {
        mem_type_ = -1;
    }
    size_ = rhs.size_;
    start_time_ = rhs.start_time_;
    end_time_ = rhs.end_time_;
}


void ChannelInfo::mem_type(hid_t mem_type)
{
    if (mem_type_ >= 0)
    {
        H5Tclose(mem_type_);
    }
    if (mem_type >= 0)
    {
        mem_type_ = H5Tcopy(mem_type);
    }
}


///////////////////////////////////////////////////////////////////////////////
// Channel class
///////////////////////////////////////////////////////////////////////////////


Channel::Channel(std::string name, hid_t group, hid_t rec_space, hid_t rec_set,
        hid_t ts_space, hid_t ts_set, hid_t mem_type, size_t size)
    : name_(name), group_(group), rec_space_(rec_space), rec_set_(rec_set),
    ts_space_(ts_space), ts_set_(ts_set), mem_type_(mem_type), size_(size)
{
}


Channel::Channel(Channel const& rhs)
    : name_(rhs.name_), group_(rhs.group_), rec_space_(rhs.rec_space_),
    rec_set_(rhs.rec_set_), ts_space_(rhs.ts_space_), ts_set_(rhs.ts_set_),
    mem_type_(rhs.mem_type_), size_(rhs.size_)
{
}


Channel::~Channel()
{
}


///////////////////////////////////////////////////////////////////////////////
// HDF5R class
///////////////////////////////////////////////////////////////////////////////


herr_t get_child_names(hid_t root, char const* const name,
        H5L_info_t const* const info, void* op_data)
{
    std::vector<std::string>* dest =
        reinterpret_cast<std::vector<std::string>*>(op_data);
    dest->push_back(name);
    return 0;
}


HDF5R::HDF5R(std::string filename, Mode mode)
    : fn_(filename), mode_(mode), file_(-1), channels_grp_(-1), tags_grp_(-1),
    next_id_(0)
{
    switch(mode_)
    {
        case RDONLY:
            file_ = H5Fopen(fn_.c_str(), H5F_ACC_RDONLY, H5P_DEFAULT);
            if (file_ < 0)
            {
                throw std::runtime_error("File not found");
            }
            break;
        case RDWR:
            // Attempt to open the file, if it doesn't exist, fail
            file_ = H5Fopen(fn_.c_str(), H5F_ACC_RDWR, H5P_DEFAULT);
            if (file_ < 0)
            {
                throw std::runtime_error("File not found");
            }
            break;
        case NEW:
            // Make a new file unless there is one already there
            file_ = H5Fcreate(fn_.c_str(), 0, H5P_DEFAULT,
                    H5P_DEFAULT);
            if (file_ < 0)
            {
                throw std::runtime_error("Could not create new file");
            }
            break;
        case TRUNCATE:
            // Make a new file, overwriting anything already there
            file_ = H5Fcreate(fn_.c_str(), H5F_ACC_TRUNC, H5P_DEFAULT,
                    H5P_DEFAULT);
            if (file_ < 0)
            {
                throw std::runtime_error("Could not create new file");
            }
            break;
    }
    prepare();
}


HDF5R::HDF5R(HDF5R const& rhs)
    : mode_(rhs.mode_), file_(rhs.file_), channels_grp_(rhs.channels_grp_),
    tags_grp_(rhs.tags_grp_), next_id_(rhs.next_id_)
{
}


HDF5R::~HDF5R()
{
    write_index();
    std::for_each(channels_.begin(), channels_.end(), close_group_fun());
    if (tags_grp_ >= 0)
    {
        H5Gclose(tags_grp_);
    }
    if (channels_grp_ >= 0)
    {
        H5Gclose(channels_grp_);
    }
    if (file_ >= 0)
    {
        H5Fclose(file_);
    }
}


ChannelID HDF5R::add_channel(std::string name, std::string type_name,
        std::string source_name, hid_t mem_type, hid_t file_type)
{
    // Check the channel doesn't already exist
    if (have_channel(name))
    {
        throw std::runtime_error("Channel already exists");
    }

    ChannelID id(next_id_++);
    // Create a group for the channel
    hid_t group = H5Gcreate(channels_grp_, name.c_str(), H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
    // Populate it with the channel's properties
    write_uint(group, "uid", id);
    write_string(group, "name", name);
    write_string(group, "type_name", type_name);
    write_string(group, "source_name", source_name);
    write_type(group, "mem_type", mem_type);
    // Create a dataset for the entries and a parallel dataset for the time
    // stamps
    hsize_t dims[1] = {10};
    hsize_t max_dims[1] = {H5S_UNLIMITED};
    // Ensure chunking is enabled so we can grow the record datasets
    hsize_t chunk_size = {1};
    hid_t parms = H5Pcreate(H5P_DATASET_CREATE);
    H5Pset_chunk(parms, 1, &chunk_size);
    hid_t rec_space = H5Screate_simple(1, dims, max_dims);
    hid_t rec_set = H5Dcreate(group, RECORDS_SET, file_type, rec_space,
            H5P_DEFAULT, parms, H5P_DEFAULT);
    hid_t ts_space = H5Screate_simple(1, dims, max_dims);
    hid_t ts_set = H5Dcreate(group, TIMESTAMPS_SET, H5T_STD_U64LE, ts_space,
            H5P_DEFAULT, parms, H5P_DEFAULT);

    channels_[id] = Channel(name, group, rec_space, rec_set, ts_space, ts_set,
            mem_type);
    return id;
}


std::vector<ChannelID> HDF5R::channels() const
{
    std::vector<ChannelID> result;
    for (std::map<ChannelID, Channel>::const_iterator ii(channels_.begin());
            ii != channels_.end(); ++ii)
    {
        result.push_back(ii->first);
    }
    return result;
}


ChannelInfo HDF5R::get_channel_info(ChannelID chan_id)
{
    if (channels_.find(chan_id) == channels_.end())
    {
        throw std::runtime_error("Bad channel ID");
    }
    return read_channel_info(channels_[chan_id]);
}


bool HDF5R::have_channel(std::string name) const
{
    for (std::map<ChannelID, Channel>::const_iterator ii(channels_.begin());
            ii != channels_.end(); ++ii)
    {
        if (ii->second.name() == name)
        {
            return true;
        }
    }
    return false;
}


void HDF5R::add_entry(ChannelID chan_id, uint64_t timestamp,
        void const* const buf)
{
    Channel& chan(channels_[chan_id]);
    hsize_t write_size[] = {1};
    hid_t write_space = H5Screate_simple(1, write_size, 0);
    hsize_t extent[1];
    extent[0] = chan.size() + 1;
    hsize_t coords[1];
    coords[0] = chan.size();

    // Extend the record data set by one element
    if (H5Dset_extent(chan.rec_set(), extent) < 0)
    {
        throw std::runtime_error("Failed to extend dataset for new record");
    }
    // Select the (new) last element in the data set
    if (H5Sselect_elements(chan.rec_space(), H5S_SELECT_SET, 1, coords) < 0)
    {
        throw std::runtime_error("Failed to select element to write record");
    }
    // Write the record
    if (H5Dwrite(chan.rec_set(), chan.mem_type(), write_space, chan.rec_space(),
                H5P_DEFAULT, buf) < 0)
    {
        throw std::runtime_error("Failed to write record");
    }

    // Repeat for the time stamp
    if (H5Dset_extent(chan.ts_set(), extent) < 0)
    {
        throw std::runtime_error("Failed to extend dataset for new timestamp");
    }
    if (H5Sselect_elements(chan.ts_space(), H5S_SELECT_SET, 1, coords) < 0)
    {
        throw std::runtime_error("Failed to select element to write timestamp");
    }
    if (H5Dwrite(chan.ts_set(), H5T_NATIVE_ULLONG, write_space, chan.ts_space(),
                H5P_DEFAULT, &timestamp) < 0)
    {
        throw std::runtime_error("Failed to write timestamp");
    }

    // Update the channel size
    chan.size(chan.size() + 1);

    IndexPointerList index_entry;
    index_entry.push_back(IndexPointer(chan_id, coords[0]));
    index_[timestamp] = index_entry;
}


size_t HDF5R::get_entry_size(ChannelID chan_id, hsize_t index)
{
    // index is actually ignored, since all entries in a dataset must be a
    // fixed size in HDF5.
    return H5Tget_size(channels_[chan_id].mem_type());
}


uint64_t HDF5R::get_entry(ChannelID chan_id, hsize_t index, void* const buf)
{
    Channel& chan(channels_[chan_id]);
    hsize_t read_size[] = {1};
    hid_t elem_space = H5Screate_simple(1, read_size, 0);
    hsize_t coords[1];
    coords[0] = index;
    // Select and read the time stamp
    if (H5Sselect_elements(chan.ts_space(), H5S_SELECT_SET, 1, coords) < 0)
    {
        throw std::runtime_error("Failed to select time stamp");
    }
    uint64_t timestamp(0);
    if (H5Dread(chan.ts_set(), H5T_NATIVE_ULLONG, elem_space, chan.ts_space(),
                H5P_DEFAULT, &timestamp) < 0)
    {
        throw std::runtime_error("Failed to read time stamp");
    }
    // Select and read the data
    if (H5Sselect_elements(chan.rec_space(), H5S_SELECT_SET, 1, coords) < 0)
    {
        throw std::runtime_error("Failed to select record");
    }
    if (H5Dread(chan.rec_set(), chan.mem_type(), elem_space, chan.rec_space(),
                H5P_DEFAULT, buf) < 0)
    {
        throw std::runtime_error("Failed to read record");
    }

    return timestamp;
}


std::string HDF5R::get_text_tag(std::string tag)
{
    if (tags_grp_ < 0)
    {
        throw std::runtime_error("No such tag: " + tag);
    }
    return read_string(tags_grp_, tag);
}


size_t HDF5R::get_binary_tag(std::string tag, void* const buf)
{
    if (tags_grp_ < 0)
    {
        throw std::runtime_error("No such tag: " + tag);
    }

    // Open the tag's dataset
    hid_t dset = H5Dopen(tags_grp_, tag.c_str(), H5P_DEFAULT);
    if (dset < 0)
    {
        throw std::runtime_error("No such tag: " + tag);
    }
    hid_t type = H5Dget_type(dset);
    if (type < 0)
    {
        throw std::runtime_error("Failed to get data type for tag " + tag);
    }
    size_t len = H5Tget_size(type);
    if (len < 0)
    {
        throw std::runtime_error("Failed to get data length for tag " + tag);
    }
    // If buf is zero, just get the size
    if (buf == 0)
    {
        return len;
    }
    // Read the tag's data
    if (H5Dread(dset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
    {
        throw std::runtime_error("Error reading data for tag " + tag);
    }
    H5Tclose(type);
    H5Dclose(dset);
    return len;
}


std::map<std::string, TagType> HDF5R::get_tags() const
{
    std::map<std::string, TagType> result;
    if (tags_grp_ < 0)
    {
        // No tags
        return result;
    }

    std::vector<std::string> tags;
    H5Literate(tags_grp_, H5_INDEX_NAME, H5_ITER_NATIVE, 0, get_child_names,
            &tags);

    for(std::vector<std::string>::const_iterator ii(tags.begin());
            ii != tags.end(); ++ii)
    {
        hid_t dset = H5Dopen(tags_grp_, ii->c_str(), H5P_DEFAULT);
        if (dset < 0)
        {
            throw std::runtime_error("Error opening tag " + *ii);
        }
        hid_t type = H5Dget_type(dset);
        if (type < 0)
        {
            throw std::runtime_error("Failed to get data type for tag " + *ii);
        }
        if (H5Tget_class(type) == H5T_STRING)
        {
            result[*ii] = STRING_TAG;
        }
        else
        {
            result[*ii] = BINARY_TAG;
        }
        H5Tclose(type);
        H5Dclose(dset);
    }
    return result;
}


void HDF5R::set_text_tag(std::string tag, std::string value)
{
    prepare_tags_group();
    write_string(tags_grp_, tag, value);
}


void HDF5R::set_binary_tag(std::string tag, void const* const buf, size_t size)
{
    prepare_tags_group();
    // Create a binary type of the necessary length
    hid_t type = H5Tcreate(H5T_OPAQUE, size);
    // Create the dataspace and dataset
    hid_t dspace = H5Screate(H5S_SCALAR);
    hid_t dset = H5Dcreate(tags_grp_, tag.c_str(), type, dspace, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
    // Write the binary blob
    if (H5Dwrite(dset, type, H5S_ALL, H5S_ALL, H5P_DEFAULT, buf) < 0)
    {
        throw std::runtime_error("Error writing binary tag " + tag);
    }
    // Clean up
    H5Dclose(dset);
    H5Sclose(dspace);
    H5Tclose(type);
}


///////////////////////////////////////////////////////////////////////////////
// Private functions
///////////////////////////////////////////////////////////////////////////////


char const* const HDF5R::CHANNELS_GROUP = "/channels";
char const* const HDF5R::TAGS_GROUP = "/tags";
char const* const HDF5R::INDEX_SET = "/index";
char const* const HDF5R::RECORDS_SET = "records";
char const* const HDF5R::TIMESTAMPS_SET = "timestamps";


void HDF5R::prepare()
{
    // If the file does not yet have a channels group, make it
    channels_grp_ = H5Gopen(file_, CHANNELS_GROUP, H5P_DEFAULT);
    if (channels_grp_ < 0)
    {
        channels_grp_ = H5Gcreate(file_, CHANNELS_GROUP, H5P_DEFAULT,
                H5P_DEFAULT, H5P_DEFAULT);
    }
    // Otherwise, read the existing channels from the file
    else
    {
        std::vector<std::string> chan_names;
        H5Literate(channels_grp_, H5_INDEX_NAME, H5_ITER_NATIVE, 0,
                get_child_names, &chan_names);
        for(std::vector<std::string>::const_iterator ii(chan_names.begin());
                ii != chan_names.end(); ++ii)
        {
            hid_t group = H5Gopen(channels_grp_, ii->c_str(), H5P_DEFAULT);
            hid_t rec_set = H5Dopen(group, RECORDS_SET, H5P_DEFAULT);
            hid_t rec_space = H5Dget_space(rec_set);
            hid_t ts_set = H5Dopen(group, TIMESTAMPS_SET, H5P_DEFAULT);
            hid_t ts_space = H5Dget_space(ts_set);
            hid_t mem_type = read_type(group, "mem_type");
            hsize_t num_recs;
            H5Sget_simple_extent_dims(ts_space, &num_recs, 0);
            unsigned int uid = read_uint(group, "uid");
            channels_[uid] = Channel(*ii, group, rec_space, rec_set,
                    ts_space, ts_set, mem_type, num_recs);
            if (uid + 1 > next_id_)
            {
                next_id_ = uid + 1;
            }
        }
    }

    prepare_tags_group();
    read_index();
}


void HDF5R::prepare_tags_group()
{
    // If the tags group is open, nothing to do
    if (tags_grp_ >= 0)
    {
        return;
    }
    // If the file does not yet have a tags group, make it
    tags_grp_ = H5Gopen(file_, TAGS_GROUP, H5P_DEFAULT);
    if (tags_grp_ < 0)
    {
        tags_grp_ = H5Gcreate(file_, TAGS_GROUP, H5P_DEFAULT,
                H5P_DEFAULT, H5P_DEFAULT);
    }
}


ChannelInfo HDF5R::read_channel_info(Channel const& chan) const
{
    // Basic properties
    std::string name = read_string(chan.group(), "name");
    std::string type_name = read_string(chan.group(), "type_name");
    std::string source_name = read_string(chan.group(), "source_name");
    hid_t mem_type = read_type(chan.group(), "mem_type");

    // Get the first and last time stamps
    uint64_t timestamps[2] = {0, 0};
    if (chan.size() > 0)
    {
        hsize_t read_size[] = {2};
        hid_t elem_space = H5Screate_simple(1, read_size, 0);
        hsize_t coords[2];
        coords[0] = 0;
        coords[1] = chan.size() - 1;
        if (H5Sselect_elements(chan.ts_space(), H5S_SELECT_SET, 2, coords) < 0)
        {
            throw std::runtime_error("Failed to select start and end time stamps");
        }
        if (H5Dread(chan.ts_set(), H5T_NATIVE_UINT64, elem_space,
                    chan.ts_space(), H5P_DEFAULT, timestamps) < 0)
        {
            throw std::runtime_error("Failed to read start and end time stamps");
        }
    }

    ChannelInfo result(name, type_name, source_name, mem_type, chan.size(),
            timestamps[0], timestamps[1]);
    H5Tclose(mem_type);
    return result;
}


std::string HDF5R::read_string(hid_t group, std::string set) const
{
    // Open the dataset
    hid_t dset = H5Dopen(group, set.c_str(), H5P_DEFAULT);
    if (dset < 0)
    {
        throw std::runtime_error("Failed to open string " + set);
    }
    // Get the string's data type
    hid_t str_type = H5Dget_type(dset);
    if (str_type < 0)
    {
        throw std::runtime_error("Failed to get string data type for" + set);
    }
    // Make some temporary space to store the string data
    size_t len = H5Tget_size(str_type);
    if (len < 0)
    {
        throw std::runtime_error("Error reading string length for " + set);
    }
    char* temp = new char[len];
    // Read the string from the dataset
    if (H5Dread(dset, str_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, temp) < 0)
    {
        throw std::runtime_error("Error reading string " + set);
    }
    std::string result(temp);
    // Clean up
    delete[] temp;
    H5Tclose(str_type);
    H5Dclose(dset);

    return result;
}


hid_t HDF5R::read_type(hid_t group, std::string set) const
{
    hid_t result = H5Topen(group, set.c_str(), H5P_DEFAULT);
    if (result < 0)
    {
        throw std::runtime_error("Error reading data type " + set);
    }
    return result;
}


unsigned int HDF5R::read_uint(hid_t group, std::string set) const
{
    hid_t dset = H5Dopen(group, set.c_str(), H5P_DEFAULT);
    if (dset < 0)
    {
        throw std::runtime_error("Failed to open uint " + set);
    }
    unsigned int result(0);
    if (H5Dread(dset, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                &result) < 0)
    {
        throw std::runtime_error("Failed to read uint " + set);
    }
    H5Dclose(dset);
    return result;
}


void HDF5R::write_string(hid_t group, std::string set, std::string str)
{
    // Create a string type of the necessary length
    hid_t str_type = H5Tcopy(H5T_C_S1);
    if (H5Tset_size(str_type, str.size() + 1) < 0)
    {
        throw std::runtime_error("Error setting string size for string " + set);
    }
    // Create the dataspace and dataset for the string
    hid_t dspace = H5Screate(H5S_SCALAR);
    hid_t dset = H5Dcreate(group, set.c_str(), str_type, dspace, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
    // Write the string to the dataset
    if (H5Dwrite(dset, str_type, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                str.c_str()) < 0)
    {
        throw std::runtime_error("Error writing string " + set);
    }
    // Clean up
    H5Dclose(dset);
    H5Sclose(dspace);
    H5Tclose(str_type);
}


void HDF5R::write_type(hid_t group, std::string set, hid_t type)
{
    // Copy the type temporarily. If this is not done, we will not be able to
    // commit immutable data types such as H5T_NATIVE_INT.
    hid_t temp = H5Tcopy(type);
    if (temp < 0)
    {
        throw std::runtime_error("Error copying data type");
    }
    if (H5Tcommit(group, set.c_str(), temp, H5P_DEFAULT, H5P_DEFAULT,
                H5P_DEFAULT) < 0)
    {
        throw std::runtime_error("Error writing data type " + set);
    }
    H5Tclose(temp);
}


void HDF5R::write_uint(hid_t group, std::string set, unsigned int value)
{
    hid_t dspace = H5Screate(H5S_SCALAR);
    hid_t dset = H5Dcreate(group, set.c_str(), H5T_STD_U64LE, dspace,
            H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    if (H5Dwrite(dset, H5T_NATIVE_UINT64, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                &value) < 0)
    {
        throw std::runtime_error("Error writing uint " + set);
    }
    H5Dclose(dset);
    H5Sclose(dspace);
}


typedef struct
{
    ChannelID channel;
    uint64_t record;
} RawIndexPointer;
typedef struct
{
    uint64_t timestamp;
    hvl_t records;
} RawIndexEntry;


hid_t HDF5R::make_index_ftype() const
{
    // Individual record pointer type
    hid_t index_ptr = H5Tcreate(H5T_COMPOUND, 8 + 8);
    herr_t s = H5Tinsert(index_ptr, "channel", 0, H5T_STD_U64LE);
    s = H5Tinsert(index_ptr, "record", 8, H5T_STD_U64LE);

    // Variable-length array of record pointers
    hid_t rec_ptr_array = H5Tvlen_create(index_ptr);

    // Single index entry type
    hid_t index_entry = H5Tcreate(H5T_COMPOUND, 8 + sizeof(hvl_t));
    s = H5Tinsert(index_entry, "timestamp", 0, H5T_STD_U64LE);
    s = H5Tinsert(index_entry, "records", 8, rec_ptr_array);
    return index_entry;
}


hid_t HDF5R::make_index_mtype() const
{
    // Individual record pointer type
    hid_t index_ptr = H5Tcreate(H5T_COMPOUND, sizeof(RawIndexPointer));
    herr_t s = H5Tinsert(index_ptr, "channel", HOFFSET(RawIndexPointer, channel),
            H5T_NATIVE_UINT64);
    s = H5Tinsert(index_ptr, "record", HOFFSET(RawIndexPointer, record),
            H5T_NATIVE_UINT64);

    // Variable-length array of record pointers
    hid_t rec_ptr_array = H5Tvlen_create(index_ptr);

    // Single index entry type
    hid_t index_entry = H5Tcreate(H5T_COMPOUND, sizeof(RawIndexEntry));
    s = H5Tinsert(index_entry, "timestamp", HOFFSET(RawIndexEntry, timestamp),
            H5T_NATIVE_UINT64);
    s = H5Tinsert(index_entry, "records", HOFFSET(RawIndexEntry, records),
            rec_ptr_array);
    return index_entry;
}


void HDF5R::read_index()
{
    // Attempt to open the index, if it exists
    hid_t index_set = H5Dopen(file_, INDEX_SET, H5P_DEFAULT);
    if (index_set < 0)
    {
        // No index, nothing to do
        return;
    }

    hid_t mtype = make_index_mtype();
    hid_t ftype = make_index_ftype();
    hsize_t read_size[] = {1};
    hid_t read_space = H5Screate_simple(1, read_size, 0);
    hid_t index_space = H5Dget_space(index_set);

    hsize_t num_entries(0);
    H5Sget_simple_extent_dims(index_space, &num_entries, 0);
    for (hsize_t coords = 0; coords < num_entries; ++coords)
    {
        // Select the next index entry
        if (H5Sselect_elements(index_space, H5S_SELECT_SET, 1, &coords) < 0)
        {
            throw std::runtime_error(
                    "Failed to select index entry for reading");
        }
        // Read the entry
        RawIndexEntry raw_entry;
        if (H5Dread(index_set, mtype, read_space, index_space, H5P_DEFAULT,
                    &raw_entry) < 0)
        {
            throw std::runtime_error("Failed to read index entry");
        }
        // Copy the data out
        IndexPointerList index_entry;
        for (unsigned int ii(0); ii < raw_entry.records.len; ++ii)
        {
            RawIndexPointer* ptrs =
                reinterpret_cast<RawIndexPointer*>(raw_entry.records.p);
            index_entry.push_back(IndexPointer(ptrs[ii].channel,
                        ptrs[ii].record));
        }
        index_[raw_entry.timestamp] = index_entry;
        // Clean up the allocated memory
        H5Dvlen_reclaim(mtype, read_space, H5P_DEFAULT, &raw_entry);
    }
    H5Sclose(index_space);
    H5Sclose(read_space);
    H5Tclose(ftype);
    H5Tclose(mtype);
    H5Dclose(index_set);
}


void HDF5R::write_index()
{
    // Can't write the index in read-only mode
    if (mode_ == RDONLY)
    {
        return;
    }
    // If the file has an index, remove it
    hid_t index = H5Dopen(file_, INDEX_SET, H5P_DEFAULT);
    if (index >= 0)
    {
        H5Dclose(index);
        H5Ldelete(file_, INDEX_SET, H5P_DEFAULT);
    }
    // If the current index is empty, do nothing
    if (index_.size() == 0)
    {
        return;
    }

    // Create the index dataset
    hid_t mtype = make_index_mtype();
    hid_t ftype = make_index_ftype();
    hsize_t write_size[] = {1};
    hid_t write_space = H5Screate_simple(1, write_size, 0);
    hsize_t len(index_.size());
    hid_t dspace = H5Screate_simple(1, &len, 0);
    hid_t dset = H5Dcreate(file_, INDEX_SET, ftype, dspace, H5P_DEFAULT,
            H5P_DEFAULT, H5P_DEFAULT);
    // Write out each index element one at a time to avoid duplicating a
    // potentially large amount of memory
    RawIndexEntry entry;
    unsigned int offset(0);
    for(Index::const_iterator ii(index_.begin()); ii != index_.end(); ++ii)
    {
        // Build the index entry in a memory format suitable for writing
        entry.timestamp = ii->first;
        entry.records.len = ii->second.size();
        RawIndexPointer* ptrs = new RawIndexPointer[entry.records.len];
        for (unsigned int jj(0); jj < entry.records.len; ++jj)
        {
            ptrs[jj].channel = ii->second[jj].first;
            ptrs[jj].record = ii->second[jj].second;
        }
        entry.records.p = ptrs;
        // Select the entry in the file
        hsize_t coords[1];
        coords[0] = offset++;
        if (H5Sselect_elements(dspace, H5S_SELECT_SET, 1, coords) < 0)
        {
            throw std::runtime_error(
                    "Failed to select element in index for writing.");
        }
        // Write the entry
        if (H5Dwrite(dset, mtype, write_space, dspace, H5P_DEFAULT,
                    &entry) < 0)
        {
            throw std::runtime_error("Failed to write index element.");
        }
        // Clean up
        delete[] ptrs;
    }
    H5Dclose(dset);
    H5Sclose(dspace);
    H5Sclose(write_space);
    H5Tclose(ftype);
    H5Tclose(mtype);
}

