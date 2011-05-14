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
 * Sample HDF5R usage.
 */

#include <algorithm>
#include <cassert>
#include <hdf5r/hdf5r.h>
#include <iostream>


struct print_tag_fun
{
    print_tag_fun(hdf5r::HDF5R& f)
        : f_(f)
    {}

    void operator()(std::pair<const std::string, hdf5r::TagType>& tag)
    {
        std::cout << tag.first << ": ";
        if (tag.second == hdf5r::STRING_TAG)
        {
            std::cout << f_.get_text_tag(tag.first) << '\n';
        }
        else
        {
            std::cout << "<Binary blob (" << f_.get_binary_tag(tag.first, 0) <<
                 "bytes)\n";
        }
    }

    hdf5r::HDF5R& f_;
};


struct print_chan_fun
{
    print_chan_fun(hdf5r::HDF5R& f)
        : f_(f)
    {}

    void operator()(hdf5r::ChannelID& chan_id)
    {
        hdf5r::ChannelInfo const& chan(f_.get_channel_info(chan_id));
        std::cout << "Channel '" << chan.name() << "'\n";
        std::cout << "\tType name: " << chan.type_name() << '\n';
        std::cout << "\tSource name: " << chan.source_name() << '\n';
        std::cout << "\tNumber of records: " << chan.size() << '\n';
        std::cout << "\tStart time: " << chan.start_time() << '\n';
        std::cout << "\tEnd time: " << chan.end_time() << '\n';
    }

    hdf5r::HDF5R& f_;
};


struct print_chan_data_fun
{
    print_chan_data_fun(hdf5r::HDF5R& f)
        : f_(f)
    {}

    void operator()(hdf5r::ChannelID& chan_id)
    {
        hdf5r::ChannelInfo const& chan(f_.get_channel_info(chan_id));
        std::cout << "Data for channel " << chan.name();
        std::cout << " (" << f_.get_entry_size(chan_id, 0) <<
            " bytes/record):\n";
        std::cout << "Time stamp\t\tRecord\n";
        uint64_t ts(0);
        uint64_t buf(0);
        for (hsize_t ii = 0; ii < chan.size(); ++ii)
        {
            ts = f_.get_entry(chan_id, ii, &buf);
            std::cout << ts << '\t';
            if (f_.get_channel_info(chan_id).type_name() == "int")
            {
                std::cout << buf << '\n';
            }
            else
            {
                std::cout << *reinterpret_cast<float*>(&buf) << '\n';
            }
        }
    }

    hdf5r::HDF5R& f_;
};


struct print_index_fun
{
    void operator()(std::pair<const uint64_t, hdf5r::IndexPointerList>& entry)
    {
        hdf5r::IndexPointerList list = entry.second;
        std::cout << entry.first << "ns: ";
        for (hdf5r::IndexPointerList::const_iterator ii(list.begin());
                ii != list.end(); ++ii)
        {
            std::cout << '\t' << ii->first << '[' << ii->second << "]\n";
        }
    }
};


uint64_t get_ts()
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}


void round_one()
{
    std::cout << "Creating new file test.hdf5r\n";
    // Open the HDF5 file
    hdf5r::HDF5R f("test.hdf5r", hdf5r::TRUNCATE);

    // Show the number of channels - it will be zero for a new file
    std::cout << "Number of channels in the file: " << f.channels().size() <<
        '\n';

    // Add a couple of channels of varying type
    std::cout << "Adding channel 'integers' to log\n";
    hdf5r::ChannelID int_chan = f.add_channel("integers", "int", "thin_air",
            H5T_NATIVE_INT, H5T_STD_I64LE);
    std::cout << "Adding channel 'floats' to log\n";
    hdf5r::ChannelID float_chan = f.add_channel("floats", "float",
            "thinner_air", H5T_NATIVE_FLOAT, H5T_IEEE_F32LE);

    std::cout << "Number of channels in the file: " << f.channels().size() <<
        '\n';

    // Put some data into the channels
    std::cout << "Writing some data\n";
    int int_data[] = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89};
    float float_data[] = {2.7182818284590451, 7.3890560989306495,
        20.085536923187664, 54.598150033144229, 148.41315910257657,
        403.428793492735, 1096.6331584284583, 2980.9579870417269,
        8103.0839275753797, 22026.465794806703};
    f.add_entry(int_chan, get_ts(), &int_data[0]);
    f.add_entry(int_chan, get_ts(), &int_data[1]);
    f.add_entry(float_chan, get_ts(), &float_data[0]);
    f.add_entry(int_chan, get_ts(), &int_data[2]);
    f.add_entry(float_chan, get_ts(), &float_data[1]);
    f.add_entry(float_chan, get_ts(), &float_data[2]);
    f.add_entry(float_chan, get_ts(), &float_data[3]);
    f.add_entry(float_chan, get_ts(), &float_data[4]);
    f.add_entry(int_chan, get_ts(), &int_data[3]);
    f.add_entry(float_chan, get_ts(), &float_data[5]);
    f.add_entry(int_chan, get_ts(), &int_data[4]);
    f.add_entry(float_chan, get_ts(), &float_data[6]);
    f.add_entry(int_chan, get_ts(), &int_data[5]);
    f.add_entry(int_chan, get_ts(), &int_data[6]);
    f.add_entry(int_chan, get_ts(), &int_data[7]);
    f.add_entry(float_chan, get_ts(), &float_data[7]);
    f.add_entry(int_chan, get_ts(), &int_data[8]);
    f.add_entry(float_chan, get_ts(), &float_data[8]);
    f.add_entry(int_chan, get_ts(), &int_data[9]);
    f.add_entry(float_chan, get_ts(), &float_data[9]);

    // Add a couple of tags
    std::cout << "Setting tags\n";
    f.set_text_tag("TITLE", "Sample HDF5R log");
    f.set_text_tag("LIB_NAME", "hdf5r 1.0");

    std::cout << "Closing file\n\n";
}


void round_two()
{
    std::cout << "Opening existing file test.hdf5r\n";
    // Open the HDF5 file
    hdf5r::HDF5R f("test.hdf5r", hdf5r::RDONLY);

    std::cout << "File tags:\n";
    std::map<std::string, hdf5r::TagType> tags(f.get_tags());
    std::for_each(tags.begin(), tags.end(), print_tag_fun(f));

    // Show the number of channels
    std::cout << "Number of channels in the file: " << f.channels().size() <<
        '\n';

    // Print out some interesting file information
    std::vector<hdf5r::ChannelID> chans(f.channels());
    std::for_each(chans.begin(), chans.end(), print_chan_fun(f));

    // Print out the data, one channel at a time
    std::for_each(chans.begin(), chans.end(), print_chan_data_fun(f));

    // Print out the data in time order

    // Print out the index
    std::cout << "Index:\n";
    hdf5r::Index index = f.index();
    std::for_each(index.begin(), index.end(), print_index_fun());
}


int main(int argc, char** argv)
{
    round_one();
    round_two();
    return 0;
}

