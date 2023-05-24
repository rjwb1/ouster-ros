/**
 * Copyright (c) 2018-2023, Ouster, Inc.
 * All rights reserved.
 *
 * @file os_sensor_nodelet.cpp
 * @brief A nodelet that connects to a live ouster sensor
 */

// prevent clang-format from altering the location of "ouster_ros/ros.h", the
// header file needs to be the first include due to PCL_NO_PRECOMPILE flag
// clang-format off
#include "ouster_ros/os_ros.h"
#include <ouster/impl/build.h>
// clang-format on

#include "ros/ros.h"

#include <pluginlib/class_list_macros.h>

#include <fstream>
#include <string>
#include <tuple>
#include <thread>
#include <boost/thread/mutex.hpp>

#include "ouster_ros/GetConfig.h"
#include "ouster_ros/PacketMsg.h"
#include "ouster_ros/SetConfig.h"
#include "ouster_ros/GetMetadata.h"

#include <std_msgs/String.h>
#include <sensor_msgs/Imu.h>
#include <sensor_msgs/PointCloud2.h>
#include <tf2_ros/transform_broadcaster.h>
#include <pcl_conversions/pcl_conversions.h>

namespace sensor = ouster::sensor;
using nonstd::optional;
using ouster_ros::GetConfig;
using ouster_ros::PacketMsg;
using ouster_ros::SetConfig;
using ouster_ros::GetMetadata;
using sensor::UDPProfileLidar;

template <typename T, typename UnaryPredicate>
int find_if_reverse(const Eigen::Array<T, -1, 1>& array,
                    UnaryPredicate predicate) {
    auto p = array.data() + array.size() - 1;
    do {
        if (predicate(*p)) return p - array.data();
    } while (p-- != array.data());
    return -1;
}

uint64_t linear_interpolate(int x0, uint64_t y0, int x1, uint64_t y1, int x) {
    uint64_t min_v, max_v;
    double sign;
    if (y1 > y0) {
        min_v = y0;
        max_v = y1;
        sign = +1;
    } else {
        min_v = y1;
        max_v = y0;
        sign = -1;
    }
    return y0 + (x - x0) * sign * (max_v - min_v) / (x1 - x0);
}

template <typename T>
uint64_t ulround(T value) {
    T rounded_value = std::round(value);
    if (rounded_value < 0) return 0ULL;
    if (rounded_value > ULLONG_MAX) return ULLONG_MAX;
    return static_cast<uint64_t>(rounded_value);
}

class OusterNode {
   public:
    OusterNode(){
        pnh = ros::NodeHandle("~");
        sensor_hostname = get_sensor_hostname(pnh);
        sensor::sensor_config config;
        uint8_t flags;
        std::tie(config, flags) = create_sensor_config_rosparams(pnh);
        configure_sensor(sensor_hostname, config, flags);
        sensor_client = create_sensor_client(sensor_hostname, config);
        create_metadata_publisher(nh);
        update_config_and_metadata(*sensor_client);
        publish_metadata();
        // save_metadata(pnh);
        create_get_metadata_service(nh);
        create_get_config_service(nh);
        create_set_config_service(nh);
        ros::Duration(1.0).sleep(); // sleep for a second
        parse_parameters();
        start_sensor_connection_thread();
        ROS_INFO("Ouster Ready!");
    }

    ~OusterNode()
    {
        stop_sensor_connection_thread();
    }
    
    private:
    std::string get_sensor_hostname(ros::NodeHandle& nh) {
        auto hostname = nh.param("sensor_hostname", std::string{});
        if (!is_arg_set(hostname)) {
            auto error_msg = "Must specify a sensor hostname";
            ROS_ERROR_STREAM(error_msg);
            throw std::runtime_error(error_msg);
        }

        return hostname;
    }

    bool update_config_and_metadata(sensor::client& cli) {
        sensor::sensor_config config;
        auto success = get_config(sensor_hostname, config);
        if (!success) {
            ROS_ERROR("Failed to collect sensor config");
            cached_config.clear();
            cached_metadata.clear();
            return false;
        }

        cached_config = to_string(config);

        try {
            cached_metadata = sensor::get_metadata(cli);
        } catch (const std::exception& e) {
            ROS_ERROR_STREAM(
                "sensor::get_metadata exception: " << e.what());
            cached_metadata.clear();
        }

        if (cached_metadata.empty()) {
            ROS_ERROR("Failed to collect sensor metadata");
            return false;
        }

        info = sensor::parse_metadata(cached_metadata);
        // TODO: revist when *min_version* is changed
        populate_metadata_defaults(info, sensor::MODE_UNSPEC);
        display_lidar_info(info);

        return cached_config.size() > 0 && cached_metadata.size() > 0;
    }

void create_get_metadata_service(ros::NodeHandle& nh) {
    get_metadata_srv =
        nh.advertiseService<GetMetadata::Request, GetMetadata::Response>(
            "get_metadata",
            [this](GetMetadata::Request&, GetMetadata::Response& res) {
                res.metadata = cached_metadata;
                return cached_metadata.size() > 0;
            });

    ROS_INFO("get_metadata service created");
}

void create_metadata_publisher(ros::NodeHandle& nh) {
    metadata_pub = nh.advertise<std_msgs::String>("metadata", 1, true);
}

void publish_metadata() {
    std_msgs::String metadata_msg;
    metadata_msg.data = cached_metadata;
    metadata_pub.publish(metadata_msg);
    metadata_handler(cached_metadata);
}

void display_lidar_info(const sensor::sensor_info& info) {
    auto lidar_profile = info.format.udp_profile_lidar;
    ROS_INFO_STREAM(
        "ouster client version: "
        << ouster::SDK_VERSION_FULL << "\n"
        << "product: " << info.prod_line << ", sn: " << info.sn
        << ", firmware rev: " << info.fw_rev << "\n"
        << "lidar mode: " << sensor::to_string(info.mode) << ", "
        << "lidar udp profile: " << sensor::to_string(lidar_profile));
}

    void save_metadata(ros::NodeHandle& nh) {
        auto meta_file = nh.param("metadata", std::string{});
        if (!is_arg_set(meta_file)) {
            meta_file = sensor_hostname.substr(0, sensor_hostname.rfind('.')) +
                        "-metadata.json";
            ROS_INFO_STREAM(
                "No metadata file was specified, using: " << meta_file);
        }

        // write metadata file. If metadata_path is relative, will use cwd
        // (usually ~/.ros)
        if (!write_metadata(meta_file, cached_metadata)) {
            ROS_ERROR("Exiting because of failure to write metadata path");
            throw std::runtime_error("Failure to write metadata path");
        }
    }

    void create_get_config_service(ros::NodeHandle& nh) {
        get_config_srv =
            nh.advertiseService<GetConfig::Request, GetConfig::Response>(
                "get_config",
                [this](GetConfig::Request&, GetConfig::Response& response) {
                    response.config = cached_config;
                    return cached_config.size() > 0;
                });

        ROS_INFO("get_config service created");
    }

    void create_set_config_service(ros::NodeHandle& nh) {
        set_config_srv =
            nh.advertiseService<SetConfig::Request, SetConfig::Response>(
                "set_config", [this](SetConfig::Request& request,
                                     SetConfig::Response& response) {
                    sensor::sensor_config config;
                    response.config = "";
                    auto success =
                        load_config_file(request.config_file, config);
                    if (!success) {
                        ROS_ERROR_STREAM("Failed to load and parse file: "
                                             << request.config_file);
                        return false;
                    }

                    try {
                        configure_sensor(sensor_hostname, config, 0);
                    } catch (const std::exception& e) {
                        return false;
                    }
                    success = update_config_and_metadata(*sensor_client);
                    response.config = cached_config;
                    return success;
                });

        ROS_INFO("set_config service created");
    }

    std::shared_ptr<sensor::client> create_sensor_client(
        const std::string& hostname, const sensor::sensor_config& config) {
        ROS_INFO_STREAM("Starting sensor " << hostname
                                               << " initialization...");

        int lidar_port =
            config.udp_port_lidar ? config.udp_port_lidar.value() : 0;
        int imu_port = config.udp_port_imu ? config.udp_port_imu.value() : 0;
        auto udp_dest = config.udp_dest ? config.udp_dest.value() : "";

        std::shared_ptr<sensor::client> cli;
        if (sensor::in_multicast(udp_dest)) {
            // use the mtp_init_client to recieve data via multicast
            // if mtp_main is true when sensor will be configured
            cli = sensor::mtp_init_client(hostname, config, mtp_dest, mtp_main);
        } else if (lidar_port != 0 && imu_port != 0) {
            // use no-config version of init_client to bind to pre-configured
            // ports
            cli = sensor::init_client(hostname, lidar_port, imu_port);
        } else {
            // use the full init_client to generate and assign random ports to
            // sensor
            cli = sensor::init_client(hostname, udp_dest, sensor::MODE_UNSPEC,
                                      sensor::TIME_FROM_UNSPEC, lidar_port,
                                      imu_port);
        }

        if (!cli) {
            auto error_msg = "Failed to initialize client";
            ROS_ERROR_STREAM(error_msg);
            throw std::runtime_error(error_msg);
        }

        return cli;
    }

    std::pair<sensor::sensor_config, uint8_t> create_sensor_config_rosparams(
        ros::NodeHandle& nh) {
        auto udp_dest = nh.param("udp_dest", std::string{});
        auto mtp_dest_arg = nh.param("mtp_dest", std::string{});
        auto mtp_main_arg = nh.param("mtp_main", false);
        auto lidar_port = nh.param("lidar_port", 0);
        auto imu_port = nh.param("imu_port", 0);
        auto lidar_mode_arg = nh.param("lidar_mode", std::string{});
        auto timestamp_mode_arg = nh.param("timestamp_mode", std::string{});
        auto udp_profile_lidar_arg =
            nh.param("udp_profile_lidar", std::string{});

        if (lidar_port < 0 || lidar_port > 65535) {
            auto error_msg =
                "Invalid lidar port number! port value should be in the range "
                "[0, 65535].";
            ROS_ERROR_STREAM(error_msg);
            throw std::runtime_error(error_msg);
        }

        if (imu_port < 0 || imu_port > 65535) {
            auto error_msg =
                "Invalid imu port number! port value should be in the range "
                "[0, 65535].";
            ROS_ERROR_STREAM(error_msg);
            throw std::runtime_error(error_msg);
        }

        optional<sensor::UDPProfileLidar> udp_profile_lidar;
        if (is_arg_set(udp_profile_lidar_arg)) {
            // set lidar profile from param
            udp_profile_lidar =
                sensor::udp_profile_lidar_of_string(udp_profile_lidar_arg);
            if (!udp_profile_lidar) {
                auto error_msg =
                    "Invalid udp profile lidar: " + udp_profile_lidar_arg;
                ROS_ERROR_STREAM(error_msg);
                throw std::runtime_error(error_msg);
            }
        }

        // set lidar mode from param
        sensor::lidar_mode lidar_mode = sensor::MODE_UNSPEC;
        if (is_arg_set(lidar_mode_arg)) {
            lidar_mode = sensor::lidar_mode_of_string(lidar_mode_arg);
            if (!lidar_mode) {
                auto error_msg = "Invalid lidar mode: " + lidar_mode_arg;
                ROS_ERROR_STREAM(error_msg);
                throw std::runtime_error(error_msg);
            }
        }

        // set timestamp mode from param
        sensor::timestamp_mode timestamp_mode = sensor::TIME_FROM_UNSPEC;
        if (is_arg_set(timestamp_mode_arg)) {
            // In case the option TIME_FROM_ROS_TIME is set then leave the
            // sensor timestamp_mode unmodified
            if (timestamp_mode_arg == "TIME_FROM_ROS_TIME") {
                ROS_INFO(
                    "TIME_FROM_ROS_TIME timestamp mode specified."
                    " IMU and pointcloud messages will use ros time");
            } else {
                timestamp_mode =
                    sensor::timestamp_mode_of_string(timestamp_mode_arg);
                if (!timestamp_mode) {
                    auto error_msg =
                        "Invalid timestamp mode: " + timestamp_mode_arg;
                    ROS_ERROR_STREAM(error_msg);
                    throw std::runtime_error(error_msg);
                }
            }
        }

        sensor::sensor_config config;
        if (lidar_port == 0) {
            ROS_WARN_COND(
                !is_arg_set(mtp_dest_arg),
                "lidar port set to zero, the client will assign a random port "
                "number!");
        } else {
            config.udp_port_lidar = lidar_port;
        }

        if (imu_port == 0) {
            ROS_WARN_COND(
                !is_arg_set(mtp_dest_arg),
                "imu port set to zero, the client will assign a random port "
                "number!");
        } else {
            config.udp_port_imu = imu_port;
        }

        config.udp_profile_lidar = udp_profile_lidar;
        config.operating_mode = sensor::OPERATING_NORMAL;
        if (lidar_mode) config.ld_mode = lidar_mode;
        if (timestamp_mode) config.ts_mode = timestamp_mode;

        uint8_t config_flags = 0;

        if (is_arg_set(udp_dest)) {
            ROS_INFO_STREAM("Will send UDP data to " << udp_dest);
            config.udp_dest = udp_dest;
            if (sensor::in_multicast(udp_dest)) {
                if (is_arg_set(mtp_dest_arg)) {
                    ROS_INFO_STREAM("Will recieve data via multicast on "
                                        << mtp_dest_arg);
                    mtp_dest = mtp_dest_arg;
                } else {
                    ROS_INFO(
                        "mtp_dest was not set, will recieve data via multicast "
                        "on first available interface");
                    mtp_dest = std::string{};
                }
                mtp_main = mtp_main_arg;
            }
        } else {
            ROS_INFO("Will use automatic UDP destination");
            config_flags |= ouster::sensor::CONFIG_UDP_DEST_AUTO;
        }

        return std::make_pair(config, config_flags);
    }

    void configure_sensor(const std::string& hostname,
                          sensor::sensor_config& config, int config_flags) {
        if (config.udp_dest && sensor::in_multicast(config.udp_dest.value()) &&
            !mtp_main) {
            if (!get_config(hostname, config, true)) {
                ROS_ERROR("Error getting active config");
            } else {
                ROS_INFO("Retrived active config of sensor");
            }
            return;
        }

        try {
            if (!set_config(hostname, config, config_flags)) {
                auto err_msg = "Error connecting to sensor " + hostname;
                ROS_ERROR_STREAM(err_msg);
                throw std::runtime_error(err_msg);
            }
        } catch (const std::exception& e) {
            ROS_ERROR("Error setting config:  %s", e.what());
            throw;
        }

        ROS_INFO_STREAM("Sensor " << hostname
                                      << " was configured successfully");
    }

    bool load_config_file(const std::string& config_file,
                          sensor::sensor_config& out_config) {
        std::ifstream ifs{};
        ifs.open(config_file);
        if (ifs.fail()) return false;
        std::stringstream buf;
        buf << ifs.rdbuf();
        out_config = sensor::parse_config(buf.str());
        return true;
    }

    // fill in values that could not be parsed from metadata
    void populate_metadata_defaults(sensor::sensor_info& info,
                                    sensor::lidar_mode specified_lidar_mode) {
        if (!info.name.size()) info.name = "UNKNOWN";
        if (!info.sn.size()) info.sn = "UNKNOWN";

        ouster::util::version v = ouster::util::version_of_string(info.fw_rev);
        if (v == ouster::util::invalid_version)
            ROS_WARN(
                "Unknown sensor firmware version; output may not be reliable");
        else if (v < sensor::min_version)
            ROS_WARN(
                "Firmware < %s not supported; output may not be reliable",
                to_string(sensor::min_version).c_str());

        if (!info.mode) {
            ROS_WARN(
                "Lidar mode not found in metadata; output may not be reliable");
            info.mode = specified_lidar_mode;
        }

        if (!info.prod_line.size()) info.prod_line = "UNKNOWN";

        if (info.beam_azimuth_angles.empty() ||
            info.beam_altitude_angles.empty()) {
            ROS_ERROR(
                "Beam angles not found in metadata; using design values");
            info.beam_azimuth_angles = sensor::gen1_azimuth_angles;
            info.beam_altitude_angles = sensor::gen1_altitude_angles;
        }
    }

    // try to write metadata file
    bool write_metadata(const std::string& meta_file,
                        const std::string& metadata) {
        std::ofstream ofs(meta_file);
        if (ofs.is_open()) {
            ofs << metadata << std::endl;
            ofs.close();
            ROS_INFO("Wrote metadata to %s", meta_file.c_str());
        } else {
            ROS_WARN(
                "Failed to write metadata to %s; check that the path is valid. "
                "If you provided a relative path, please note that the working "
                "directory of all ROS nodes is set by default to $ROS_HOME",
                meta_file.c_str());
            return false;
        }
        return true;
    }

    void allocate_buffers() {
        auto& pf = sensor::get_format(info);
        lidar_packet.buf.resize(pf.lidar_packet_size + 1);
        imu_packet.buf.resize(pf.imu_packet_size + 1);
    }

    void start_sensor_connection_thread() {
        sensor_connection_active = true;
        sensor_connection_thread = std::make_unique<std::thread>([this]() {
            allocate_buffers();
            create_publishers(nh);
            auto& pf = sensor::get_format(info);
            while (sensor_connection_active) {
                connection_loop(*sensor_client, pf);
            }
            ROS_INFO("connection_loop DONE.");
        });
    }

    void stop_sensor_connection_thread() {
        if (sensor_connection_thread->joinable()) {
            sensor_connection_active = false;
            sensor_connection_thread->join();
        }
    }

    //////////

    bool is_arg_set(const std::string& arg) const {
        return arg.find_first_not_of(' ') != std::string::npos;
    }

    void parse_parameters() {
        auto tf_prefix = pnh.param("tf_prefix", std::string{});
        if (is_arg_set(tf_prefix) && tf_prefix.back() != '/')
            tf_prefix.append("/");
        sensor_frame = tf_prefix + "os_sensor";
        imu_frame = tf_prefix + "os_imu";
        lidar_frame = tf_prefix + "os_lidar";
        auto timestamp_mode_arg = pnh.param("timestamp_mode", std::string{});
        use_ros_time = timestamp_mode_arg == "TIME_FROM_ROS_TIME";
    }

    void metadata_handler(const std::string metadata) {
        // TODO: handle sensor reconfigurtion
        ROS_INFO("OusterCloud: retrieved new sensor metadata!");
        info = sensor::parse_metadata(metadata);
        n_returns = compute_n_returns(info.format);
        scan_col_ts_spacing_ns = compute_scan_col_ts_spacing_ns(info.mode);
        create_lidarscan_objects();
        compute_scan_ts = [this](const auto& ts_v) {
            return compute_scan_ts_0(ts_v);
        };
        create_publishers(nh);
    }

    static int compute_n_returns(const sensor::data_format& format) {
        return format.udp_profile_lidar ==
                       UDPProfileLidar::PROFILE_RNG19_RFL8_SIG16_NIR16_DUAL
                   ? 2
                   : 1;
    }

    static double compute_scan_col_ts_spacing_ns(sensor::lidar_mode ld_mode) {
        const auto scan_width = sensor::n_cols_of_lidar_mode(ld_mode);
        const auto scan_frequency = sensor::frequency_of_lidar_mode(ld_mode);
        const double one_sec_in_ns = 1e+9;
        return one_sec_in_ns / (scan_width * scan_frequency);
    }

    void create_lidarscan_objects() {
        // The ouster_ros drive currently only uses single precision when it
        // produces the point cloud. So it isn't of a benefit to compute point
        // cloud xyz coordinates using double precision (for the time being).
        auto xyz_lut = ouster::make_xyz_lut(info);
        lut_direction = xyz_lut.direction.cast<float>();
        lut_offset = xyz_lut.offset.cast<float>();
        points = ouster::PointsF(lut_direction.rows(), lut_offset.cols());
        pc_ptr = boost::make_shared<sensor_msgs::PointCloud2>();

        uint32_t H = info.format.pixels_per_column;
        uint32_t W = info.format.columns_per_frame;
        ls = ouster::LidarScan{W, H, info.format.udp_profile_lidar};
        cloud = ouster_ros::Cloud{W, H};

        scan_batcher = std::make_unique<ouster::ScanBatcher>(info);
    }

    void create_publishers(ros::NodeHandle& nh) {
        imu_pub = nh.advertise<sensor_msgs::Imu>("imu", 100);

        auto img_suffix = [](int ind) {
            if (ind == 0) return std::string();
            return std::to_string(ind + 1);  // need second return to return 2
        };

        lidar_pubs.resize(n_returns);
        for (int i = 0; i < n_returns; i++) {
            auto pub = nh.advertise<sensor_msgs::PointCloud2>(
                std::string("points") + img_suffix(i), 10);
            lidar_pubs[i] = pub;
        }
    }

    void pcl_toROSMsg(const ouster_ros::Cloud& pcl_cloud,
                      sensor_msgs::PointCloud2& cloud) {
        // TODO: remove the staging step in the future
        static pcl::PCLPointCloud2 pcl_pc2;
        pcl::toPCLPointCloud2(pcl_cloud, pcl_pc2);
        pcl_conversions::moveFromPCL(pcl_pc2, cloud);
    }

    void convert_scan_to_pointcloud_publish(uint64_t scan_ts,
                                            const ros::Time& msg_ts) {
        for (int i = 0; i < n_returns; ++i) {
            scan_to_cloud_f(points, lut_direction, lut_offset, scan_ts, ls,
                            cloud, i);
            pcl_toROSMsg(cloud, *pc_ptr);
            pc_ptr->header.stamp = msg_ts;
            pc_ptr->header.frame_id = sensor_frame;
            lidar_pubs[i].publish(pc_ptr);
        }

        tf_bcast.sendTransform(ouster_ros::transform_to_tf_msg(
            info.lidar_to_sensor_transform, sensor_frame, lidar_frame, msg_ts));

        tf_bcast.sendTransform(ouster_ros::transform_to_tf_msg(
            info.imu_to_sensor_transform, sensor_frame, imu_frame, msg_ts));
    }

    uint64_t impute_value(int last_scan_last_nonzero_idx,
                          uint64_t last_scan_last_nonzero_value,
                          int curr_scan_first_nonzero_idx,
                          uint64_t curr_scan_first_nonzero_value,
                          int scan_width) {
        assert(scan_width + curr_scan_first_nonzero_idx >
               last_scan_last_nonzero_idx);
        double interpolated_value = linear_interpolate(
            last_scan_last_nonzero_idx, last_scan_last_nonzero_value,
            scan_width + curr_scan_first_nonzero_idx,
            curr_scan_first_nonzero_value, scan_width);
        return ulround(interpolated_value);
    }

    uint64_t extrapolate_value(int curr_scan_first_nonzero_idx,
                               uint64_t curr_scan_first_nonzero_value) {
        double extrapolated_value =
            curr_scan_first_nonzero_value -
            scan_col_ts_spacing_ns * curr_scan_first_nonzero_idx;
        return ulround(extrapolated_value);
    }

    // compute_scan_ts_0 for first scan
    uint64_t compute_scan_ts_0(
        const ouster::LidarScan::Header<uint64_t>& ts_v) {
        auto idx = std::find_if(ts_v.data(), ts_v.data() + ts_v.size(),
                                [](uint64_t h) { return h != 0; });
        assert(idx != ts_v.data() + ts_v.size());  // should never happen
        int curr_scan_first_nonzero_idx = idx - ts_v.data();
        uint64_t curr_scan_first_nonzero_value = *idx;

        uint64_t scan_ns =
            curr_scan_first_nonzero_idx == 0
                ? curr_scan_first_nonzero_value
                : extrapolate_value(curr_scan_first_nonzero_idx,
                                    curr_scan_first_nonzero_value);

        last_scan_last_nonzero_idx =
            find_if_reverse(ts_v, [](uint64_t h) { return h != 0; });
        assert(last_scan_last_nonzero_idx >= 0);  // should never happen
        last_scan_last_nonzero_value = ts_v(last_scan_last_nonzero_idx);
        compute_scan_ts = [this](const auto& ts_v) {
            return compute_scan_ts_n(ts_v);
        };
        return scan_ns;
    }

    // compute_scan_ts_n applied to all subsequent scans except first one
    uint64_t compute_scan_ts_n(
        const ouster::LidarScan::Header<uint64_t>& ts_v) {
        auto idx = std::find_if(ts_v.data(), ts_v.data() + ts_v.size(),
                                [](uint64_t h) { return h != 0; });
        assert(idx != ts_v.data() + ts_v.size());  // should never happen
        int curr_scan_first_nonzero_idx = idx - ts_v.data();
        uint64_t curr_scan_first_nonzero_value = *idx;

        uint64_t scan_ns = curr_scan_first_nonzero_idx == 0
                               ? curr_scan_first_nonzero_value
                               : impute_value(last_scan_last_nonzero_idx,
                                              last_scan_last_nonzero_value,
                                              curr_scan_first_nonzero_idx,
                                              curr_scan_first_nonzero_value,
                                              static_cast<int>(ts_v.size()));

        last_scan_last_nonzero_idx =
            find_if_reverse(ts_v, [](uint64_t h) { return h != 0; });
        assert(last_scan_last_nonzero_idx >= 0);  // should never happen
        last_scan_last_nonzero_value = ts_v(last_scan_last_nonzero_idx);
        return scan_ns;
    }

    uint16_t packet_col_index(const uint8_t* packet_buf) {
        const auto& pf = sensor::get_format(info);
        return pf.col_measurement_id(pf.nth_col(0, packet_buf));
    }

    ros::Time extrapolate_frame_ts(const uint8_t* lidar_buf,
                                   const ros::Time current_time) {
        auto curr_scan_first_arrived_idx = packet_col_index(lidar_buf);
        auto delta_time = ros::Duration(
            0,
            std::lround(scan_col_ts_spacing_ns * curr_scan_first_arrived_idx));
        return current_time - delta_time;
    }

    /////////

    void handle_lidar_data(PacketMsg lidar_packet) {
        auto packet_receive_time = ros::Time::now();
        const uint8_t* packet_buf = lidar_packet.buf.data();
        static auto frame_ts = extrapolate_frame_ts(
            packet_buf, packet_receive_time);  // first point cloud time
        if (!(*scan_batcher)(packet_buf, ls)) return;
        auto scan_ts = compute_scan_ts(ls.timestamp());
        convert_scan_to_pointcloud_publish(scan_ts, frame_ts);
        // set time for next point cloud msg
        frame_ts = extrapolate_frame_ts(packet_buf, packet_receive_time);
    }

    void handle_imu_data(PacketMsg imu_packet) {
        const auto& pf = sensor::get_format(info);
        ros::Time msg_ts = ros::Time::now();
        sensor_msgs::Imu imu_msg =
            ouster_ros::packet_to_imu_msg(imu_packet, msg_ts, imu_frame, pf);
        sensor_msgs::ImuPtr imu_msg_ptr =
            boost::make_shared<sensor_msgs::Imu>(imu_msg);
        imu_pub.publish(imu_msg_ptr);
    }

    void connection_loop(sensor::client& cli, const sensor::packet_format& pf) {
        boost::mutex::scoped_lock lock(connection_mutex);
        auto state = sensor::poll_client(cli);
        if (state == sensor::EXIT) {
            ROS_INFO("poll_client: caught signal, exiting");
            return;
        }
        if (state & sensor::CLIENT_ERROR) {
            ROS_ERROR("poll_client: returned error");
            return;
        }
        if (state & sensor::LIDAR_DATA) {
            if (sensor::read_lidar_packet(cli, lidar_packet.buf.data(), pf))
                handle_lidar_data(lidar_packet);
        }
        if (state & sensor::IMU_DATA) {
            if (sensor::read_imu_packet(cli, imu_packet.buf.data(), pf))
                handle_imu_data(imu_packet);
        }
    }

    private:

    ros::NodeHandle nh; //, nh_param("~");
    ros::NodeHandle pnh; //, nh_param("~");

    ouster::sensor::sensor_info info;
    ros::ServiceServer get_metadata_srv;
    std::string cached_metadata;
    ros::Publisher metadata_pub;

    PacketMsg lidar_packet;
    PacketMsg imu_packet;
    std::shared_ptr<sensor::client> sensor_client;
    std::string sensor_hostname;
    ros::ServiceServer get_config_srv;
    ros::ServiceServer set_config_srv;
    std::string cached_config;
    std::string mtp_dest;
    bool mtp_main;

    ros::Subscriber metadata_sub;
    std::vector<ros::Publisher> lidar_pubs;
    ros::Publisher imu_pub;
    sensor_msgs::PointCloud2::Ptr pc_ptr;
    int n_returns = 0;

    ouster::PointsF lut_direction;
    ouster::PointsF lut_offset;
    ouster::PointsF points;
    ouster::LidarScan ls;
    ouster_ros::Cloud cloud;
    std::unique_ptr<ouster::ScanBatcher> scan_batcher;

    std::string sensor_frame;
    std::string imu_frame;
    std::string lidar_frame;

    tf2_ros::TransformBroadcaster tf_bcast;

    bool use_ros_time;

    int last_scan_last_nonzero_idx = -1;
    uint64_t last_scan_last_nonzero_value = 0;
    std::function<uint64_t(const ouster::LidarScan::Header<uint64_t>&)>
        compute_scan_ts;
    double scan_col_ts_spacing_ns;  // interval or spacing between columns of a
                                    // scan

    std::unique_ptr<std::thread> sensor_connection_thread;
    std::atomic<bool> sensor_connection_active = {false};
    boost::mutex connection_mutex;
};

int main(int argc, char **argv)
{
    ros::init(argc, argv, "ouster_node");

    OusterNode ouster_node;

    ros::MultiThreadedSpinner spinner(1); // Use 1 threads

    while (ros::ok())
    {
        spinner.spin(); // spin() will not return until the node has been shutdown
    }

    ros::waitForShutdown();

    return 0;
}