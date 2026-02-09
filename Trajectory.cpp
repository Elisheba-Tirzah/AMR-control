#include <iostream>
#include <thread>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include <csignal>
#include <cmath>
#include <vector>
#include <fstream>
#include <sstream>
#include <queue>
#include <mutex>
#include <atomic>
#include <time.h>
#include <cerrno> 

#define CAN_INTERFACE               "can1"
#define CSV_LOG_FILENAME            "circular_trajectory_v1.csv"
#define CYCLE_FREQUENCY_HZ          500      
#define CYCLE_PERIOD_MS             (1000 / CYCLE_FREQUENCY_HZ) 

#define HEARTBEAT_INTERVAL_MS       500  
#define CONSUMER_TIMEOUT_MS         1000 

#define TPDO1_SYNC_INTERVAL         1    
#define TPDO2_SYNC_INTERVAL         20   

#define ENCODER_RESOLUTION_STEERING        65541.0f
#define MOTOR_GEAR_RATIO_STEERING          138.6f
#define INC_PER_DEGREE                     25595.0f  

#define ENCODER_RESOLUTION_TRAVEL        10000.0f  
#define MOTOR_GEAR_RATIO_TRAVEL          45.5f
#define WHEEL_RADIUS                     0.0825  

#define RPM_TO_COUNTS_PER_SEC(rpm, enc_res)  ((int32_t)(((rpm) * 512.0f * (enc_res)) / 1875.0f))
#define RPM_PER_SEC_TO_INTERNAL_ACCEL(rpm_per_s, enc_res)  ((int32_t)((((rpm_per_s) / 60.0f) * 65536.0f * (enc_res)) / 4000000.0f))

#define MAX_STEERING_RPM            1000.0f     
#define STEER_ACCEL_RPM_PER_SEC     80.0f
#define MAX_TRAVEL_RPM              100.0f   
#define TRAVEL_ACCEL_RPM_PER_SEC    50.0f

#define STEER_DEADBAND_DEG          0.5      
#define STEER_MIN_RPM               2.0      
#define TRAVEL_MIN_RPM              3.0      

#define MAX_VEL_CHANGE_PER_CYCLE    10.0     

#define TRAJECTORY_TYPE             1        
#define CIRCLE_RADIUS               2.0      
#define DESIRED_WHEEL_RPM           2.2     
#define STEER_SLOWDOWN_THRESHOLD    20.0     
#define FRONT_WHEEL_X               0.342   
#define FRONT_WHEEL_Y               0.000   
#define REAR_LEFT_WHEEL_X          -0.341   
#define REAR_LEFT_WHEEL_Y           0.254   
#define REAR_RIGHT_WHEEL_X         -0.341   
#define REAR_RIGHT_WHEEL_Y         -0.254   


enum MotorType {
    MOTOR_TYPE_STEERING,  
    MOTOR_TYPE_TRAVEL     
};

struct WheelPosition {
    double x;  
    double y;  
};

struct NodeControl {
    uint8_t id;
    MotorType motor_type;
    float encoder_resolution;
    float gear_ratio;
    
    uint16_t controlword;
    int32_t velocity_forward;
    int32_t velocity_reverse;
    int32_t accel_dec;
    
    double current_rpm;              
    double target_rpm;               
    int32_t current_target_velocity; 
    
    double target_angle_deg;  
    double current_angle_deg; 
    
    int32_t actual_value;  
    bool tpdo1_received;
    uint32_t tpdo1_receive_count;
    
    uint16_t statusword;
    bool tpdo2_received;
    bool fault_active;
    
    struct timespec command_sent_time;
    bool waiting_for_response;

    NodeControl(uint8_t node_id, MotorType type) 
        : id(node_id), 
        motor_type(type),
        controlword(0x000F),
        current_rpm(0.0),
        target_rpm(0.0),
        target_angle_deg(0.0),
        current_angle_deg(0.0),
        actual_value(0),
        tpdo1_received(false),
        tpdo1_receive_count(0),
        statusword(0),
        tpdo2_received(false),
        fault_active(false),
        waiting_for_response(false) {
        
        if (motor_type == MOTOR_TYPE_STEERING) {
            encoder_resolution = ENCODER_RESOLUTION_STEERING;
            gear_ratio = MOTOR_GEAR_RATIO_STEERING;
            velocity_forward = RPM_TO_COUNTS_PER_SEC(MAX_STEERING_RPM, encoder_resolution);
            velocity_reverse = RPM_TO_COUNTS_PER_SEC(-MAX_STEERING_RPM, encoder_resolution);
            accel_dec = RPM_PER_SEC_TO_INTERNAL_ACCEL(STEER_ACCEL_RPM_PER_SEC, encoder_resolution);
            current_target_velocity = 0;
            std::cout << "[NODE " << (int)node_id << "] STEERING initialized\n";
        } else {
            encoder_resolution = ENCODER_RESOLUTION_TRAVEL;
            gear_ratio = MOTOR_GEAR_RATIO_TRAVEL;
            velocity_forward = RPM_TO_COUNTS_PER_SEC(MAX_TRAVEL_RPM, encoder_resolution);
            velocity_reverse = RPM_TO_COUNTS_PER_SEC(-MAX_TRAVEL_RPM, encoder_resolution);
            accel_dec = RPM_PER_SEC_TO_INTERNAL_ACCEL(TRAVEL_ACCEL_RPM_PER_SEC, encoder_resolution);
            current_target_velocity = 0;
            std::cout << "[NODE " << (int)node_id << "] TRAVEL initialized\n";
        }
    }
    
    const char* getMotorTypeName() const {
        return (motor_type == MOTOR_TYPE_STEERING) ? "STEERING" : "TRAVEL";
    }
};

static std::atomic<bool> running(true);
static int can_socket = -1;
static std::vector<NodeControl> nodes;
static uint32_t sync_counter = 0;

static std::vector<WheelPosition> wheel_positions = {
    {FRONT_WHEEL_X, FRONT_WHEEL_Y},        
    {REAR_LEFT_WHEEL_X, REAR_LEFT_WHEEL_Y}, 
    {REAR_RIGHT_WHEEL_X, REAR_RIGHT_WHEEL_Y}
};

static std::queue<std::string> log_queue;
static std::mutex log_mutex;
static std::atomic<bool> logger_running(true);
static std::thread logger_thread;
static std::thread heartbeat_thread;
static struct timespec start_time;

void signal_handler(int) { running = false; }
inline void delay_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

inline double timespec_diff_ms(const struct timespec& start, const struct timespec& end) {
    double sec_diff = (double)(end.tv_sec - start.tv_sec);
    double nsec_diff = (double)(end.tv_nsec - start.tv_nsec);
    return (sec_diff * 1000.0) + (nsec_diff / 1000000.0);
}

double normalizeAngleDeg(double angle) {
    angle = fmod(angle + 180.0, 360.0);
    if (angle < 0) angle += 360.0;
    return angle - 180.0;
}

double applyVelocityRamp(double current_rpm, double target_rpm, double max_change) {
    double error = target_rpm - current_rpm;
    
    if (std::abs(error) <= max_change) {
        return target_rpm;  
    } else if (error > 0) {
        return current_rpm + max_change;  
    } else {
        return current_rpm - max_change;  
    }
}

void loggerThreadFunc() {
    std::ofstream log_file(CSV_LOG_FILENAME);
    log_file << "Timestamp_ms,PathAngle_deg,Front_TargetAngle,Front_ActualAngle,Front_RPM,"
             << "RL_TargetAngle,RL_ActualAngle,RL_RPM,"
             << "RR_TargetAngle,RR_ActualAngle,RR_RPM,SlowMode\n";
    log_file.flush();
    
    while (logger_running || !log_queue.empty()) {
        std::string entry;
        bool has_entry = false;
        {
            std::lock_guard<std::mutex> lock(log_mutex);
            if (!log_queue.empty()) {
                entry = log_queue.front();
                log_queue.pop();
                has_entry = true;
            }
        }
        if (has_entry) {
            log_file << entry << "\n";
            log_file.flush();
        } else {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    log_file.close();
}

void addLog(double path_angle, 
            double f_tgt, double f_act, double f_rpm,
            double rl_tgt, double rl_act, double rl_rpm,
            double rr_tgt, double rr_act, double rr_rpm,
            bool slow) {
    struct timespec current_time;
    clock_gettime(CLOCK_MONOTONIC_RAW, &current_time);
    double timestamp_ms = timespec_diff_ms(start_time, current_time);
    
    std::stringstream ss;
    ss << std::fixed << std::setprecision(4) << timestamp_ms << ","
       << std::setprecision(2) << path_angle << ","
       << f_tgt << "," << f_act << "," << f_rpm << ","
       << rl_tgt << "," << rl_act << "," << rl_rpm << ","
       << rr_tgt << "," << rr_act << "," << rr_rpm << ","
       << (slow ? "1" : "0");
    
    std::lock_guard<std::mutex> lock(log_mutex);
    log_queue.push(ss.str());
}

int setupCAN(const char* ifname) {
    int s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if (s < 0) return -1;
    
    int rcvbuf_size = 1048576;
    setsockopt(s, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(rcvbuf_size));
    
    struct ifreq ifr{};
    strcpy(ifr.ifr_name, ifname);
    ioctl(s, SIOCGIFINDEX, &ifr);
    
    struct sockaddr_can addr{};
    addr.can_family = AF_CAN;
    addr.can_ifindex = ifr.ifr_ifindex;
    
    int flags = fcntl(s, F_GETFL, 0);
    fcntl(s, F_SETFL, flags | O_NONBLOCK);

    if (bind(s, (struct sockaddr*)&addr, sizeof(addr)) < 0) return -1;
    return s;
}

void enableTPDOFilter() {
    struct can_filter broad_filters[4];
    broad_filters[0].can_id = 0x180; broad_filters[0].can_mask = 0x780;
    broad_filters[1].can_id = 0x280; broad_filters[1].can_mask = 0x780;
    broad_filters[2].can_id = 0x080; broad_filters[2].can_mask = 0x780;
    broad_filters[3].can_id = 0x580; broad_filters[3].can_mask = 0x780;
    setsockopt(can_socket, SOL_CAN_RAW, CAN_RAW_FILTER, broad_filters, sizeof(broad_filters));
}

void sendFrame(uint32_t id, uint8_t len, const uint8_t* d = nullptr) {
    struct can_frame f{};
    f.can_id = id;
    f.can_dlc = len;
    if (d) memcpy(f.data, d, len);
    int res = write(can_socket, &f, sizeof(f));
    if (res < 0) {
    }
}

void sendSYNC() { sendFrame(0x080, 0); }

void sendMasterHeartbeat() {
    uint8_t d[1] = {0x05}; 
    sendFrame(0x700, 1, d); 
}

void masterHeartbeatLoop() {
    auto next_wake = std::chrono::steady_clock::now();
    const auto interval = std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS);
    while (running) {
        sendMasterHeartbeat();
        next_wake += interval;
        std::this_thread::sleep_until(next_wake);
    }
}

void sendNMT(uint8_t cmd, uint8_t id) { 
    uint8_t d[2] = {cmd, id}; 
    sendFrame(0x000, 2, d);
}

void sendSDO(uint8_t id, uint8_t cs, uint16_t idx, uint8_t sub, int32_t val) {
    uint8_t d[8]{};
    d[0] = cs; d[1] = idx & 0xFF; d[2] = idx >> 8; d[3] = sub;
    d[4] = (val & 0xFF); d[5] = ((val >> 8) & 0xFF);
    d[6] = ((val >> 16) & 0xFF); d[7] = ((val >> 24) & 0xFF);
    sendFrame(0x600 + id, 8, d);
    delay_ms(15);
}

void sendSDO16(uint8_t id, uint16_t idx, uint8_t sub, uint16_t val) {
    uint8_t d[8]{};
    d[0] = 0x2B; d[1] = idx & 0xFF; d[2] = idx >> 8; d[3] = sub;
    d[4] = (val & 0xFF); d[5] = ((val >> 8) & 0xFF);
    sendFrame(0x600 + id, 8, d);
    delay_ms(15);
}

void sendSDO8(uint8_t id, uint16_t idx, uint8_t sub, int8_t val) {
    uint8_t d[8]{};
    d[0] = 0x2F; d[1] = idx & 0xFF; d[2] = idx >> 8; d[3] = sub;
    d[4] = val;
    sendFrame(0x600 + id, 8, d);
    delay_ms(15);
}

void sendRPDO_Command(uint8_t id, uint16_t controlword, int32_t vel) {
    uint8_t d[6];
    d[0] = controlword & 0xFF;
    d[1] = (controlword >> 8) & 0xFF;
    memcpy(&d[2], &vel, 4);
    sendFrame(0x200 + id, 6, d); 
}

bool processIncomingFrame() {
    struct can_frame f{};
    
    ssize_t nbytes = read(can_socket, &f, sizeof(f));
    
    if (nbytes > 0) {
        if ((f.can_id & 0x780) == 0x080) {
            uint8_t id = f.can_id & 0x00F;
            if (id > 0) {
                 for (auto& node : nodes) {
                    if (node.id == id) {
                        node.fault_active = true;
                        return true;
                    }
                }
            }
        }
        else if ((f.can_id & 0x780) == 0x180) { // TPDO1
            uint8_t id = f.can_id & 0x00F;
            for (auto& node : nodes) {
                if (node.id == id && f.can_dlc == 4) {
                    memcpy(&node.actual_value, f.data, 4);
                    
                    if (node.motor_type == MOTOR_TYPE_STEERING) {
                        double raw_angle_deg = (double)node.actual_value / INC_PER_DEGREE;
                        node.current_angle_deg = normalizeAngleDeg(raw_angle_deg);
                    }
                    
                    node.tpdo1_received = true;
                    node.tpdo1_receive_count++;
                    return true;
                }
            }
        }
        else if ((f.can_id & 0x780) == 0x280) { // TPDO2
            uint8_t id = f.can_id & 0x00F;
            for (auto& node : nodes) {
                if (node.id == id && f.can_dlc >= 2) {
                    node.statusword = f.data[0] | (f.data[1] << 8);
                    node.tpdo2_received = true;
                    return true;
                }
            }
        }
    } else {
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            // Real error
        }
    }
    return false;
}

void initSingleNode(NodeControl& node) {
    std::cout << "\n   -> Init Node " << (int)node.id << " (" << node.getMotorTypeName() << ")...\n";
    sendNMT(0x81, node.id); delay_ms(150);
    sendNMT(0x80, node.id); delay_ms(150);
    
    sendSDO(node.id, 0x23, 0x1016, 0x01, (0x00 << 16) | CONSUMER_TIMEOUT_MS);
    sendSDO8(node.id, 0x6060, 0x00, -3);
    
    sendSDO(node.id, 0x23, 0x1400, 0x01, 0x80000200 + node.id); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1600, 0x00, 0x00); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1600, 0x01, 0x60400010); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1600, 0x02, 0x60FF0020); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1600, 0x00, 0x02); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1400, 0x02, 0x01); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1400, 0x01, 0x0200 + node.id); delay_ms(50);

    sendSDO(node.id, 0x23, 0x1800, 0x01, 0x80000180 + node.id); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1A00, 0x00, 0x00); delay_ms(50);
    if (node.motor_type == MOTOR_TYPE_STEERING) {
        sendSDO(node.id, 0x23, 0x1A00, 0x01, 0x60640020); 
    } else {
        sendSDO(node.id, 0x23, 0x1A00, 0x01, 0x606C0020); 
    }
    delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1A00, 0x00, 0x01); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1800, 0x02, TPDO1_SYNC_INTERVAL); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1800, 0x01, 0x0180 + node.id); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1801, 0x01, 0x80000280 + node.id); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1A01, 0x00, 0x00); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1A01, 0x01, 0x60410010); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1A01, 0x00, 0x01); delay_ms(50);
    sendSDO(node.id, 0x2F, 0x1801, 0x02, TPDO2_SYNC_INTERVAL); delay_ms(50);
    sendSDO(node.id, 0x23, 0x1801, 0x01, 0x0280 + node.id); delay_ms(50);
    
    sendNMT(0x01, node.id); delay_ms(150);

    sendSDO16(node.id, 0x6040, 0x00, 0x0006); delay_ms(100);
    sendSDO16(node.id, 0x6040, 0x00, 0x0007); delay_ms(100);
    sendSDO16(node.id, 0x6040, 0x00, 0x000F); delay_ms(100);
    
    std::cout << "   -> Node " << (int)node.id << " initialized\n";
}

void initAllNodes() {
    std::cout << "\nINIT 6 NODES FOR CIRCULAR MOTION\n";
    nodes.emplace_back(2, MOTOR_TYPE_STEERING); 
    nodes.emplace_back(4, MOTOR_TYPE_STEERING); 
    nodes.emplace_back(6, MOTOR_TYPE_STEERING); 

    nodes.emplace_back(1, MOTOR_TYPE_TRAVEL);   
    nodes.emplace_back(3, MOTOR_TYPE_TRAVEL);   
    nodes.emplace_back(5, MOTOR_TYPE_TRAVEL);   

    for (auto& node : nodes) {
        initSingleNode(node);
        delay_ms(100);
    }
    
    enableTPDOFilter();
    std::cout << "\n[INIT] All Nodes Ready.\n";
}

int32_t wheelRPMToMotorVelocity(double wheel_rpm) {
    double motor_rpm = wheel_rpm * MOTOR_GEAR_RATIO_TRAVEL;
    return RPM_TO_COUNTS_PER_SEC(motor_rpm, ENCODER_RESOLUTION_TRAVEL);
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    clock_gettime(CLOCK_MONOTONIC_RAW, &start_time);
    
    logger_running = true;
    logger_thread = std::thread(loggerThreadFunc);

    can_socket = setupCAN(argc > 1 ? argv[1] : CAN_INTERFACE);
    if (can_socket < 0) {
        std::cerr << "[ERROR] Failed to setup CAN interface\n";
        return 1;
    }

    heartbeat_thread = std::thread(masterHeartbeatLoop);
    initAllNodes();

    std::cout << "\n[MOTION] STARTING DYNAMIC CIRCULAR TRAJECTORY\n";
    std::cout << "  - Circle Radius: " << CIRCLE_RADIUS << " m\n";
    std::cout << "  - Target Wheel Speed: " << DESIRED_WHEEL_RPM << " RPM (Motor: ~100 RPM)\n";
    std::cout << "  - Anti-Jerk: Enabled (max " << MAX_VEL_CHANGE_PER_CYCLE << " RPM/cycle)\n";
    std::cout << "  - Anti-Drift: Enabled (deadband " << STEER_DEADBAND_DEG << "°)\n\n";

    double path_angle = 0.0;
   
    double linear_velocity_mps = (DESIRED_WHEEL_RPM * 2.0 * M_PI * WHEEL_RADIUS) / 60.0;
    double angular_velocity_rad_s = linear_velocity_mps / CIRCLE_RADIUS;
    double angular_velocity_deg_per_cycle = (angular_velocity_rad_s * 180.0 / M_PI) / CYCLE_FREQUENCY_HZ;
 
    std::cout << "[CALC] Linear Speed: " << linear_velocity_mps << " m/s\n";
    std::cout << "[CALC] Steer Speed: " << angular_velocity_rad_s * 180.0 / M_PI << " deg/s\n";
    
    auto next = std::chrono::steady_clock::now();
    const auto period = std::chrono::milliseconds(CYCLE_PERIOD_MS);
    
    int loop_counter = 0;
    
    std::cout << "[INIT] Reading initial steering angles...\n";
    for (int k = 0; k < 20; k++) {
        sendSYNC();
        delay_ms(5);
        int proc = 0;
        while (processIncomingFrame() && proc++ < 30) {}
    }
    
    std::cout << "Initial angles: Front=" << nodes[1].current_angle_deg 
              << "° RL=" << nodes[3].current_angle_deg
              << "° RR=" << nodes[5].current_angle_deg << "°\n\n";
    
    while (running) {
        for (auto& node : nodes) {
            node.tpdo1_received = false;
            node.tpdo2_received = false;
        }
        
        int pre_read = 0;
        while (processIncomingFrame() && pre_read++ < 50) {}
        
        path_angle += angular_velocity_deg_per_cycle;
        if (path_angle >= 360.0) path_angle -= 360.0;
        
        double target_steer_angle = path_angle - 90.0;
        target_steer_angle = normalizeAngleDeg(target_steer_angle);
        
        nodes[1].target_angle_deg = target_steer_angle;         // Front
        nodes[3].target_angle_deg = target_steer_angle;   // RL
        nodes[5].target_angle_deg = target_steer_angle;   // RR
        
        bool any_wheel_slow_mode = false;

        for (size_t i = 0; i < 3; i++) {
            NodeControl& travel_node = nodes[i * 2];
            NodeControl& steer_node = nodes[i * 2 + 1];
            
            /* Anti-drift and smooth proportional control*/
            double angle_error = normalizeAngleDeg(steer_node.target_angle_deg - steer_node.current_angle_deg);
            double steer_rpm = 0.0;
            
            if (std::abs(angle_error) > STEER_DEADBAND_DEG) {
                double max_steer_command = 50.0; 
                steer_rpm = std::max(-max_steer_command, 
                                     std::min(max_steer_command, 
                                              angle_error * 0.8)); 
                
                if (std::abs(steer_rpm) < STEER_MIN_RPM) {
                    steer_rpm = (steer_rpm > 0) ? STEER_MIN_RPM : -STEER_MIN_RPM;
                }
            }
            
            if (steer_rpm > MAX_STEERING_RPM) steer_rpm = MAX_STEERING_RPM;
            if (steer_rpm < -MAX_STEERING_RPM) steer_rpm = -MAX_STEERING_RPM;
            
            steer_node.target_rpm = steer_rpm;
            steer_node.current_rpm = applyVelocityRamp(
                steer_node.current_rpm, 
                steer_node.target_rpm, 
                MAX_VEL_CHANGE_PER_CYCLE
            );
            
            steer_node.current_target_velocity = RPM_TO_COUNTS_PER_SEC(
                steer_node.current_rpm, 
                ENCODER_RESOLUTION_STEERING
            );
            
            double abs_error = std::abs(angle_error);
            double speed_scalar = 1.0;

            if (abs_error > STEER_SLOWDOWN_THRESHOLD) {
                speed_scalar = 0.15; 
                any_wheel_slow_mode = true;
            }

            double target_wheel_rpm = DESIRED_WHEEL_RPM * speed_scalar;
            
            if (target_wheel_rpm > 0 && target_wheel_rpm < TRAVEL_MIN_RPM) {
                target_wheel_rpm = TRAVEL_MIN_RPM;
            }
            
            travel_node.target_rpm = target_wheel_rpm;
            travel_node.current_rpm = applyVelocityRamp(
                travel_node.current_rpm,
                travel_node.target_rpm,
                MAX_VEL_CHANGE_PER_CYCLE
            );
            
            travel_node.current_target_velocity = wheelRPMToMotorVelocity(travel_node.current_rpm);
        }
        
        for (auto& node : nodes) {
            sendRPDO_Command(node.id, 0x000F, node.current_target_velocity);
        }
        
        sendSYNC();
        sync_counter++;
        
        if (loop_counter % 10 == 0) {
            addLog(path_angle,
                   nodes[1].target_angle_deg, nodes[1].current_angle_deg, nodes[1].current_rpm,
                   nodes[3].target_angle_deg, nodes[3].current_angle_deg, nodes[3].current_rpm,
                   nodes[5].target_angle_deg, nodes[5].current_angle_deg, nodes[5].current_rpm,
                   any_wheel_slow_mode);
        }
        
        if (loop_counter % 100 == 0) {
            std::cout << "[T=" << std::setw(6) << loop_counter * CYCLE_PERIOD_MS << "ms] "
                      << "PathAngle=" << std::setw(6) << std::fixed << std::setprecision(1) << path_angle << "° | "
                      << "Mode: " << (any_wheel_slow_mode ? "SLOW " : "FULL ") << "| "
                      << "Front: Tgt=" << std::setw(6) << nodes[1].target_angle_deg 
                      << "° Act=" << std::setw(6) << nodes[1].current_angle_deg 
                      << "° Err=" << std::setw(5) << normalizeAngleDeg(nodes[1].target_angle_deg - nodes[1].current_angle_deg) 
                      << "° SteerRPM=" << std::setw(5) << std::setprecision(1) << nodes[1].current_rpm << "\n";
        }
        
        next += period;
        std::this_thread::sleep_until(next);
        loop_counter++;
    }

    std::cout << "\n[EXIT] Shutting down...\n";
    
    for (int i = 0; i < 20; i++) {
        for (auto& node : nodes) {
            node.current_rpm *= 0.8; 
            if (node.motor_type == MOTOR_TYPE_STEERING) {
                node.current_target_velocity = RPM_TO_COUNTS_PER_SEC(node.current_rpm, ENCODER_RESOLUTION_STEERING);
            } else {
                node.current_target_velocity = wheelRPMToMotorVelocity(node.current_rpm);
            }
            sendRPDO_Command(node.id, 0x000F, node.current_target_velocity);
        }
        sendSYNC();
        delay_ms(CYCLE_PERIOD_MS);
    }
    
    for (auto& node : nodes) {
        sendRPDO_Command(node.id, 0x0006, 0);
    }
    
    delay_ms(100);
    close(can_socket);
    
    if (logger_thread.joinable()) {
        logger_running = false;
        logger_thread.join();
    }
    
    if (heartbeat_thread.joinable()) {
        heartbeat_thread.join();
    }

    return 0;
}