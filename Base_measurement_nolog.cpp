#include <iostream>
#include <thread>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <vector>
#include <fstream>
#include <sstream>
#include <queue>
#include <mutex>
#include <atomic>
#include <cmath>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <linux/can.h>
#include <linux/can/raw.h>
#include <csignal>
#include <time.h>
#include <cerrno>

#include <sched.h>
#include <pthread.h>

static std::mutex can_mutex;

constexpr const char* CAN_INTERFACE          = "can1";
constexpr const char* CSV_LOG_FILENAME       = "schema.csv";
constexpr int CYCLE_FREQUENCY_HZ             = 500;
constexpr int CYCLE_PERIOD_MS                = (1000 / CYCLE_FREQUENCY_HZ);
constexpr int SYNC_PROCESSING_DELAY_US       = 800; 
constexpr int MASTER_HEARTBEAT_INTERVAL_MS   = 100;
constexpr int NODE_HEARTBEAT_INTERVAL_MS     = 500;  
constexpr int HEARTBEAT_TIMEOUT_MS           = 500; 
constexpr int TPDO1_SYNC_INTERVAL            = 1; 
constexpr int TPDO2_SYNC_INTERVAL            = 20;

constexpr float ENCODER_RESOLUTION_STEERING  = 92500.0f;
constexpr float MOTOR_GEAR_RATIO_STEERING    = 138.6f;
constexpr float ENCODER_RESOLUTION_TRAVEL    = 10000.0f;
constexpr float MOTOR_GEAR_RATIO_TRAVEL      = 45.5f;
constexpr float STEER_TARGET_RPM_FORWARD     = 10.0f;
constexpr float STEER_TARGET_RPM_REVERSE     = -10.0f;
constexpr float STEER_ACCEL_RPM_PER_SEC      = 50.0f;

constexpr uint16_t SW_NOT_READY_TO_SWITCH_ON = 0x0000;
constexpr uint16_t SW_SWITCH_ON_DISABLED     = 0x0040;
constexpr uint16_t SW_READY_TO_SWITCH_ON     = 0x0021;
constexpr uint16_t SW_SWITCHED_ON            = 0x0023;
constexpr uint16_t SW_OPERATION_ENABLED      = 0x0027;
constexpr uint16_t SW_FAULT                  = 0x0008;
constexpr uint16_t SW_TARGET_REACHED         = 0x0400;
constexpr uint16_t SW_WARNING                = 0x0080;

inline int32_t RPM_TO_COUNTS_PER_SEC(float rpm, float enc_res) {
    return static_cast<int32_t>((rpm * 512.0f * enc_res) / 1875.0f);
}

inline int32_t RPM_PER_SEC_TO_INTERNAL_ACCEL(float rpm_per_s, float enc_res) {
    return static_cast<int32_t>(((rpm_per_s / 60.0f) * 65536.0f * enc_res) / 4000000.0f);
}

enum MotorType {
    STEERING_MOTOR,  
    TRAVEL_MOTOR   
};

struct NodeControl {
    uint8_t id;
    MotorType motor_type;
    float encoder_resolution;
    float gear_ratio;
    int32_t velocity_forward;
    int32_t velocity_reverse;
    int32_t accel_dec;
    int32_t current_target_velocity;
    int32_t actual_velocity;
    uint32_t tpdo1_receive_count;
    uint32_t tpdo1_expected_count;
    uint32_t tpdo1_missed_count;
    uint16_t statusword;
    uint16_t last_statusword;
    uint32_t tpdo2_receive_count;
    uint32_t tpdo2_expected_count;
    uint32_t tpdo2_missed_count;
    std::chrono::steady_clock::time_point last_heartbeat_time;
    std::chrono::steady_clock::time_point command_sent_time;
    double last_response_time_ms;
    bool tpdo1_received;
    bool tpdo1_expected_this_cycle;
    bool tpdo2_received;
    bool tpdo2_expected_this_cycle;
    bool fault_active;
    bool waiting_for_response;
    bool heartbeat_timeout;

    /*Recovery state machine variables*/ 
    bool is_recovering;
    int recovery_step;
    int recovery_timer; 
    int toggle_cooldown = 0;


    NodeControl(uint8_t node_id, MotorType type) 
        : id(node_id), 
        motor_type(type),
        actual_velocity(0),
        tpdo1_receive_count(0), 
        tpdo1_expected_count(0), 
        tpdo1_missed_count(0),
        statusword(0), last_statusword(0),
        tpdo2_receive_count(0), 
        tpdo2_expected_count(0), 
        tpdo2_missed_count(0),
        last_heartbeat_time(std::chrono::steady_clock::now()),
        last_response_time_ms(0.0),
        tpdo1_received(false), 
        tpdo1_expected_this_cycle(false),
        tpdo2_received(false), 
        tpdo2_expected_this_cycle(false),
        fault_active(false), 
        waiting_for_response(false),
        heartbeat_timeout(false),
        is_recovering(false),
        recovery_step(0),
        recovery_timer(0) {
        
        if (motor_type == STEERING_MOTOR) {
            encoder_resolution = ENCODER_RESOLUTION_STEERING;
            gear_ratio = MOTOR_GEAR_RATIO_STEERING;
        } else {
            encoder_resolution = ENCODER_RESOLUTION_TRAVEL;
            gear_ratio = MOTOR_GEAR_RATIO_TRAVEL;
        }
        
        velocity_forward = RPM_TO_COUNTS_PER_SEC(STEER_TARGET_RPM_FORWARD, encoder_resolution);
        velocity_reverse = RPM_TO_COUNTS_PER_SEC(STEER_TARGET_RPM_REVERSE, encoder_resolution);
        accel_dec = RPM_PER_SEC_TO_INTERNAL_ACCEL(STEER_ACCEL_RPM_PER_SEC, encoder_resolution);
        
        current_target_velocity = velocity_forward;
    }
};

// struct FastLogEntry {
//     uint64_t timestamp_us;   
//     uint8_t node_id;
//     uint32_t can_id;
//     char direction[4];       
//     uint8_t data[8];         
//     uint8_t data_len;
//     char description[32];    
// };

static std::atomic<bool> running(true);
static int can_socket = -1;
static std::vector<NodeControl> nodes;
static uint32_t sync_counter = 0;

/*Lock-Free Ring Buffer Definition*/ 
template <typename T, size_t Size>
class LockFreeRingBuffer {
private:
    T buffer[Size];
    std::atomic<size_t> head{0};
    std::atomic<size_t> tail{0};
    std::mutex write_mutex; 

public:
    bool push(const T& item) {
        std::lock_guard<std::mutex> lock(write_mutex); 

        size_t current_head = head.load(std::memory_order_relaxed);
        size_t next_head = (current_head + 1) % Size;

        if (next_head == tail.load(std::memory_order_acquire)) {
            return false; // Buffer is full
        }

        buffer[current_head] = item;
        head.store(next_head, std::memory_order_release);
        return true;
    }

    bool pop(T& item) {
        size_t current_tail = tail.load(std::memory_order_relaxed);
        if (current_tail == head.load(std::memory_order_acquire)) {
            return false; 
        }

        item = buffer[current_tail];
        tail.store((current_tail + 1) % Size, std::memory_order_release);
        return true;
    }
};

// static LockFreeRingBuffer<FastLogEntry, 4096> fast_log_queue;

// static std::atomic<bool> logger_running(true);
// static std::thread logger_thread;
static std::thread heartbeat_thread;
static std::chrono::steady_clock::time_point global_start_time;

/*Utils*/
void signal_handler(int) { running = false; }
inline void delay_ms(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }
inline void delay_us(int us) { std::this_thread::sleep_for(std::chrono::microseconds(us)); }

void pinThisThreadToCore(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    
    int rc = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    // if (rc != 0) {
    //     std::cerr << "[SYSTEM] Failed to pin to Core " << core_id << "\n";
    // } else {
    //     std::cout << "[SYSTEM] Thread pinned to Core " << core_id << "\n";
    // }

    struct sched_param param;
    param.sched_priority = 90; 
    rc = pthread_setschedparam(current_thread, SCHED_FIFO, &param);
    
    // if (rc != 0) {
    //     std::cerr << "[SYSTEM] Failed to set SCHED_FIFO: " << strerror(errno) 
    //               << " (Run with sudo!)\n";
    // } else {
    //     std::cout << "[SYSTEM] Real-time priority (SCHED_FIFO, 90) active.\n";
    // }
}

// void loggerThreadFunc() {
//     std::ofstream log_file(CSV_LOG_FILENAME);
//     log_file << "Timestamp_ms,Node_ID,CAN_ID,Direction,Data_Hex,Description\n";
    
//     FastLogEntry entry;
//     char hex_buffer[24]; 
    
//     while (logger_running || fast_log_queue.pop(entry)) {
//         while (fast_log_queue.pop(entry)) {
//             if (entry.data_len > 0) {
//                 for(int i=0; i<entry.data_len; i++) {
//                     sprintf(&hex_buffer[i*3], "%02X ", entry.data[i]);
//                 }
//             } else {
//                 hex_buffer[0] = '\0';
//             }

//             log_file << std::fixed << std::setprecision(3) << (entry.timestamp_us / 1000.0) << ","
//                      << (int)entry.node_id << ","
//                      << "0x" << std::hex << entry.can_id << std::dec << ","
//                      << entry.direction << ","
//                      << hex_buffer << ","
//                      << entry.description << "\n";
//         }
//         std::this_thread::sleep_for(std::chrono::milliseconds(5));
//     }
//     log_file.close();
// }

// void addLog(const char* msg_type, uint8_t node_id, uint32_t can_id, 
//     const char* direction, const uint8_t* data, uint8_t len,
//     const char* desc) {

//     FastLogEntry entry;
//     auto now = std::chrono::steady_clock::now();
//     entry.timestamp_us = std::chrono::duration_cast<std::chrono::microseconds>(
//         now - global_start_time).count();
//     entry.node_id = node_id;
//     entry.can_id = can_id;
//     entry.data_len = (len > 8) ? 8 : len;
//     if (len > 0 && data != nullptr) memcpy(entry.data, data, entry.data_len);
    
//     strncpy(entry.direction, direction, sizeof(entry.direction) - 1);
//     entry.direction[sizeof(entry.direction) - 1] = '\0';

//     strncpy(entry.description, desc, sizeof(entry.description) - 1);
//     entry.description[sizeof(entry.description) - 1] = '\0';
    
//     fast_log_queue.push(entry);
// }

int setupCAN(const char* ifname) {
    int s = socket(PF_CAN, SOCK_RAW, CAN_RAW);
    if (s < 0) return -1;
    
    int flags = fcntl(s, F_GETFL, 0);
    fcntl(s, F_SETFL, flags | O_NONBLOCK);

    int rcvbuf_size = 1048576; 
    setsockopt(s, SOL_SOCKET, SO_RCVBUF, &rcvbuf_size, sizeof(rcvbuf_size));
    
    struct ifreq ifr{};
    strncpy(ifr.ifr_name, ifname, IFNAMSIZ - 1);
    ioctl(s, SIOCGIFINDEX, &ifr);
    
    struct sockaddr_can addr{};
    addr.can_family = AF_CAN;
    addr.can_ifindex = ifr.ifr_ifindex;
    
    if (bind(s, (struct sockaddr*)&addr, sizeof(addr)) < 0) return -1;
    return s;
}

void enableTPDOFilter() {
    struct can_filter filters[3];
    filters[0].can_id = 0x180; filters[0].can_mask = 0x7F0; // TPDO1
    filters[1].can_id = 0x280; filters[1].can_mask = 0x7F0; // TPDO2
    filters[2].can_id = 0x080; filters[2].can_mask = 0x7F0; // EMCY
    setsockopt(can_socket, SOL_CAN_RAW, CAN_RAW_FILTER, filters, sizeof(filters));
}

void clearFilter() {
    setsockopt(can_socket, SOL_CAN_RAW, CAN_RAW_FILTER, nullptr, 0);
}

void sendFrame(uint32_t id, uint8_t len, const uint8_t* d = nullptr) {
    struct can_frame f{};
    f.can_id = id;
    f.can_dlc = len;
    if (d) memcpy(f.data, d, len);

    std::lock_guard<std::mutex> lock(can_mutex);

    int retries = 0;
    while (true) {
        ssize_t nbytes = write(can_socket, &f, sizeof(f));
        
        if (nbytes == sizeof(f)) {
            break; 
        }

        if (nbytes < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ENOBUFS) {
                if (++retries > 50) {
                    std::cerr << "[!] CAN Write Timeout (ID: 0x" << std::hex << id << ")\n";
                    break; 
                }
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                continue;
            } else {
                perror("[!] CAN Write Error");
                break;
            }
        }
    }
}

void sendSYNC() { 
    sendFrame(0x080, 0); 
    // addLog("SYNC", 0, 0x080, "TX", nullptr, 0, "Global Sync");
}

void masterHeartbeatLoop() {
    // std::cout << "[HEARTBEAT] Master heartbeat thread started.\n";
    auto next_wake = std::chrono::steady_clock::now();
    const auto interval = std::chrono::milliseconds(MASTER_HEARTBEAT_INTERVAL_MS);
    uint8_t d[1] = {0x05}; 

    while (running) {
        sendFrame(0x700, 1, d); 
        // addLog("HEARTBEAT", 0, 0x700, "TX", d, 1, "Master Heartbeat");
        next_wake += interval;
        std::this_thread::sleep_until(next_wake);
    }
}

void sendNMT(uint8_t cmd, uint8_t id) { 
    uint8_t d[2] = {cmd, id}; 
    sendFrame(0x000, 2, d);
    // addLog("NMT", id, 0x000, "TX", d, 2, "NMT Command");
}

void sendSDO(uint8_t id, uint8_t cs, uint16_t idx, uint8_t sub, int32_t val) {
    uint8_t d[8]{};
    d[0] = cs; 
    d[1] = idx & 0xFF; 
    d[2] = idx >> 8; 
    d[3] = sub;
    memcpy(&d[4], &val, 4);
    
    sendFrame(0x600 + id, 8, d);
    
    char desc[32];
    snprintf(desc, sizeof(desc), "SDO Wr 0x%04X Sub %d", idx, sub);
    // addLog("SDO", id, 0x600 + id, "TX", d, 8, desc);
}

void sendRPDO_Velocity(uint8_t id, int32_t vel) {
    uint8_t d[4]; 
    memcpy(d, &vel, 4);
    sendFrame(0x200 + id, 4, d);
    
    for (auto& node : nodes) {
        if (node.id == id) {
            node.command_sent_time = std::chrono::steady_clock::now();
            node.waiting_for_response = true;
            break;
        }
    }
}

int processIncomingFrame() {
    struct can_frame f{};
    ssize_t nbytes = read(can_socket, &f, sizeof(f));
    
    if (nbytes <= 0) return 0; 
    
    if ((f.can_id & 0x7F0) == 0x080) { // EMCY
        uint8_t id = f.can_id & 0x00F;
        for (auto& node : nodes) {
            if (node.id == id) {
                node.fault_active = true;
                // addLog("EMCY", id, f.can_id, "RX", f.data, f.can_dlc, "Emergency");
                break;
            }
        }
    }
    else if ((f.can_id & 0x7F0) == 0x180) { // TPDO1
        uint8_t id = f.can_id & 0x00F;
        for (auto& node : nodes) {
            if (node.id == id && f.can_dlc == 4) {
                auto now = std::chrono::steady_clock::now();
                memcpy(&node.actual_velocity, f.data, 4);
                
                node.tpdo1_received = true;
                node.tpdo1_receive_count++;
                if (node.waiting_for_response) {
                    std::chrono::duration<double, std::milli> diff = now - node.command_sent_time;
                    node.last_response_time_ms = diff.count();
                    node.waiting_for_response = false;
                }
                // addLog("TPDO1", id, f.can_id, "RX", f.data, 4, "Velocity Feedback");
                break;
            }
        }
    }
    else if ((f.can_id & 0x7F0) == 0x280) { // TPDO2
        uint8_t id = f.can_id & 0x00F;
        for (auto& node : nodes) {
            if (node.id == id && f.can_dlc >= 2) {
                node.statusword = f.data[0] | (f.data[1] << 8);
                node.tpdo2_received = true;
                node.tpdo2_receive_count++;
                node.last_heartbeat_time = std::chrono::steady_clock::now();
                node.heartbeat_timeout = false; 
                // addLog("TPDO2", id, f.can_id, "RX", f.data, f.can_dlc, "Statusword/Heartbeat");
                break;
            }
        }
    }
    return 1; 
}

void initSingleNode(NodeControl& node) {
    // std::cout << "   -> Init Node " << (int)node.id << "...\n";
    sendNMT(0x81, node.id); delay_ms(150); 
    sendNMT(0x80, node.id); delay_ms(150); 
    uint32_t master_hb_config = (static_cast<uint32_t>(HEARTBEAT_TIMEOUT_MS) << 16) | 0x00;
    sendSDO(node.id, 0x23, 0x1016, 0x01, master_hb_config); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x6060, 0x00, 0xFD); delay_ms(20);

    /*MAPPING RPDO1: Velocity Only (0x60FF sub 0)*/
    sendSDO(node.id, 0x23, 0x1400, 0x01, 0x80000200 + node.id); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1600, 0x00, 0x00); delay_ms(20);
    sendSDO(node.id, 0x23, 0x1600, 0x01, 0x60FF0020); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1600, 0x00, 0x01); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1400, 0x02, 0x01); delay_ms(20);
    sendSDO(node.id, 0x23, 0x1400, 0x01, 0x0200 + node.id); delay_ms(20);

    /* MAPPING TPDO1: Velocity Feedback*/
    sendSDO(node.id, 0x23, 0x1800, 0x01, 0x80000180 + node.id); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1A00, 0x00, 0x00); delay_ms(20);
    sendSDO(node.id, 0x23, 0x1A00, 0x01, 0x606C0020); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1A00, 0x00, 0x01); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1800, 0x02, TPDO1_SYNC_INTERVAL); delay_ms(20);
    sendSDO(node.id, 0x23, 0x1800, 0x01, 0x0180 + node.id); delay_ms(20);

    // MAPPING TPDO2: Statusword (Heartbeat)
    sendSDO(node.id, 0x23, 0x1801, 0x01, 0x80000280 + node.id); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1A01, 0x00, 0x00); delay_ms(20);
    sendSDO(node.id, 0x23, 0x1A01, 0x01, 0x60410010); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1A01, 0x00, 0x01); delay_ms(20);
    sendSDO(node.id, 0x2F, 0x1801, 0x02, TPDO2_SYNC_INTERVAL); delay_ms(20);
    sendSDO(node.id, 0x2B, 0x1801, 0x05, 0x00); delay_ms(20);
    sendSDO(node.id, 0x23, 0x1801, 0x01, 0x0280 + node.id); delay_ms(20);
    
    sendNMT(0x01, node.id); delay_ms(100); 

    sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x0006); delay_ms(50); 
    sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x0007); delay_ms(50); 
    sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x000F); delay_ms(50); 
}

void initAllNodes() {
    clearFilter();
    // std::cout << "[INIT] Initializing All 6 Nodes...\n";
    nodes.reserve(6); 
    nodes.emplace_back(2, STEERING_MOTOR);
    nodes.emplace_back(4, STEERING_MOTOR);
    nodes.emplace_back(6, STEERING_MOTOR);

    nodes.emplace_back(1, TRAVEL_MOTOR);
    nodes.emplace_back(3, TRAVEL_MOTOR);
    nodes.emplace_back(5, TRAVEL_MOTOR);

    for (auto& node : nodes) {
        struct can_frame f{};
        while (read(can_socket, &f, sizeof(f)) > 0) {} 
        initSingleNode(node);
        delay_ms(200); 
    }
    
    enableTPDOFilter();
    // std::cout << "[INIT] All Nodes Ready.\n";
}

void checkNodeHeartbeats() {
    auto now = std::chrono::steady_clock::now();
    for (auto& node : nodes) {
        auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - node.last_heartbeat_time).count();
        if (elapsed > HEARTBEAT_TIMEOUT_MS && !node.heartbeat_timeout) {
            node.heartbeat_timeout = true;
            // addLog("ERROR", node.id, 0, "ERR", nullptr, 0, "Heartbeat Timeout");
        }
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    global_start_time = std::chrono::steady_clock::now();
    
    // logger_running = true;
    // logger_thread = std::thread(loggerThreadFunc);
    // std::cout << "[LOGGING] CSV logging started - writing to " << CSV_LOG_FILENAME << "\n";

    can_socket = setupCAN(argc > 1 ? argv[1] : CAN_INTERFACE);
    if (can_socket < 0) {
        // std::cerr << "[ERROR] Failed to setup CAN interface\n";
        return 1;
    }

    heartbeat_thread = std::thread(masterHeartbeatLoop);

    initAllNodes();
    pinThisThreadToCore(8); 

    auto next = std::chrono::steady_clock::now();
    const auto period = std::chrono::milliseconds(CYCLE_PERIOD_MS);

    // std::cout << "[LOOP] Starting Loop on Core 8...\n";

    int loop_counter = 0;

    while (running) {
        for (auto& node : nodes) {
            node.tpdo1_received = false;
            node.tpdo2_received = false;
            node.tpdo2_expected_this_cycle = (sync_counter % 20 == 0);
        }

        while (processIncomingFrame() > 0) {}

        for (auto& node : nodes) {
            if (node.fault_active || (node.tpdo2_received && (node.statusword & SW_FAULT))) {
                if (!node.fault_active) {
                    //    addLog("ERROR", node.id, 0, "ERR", nullptr, 0, "Node Fault Detected");
                       node.fault_active = true;
                }
                if (!node.is_recovering) {
                    node.is_recovering = true;
                    node.recovery_step = 0;
                    node.recovery_timer = 0;
                }
            }
            else if (node.tpdo2_received && 
                    (node.statusword & SW_TARGET_REACHED) && 
                    !(node.last_statusword & SW_TARGET_REACHED)) {
                
                node.current_target_velocity = (node.current_target_velocity == node.velocity_forward) 
                                                                              ? node.velocity_reverse 
                                                                              : node.velocity_forward;
            }
            if (node.tpdo2_received) node.last_statusword = node.statusword;
        }

        checkNodeHeartbeats();

        for (auto& node : nodes) {
            if (node.is_recovering) {
                node.recovery_timer++;
                if (node.recovery_timer >= 50) { 
                    node.recovery_timer = 0;
                    if (node.recovery_step == 0) {
                        sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x0080); 
                        node.recovery_step++;
                    } else if (node.recovery_step == 1) {
                        sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x0006); 
                        node.recovery_step++;
                    } else if (node.recovery_step == 2) {
                        sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x000F); 
                        node.recovery_step++;
                    } else {

                        node.fault_active = false;
                        node.heartbeat_timeout = false;
                        node.is_recovering = false;
                        node.last_heartbeat_time = std::chrono::steady_clock::now();
                        // addLog("INFO", node.id, 0, "SYS", nullptr, 0, "Node Recovered");
                    }
                }
                /* Send 0 velocity while recovering*/
                sendRPDO_Velocity(node.id, 0);
            } else {
                /* Normal Operation*/
                sendRPDO_Velocity(node.id, node.current_target_velocity);
            }
        }

        sendSYNC();
        sync_counter++;
        
        delay_us(SYNC_PROCESSING_DELAY_US);

        while (processIncomingFrame() > 0) {}

        for (auto& node : nodes) {
            if (node.tpdo1_expected_this_cycle && !node.tpdo1_received) node.tpdo1_missed_count++;
        }
        
        // if (loop_counter % 500 == 0 && loop_counter > 0) {
        //     std::cout << "\n[STATUS] Cycle " << loop_counter << "\n";
        //     for (const auto& node : nodes) {
        //         std::cout << "  Node " << (int)node.id 
        //                   << " | TPDO1: " << node.tpdo1_receive_count 
        //                   << " | TPDO2(HB): " << node.tpdo2_receive_count
        //                   << " | Recovering: " << (node.is_recovering ? "YES" : "NO")
        //                   << "\n";
        //     }
        // }

        next += period;
        std::this_thread::sleep_until(next);
        loop_counter++;
    }

    // std::cout << "\n Disabling motors \n";
    clearFilter();
    
    for (auto& node : nodes) {
        sendRPDO_Velocity(node.id, 0);
        sendSDO(node.id, 0x2B, 0x6040, 0x00, 0x0006); 
    }
    sendSYNC(); 
    
    sendNMT(0x80, 0x00); 
    delay_ms(20);

    running = false;
    if(heartbeat_thread.joinable()) heartbeat_thread.join();
    // if(logger_thread.joinable()) {
    //     logger_running = false;
    //     logger_thread.join();
    // }
    close(can_socket);
    
    return 0;
}