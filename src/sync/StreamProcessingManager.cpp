#include "sync/StreamProcessingManager.h"
#include "core/logger.h"
#include <sstream>
#include <iomanip>
#include <chrono>
#include <random>

StreamProcessingManager::StreamProcessingManager(const StreamConfig& config)
    : defaultConfig_(config) {
  Logger::info(LogCategory::SYSTEM, "StreamProcessingManager",
               "Initializing StreamProcessingManager");
}

StreamProcessingManager::~StreamProcessingManager() {
  shutdown();
}

bool StreamProcessingManager::initialize() {
  if (initialized_) {
    Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                    "Already initialized");
    return true;
  }

  // Crear engines según configuración
  if (defaultConfig_.streamType == StreamType::KAFKA || 
      defaultConfig_.engineConfig.contains("kafka")) {
    json kafkaConfig = defaultConfig_.engineConfig.value("kafka", json::object());
    if (!createEngine(StreamType::KAFKA, kafkaConfig)) {
      Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                      "Failed to create Kafka engine");
    }
  }

  if (defaultConfig_.streamType == StreamType::RABBITMQ ||
      defaultConfig_.engineConfig.contains("rabbitmq")) {
    json rabbitmqConfig = defaultConfig_.engineConfig.value("rabbitmq", json::object());
    if (!createEngine(StreamType::RABBITMQ, rabbitmqConfig)) {
      Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                      "Failed to create RabbitMQ engine");
    }
  }

  if (defaultConfig_.streamType == StreamType::REDIS_STREAMS ||
      defaultConfig_.engineConfig.contains("redis")) {
    json redisConfig = defaultConfig_.engineConfig.value("redis", json::object());
    if (!createEngine(StreamType::REDIS_STREAMS, redisConfig)) {
      Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                      "Failed to create Redis Streams engine");
    }
  }

  initialized_ = true;
  Logger::info(LogCategory::SYSTEM, "StreamProcessingManager",
               "StreamProcessingManager initialized successfully");
  return true;
}

void StreamProcessingManager::shutdown() {
  if (!initialized_) {
    return;
  }

  running_ = false;

  // Detener todos los consumidores
  std::lock_guard<std::mutex> lock(consumersMutex_);
  for (auto& [consumerId, consumer] : consumers_) {
    if (consumer.isRunning) {
      stopConsumer(consumerId);
    }
  }

  // Esperar a que terminen los threads
  for (auto& [consumerId, thread] : consumerThreads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  // Cerrar engines
  if (kafkaEngine_) {
    kafkaEngine_->shutdown();
    kafkaEngine_.reset();
  }
  if (rabbitMQEngine_) {
    rabbitMQEngine_->shutdown();
    rabbitMQEngine_.reset();
  }
  if (redisEngine_) {
    redisEngine_->shutdown();
    redisEngine_.reset();
  }

  // Cerrar transformation engine
  transformationEngine_.reset();

  initialized_ = false;
  Logger::info(LogCategory::SYSTEM, "StreamProcessingManager",
               "StreamProcessingManager shutdown");
}

std::string StreamProcessingManager::startConsumer(
    const StreamConfig& config,
    std::function<bool(const StreamMessage&)> messageHandler) {
  if (!initialized_) {
    Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                 "Cannot start consumer: manager not initialized");
    return "";
  }

  if (!validateConfig(config)) {
    Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                 "Invalid stream configuration");
    return "";
  }

  std::string consumerId = generateConsumerId();

  // Crear información del consumidor
  ConsumerInfo consumerInfo;
  consumerInfo.consumerId = consumerId;
  consumerInfo.consumerGroup = config.consumerGroup;
  consumerInfo.consumerName = config.consumerName;
  consumerInfo.streamType = config.streamType;
  consumerInfo.isRunning = true;
  consumerInfo.stats.startTime = std::chrono::system_clock::now();

  // Determinar source según tipo
  switch (config.streamType) {
    case StreamType::KAFKA:
      consumerInfo.source = config.topic;
      break;
    case StreamType::RABBITMQ:
      consumerInfo.source = config.queue;
      break;
    case StreamType::REDIS_STREAMS:
      consumerInfo.source = config.stream;
      break;
  }

  // Iniciar thread de procesamiento
  std::thread consumerThread([this, consumerId, config, messageHandler]() {
    processStream(consumerId, messageHandler);
  });

  consumerInfo.threadId = consumerThread.get_id();

  // Guardar consumidor
  {
    std::lock_guard<std::mutex> lock(consumersMutex_);
    consumers_[consumerId] = consumerInfo;
    consumerThreads_[consumerId] = std::move(consumerThread);
  }

  Logger::info(LogCategory::SYSTEM, "StreamProcessingManager",
               "Consumer started: " + consumerId);
  return consumerId;
}

bool StreamProcessingManager::stopConsumer(const std::string& consumerId) {
  std::lock_guard<std::mutex> lock(consumersMutex_);
  
  auto it = consumers_.find(consumerId);
  if (it == consumers_.end()) {
    Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                    "Consumer not found: " + consumerId);
    return false;
  }

  it->second.isRunning = false;

  // Esperar a que termine el thread
  auto threadIt = consumerThreads_.find(consumerId);
  if (threadIt != consumerThreads_.end() && threadIt->second.joinable()) {
    threadIt->second.join();
    consumerThreads_.erase(threadIt);
  }

  consumers_.erase(it);

  Logger::info(LogCategory::SYSTEM, "StreamProcessingManager",
               "Consumer stopped: " + consumerId);
  return true;
}

void StreamProcessingManager::processStream(
    const std::string& consumerId,
    std::function<bool(const StreamMessage&)> messageHandler) {
  
  std::lock_guard<std::mutex> lock(consumersMutex_);
  auto it = consumers_.find(consumerId);
  if (it == consumers_.end()) {
    Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                 "Consumer not found: " + consumerId);
    return;
  }

  ConsumerInfo& consumer = it->second;
  StreamType streamType = consumer.streamType;

  // Procesar según tipo de stream
  while (consumer.isRunning && running_) {
    try {
      std::vector<StreamMessage> messages;

      switch (streamType) {
        case StreamType::KAFKA:
          if (kafkaEngine_ && kafkaEngine_->isAvailable()) {
            auto kafkaMessages = kafkaEngine_->pollMessages(1000, 100);
            for (const auto& msg : kafkaMessages) {
              messages.push_back(convertKafkaMessage(msg));
            }
          }
          break;

        case StreamType::RABBITMQ:
          if (rabbitMQEngine_ && rabbitMQEngine_->isAvailable()) {
            // RabbitMQ usa callback, así que procesamos directamente
            rabbitMQEngine_->consumeMessages(consumer.source,
              [&](const RabbitMQEngine::RabbitMQMessage& msg) {
                StreamMessage streamMsg = convertRabbitMQMessage(msg);
                if (!messageHandler(streamMsg)) {
                  return false;  // Detener consumo
                }
                consumer.stats.messagesProcessed++;
                consumer.stats.lastMessageTime = std::chrono::system_clock::now();
                return true;
              });
            // Si consumeMessages retorna, el consumidor se detuvo
            consumer.isRunning = false;
            break;
          }
          break;

        case StreamType::REDIS_STREAMS:
          if (redisEngine_ && redisEngine_->isAvailable()) {
            std::vector<std::string> streams = {consumer.source};
            std::vector<std::string> ids = {"$"};  // Solo nuevos mensajes
            auto results = redisEngine_->xreadgroup(
              consumer.consumerGroup, consumer.consumerName,
              streams, ids, 1000, 100);
            
            for (const auto& result : results) {
              for (const auto& entry : result.entries) {
                messages.push_back(convertRedisMessage(entry, result.streamName));
              }
            }
          }
          break;
      }

      // Procesar mensajes
      for (const auto& msg : messages) {
        StreamMessage processedMsg = msg;

        // Aplicar transformaciones si están configuradas
        if (transformationEngine_ && defaultConfig_.engineConfig.contains("transformations")) {
          try {
            json msgJson = json::parse(msg.value);
            std::vector<json> inputData = {msgJson};
            json transformations = defaultConfig_.engineConfig["transformations"];
            
            std::vector<json> transformedData = 
                transformationEngine_->executePipeline(inputData, transformations);
            
            if (!transformedData.empty()) {
              processedMsg.value = transformedData[0].dump();
            }
          } catch (const std::exception& e) {
            Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                           "Error applying transformations: " + std::string(e.what()));
          }
        }

        if (!messageHandler(processedMsg)) {
          consumer.isRunning = false;
          break;
        }

        consumer.stats.messagesProcessed++;
        consumer.stats.bytesProcessed += processedMsg.value.size();
        consumer.stats.lastMessageTime = std::chrono::system_clock::now();

        // Commit offset para Kafka
        if (streamType == StreamType::KAFKA && kafkaEngine_) {
          kafkaEngine_->commitOffsets();
        }

        // ACK para Redis
        if (streamType == StreamType::REDIS_STREAMS && redisEngine_) {
          std::vector<std::string> ids = {msg.id};
          redisEngine_->xack(consumer.source, consumer.consumerGroup, ids);
        }
      }

    } catch (const std::exception& e) {
      consumer.stats.errors++;
      consumer.stats.messagesFailed++;
      Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                   "Error processing stream: " + std::string(e.what()));
      
      // Esperar un poco antes de reintentar
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  }

  consumer.isRunning = false;
  Logger::info(LogCategory::SYSTEM, "StreamProcessingManager",
               "Consumer thread finished: " + consumerId);
}

StreamProcessingManager::StreamStats StreamProcessingManager::getStreamStats(
    const std::string& consumerId) const {
  std::lock_guard<std::mutex> lock(consumersMutex_);
  
  auto it = consumers_.find(consumerId);
  if (it == consumers_.end()) {
    return StreamStats{};
  }

  return it->second.stats;
}

std::vector<StreamProcessingManager::ConsumerInfo> StreamProcessingManager::getConsumers() const {
  std::lock_guard<std::mutex> lock(consumersMutex_);
  
  std::vector<ConsumerInfo> result;
  for (const auto& [id, consumer] : consumers_) {
    result.push_back(consumer);
  }
  return result;
}

bool StreamProcessingManager::isKafkaAvailable() const {
  return kafkaEngine_ && kafkaEngine_->isAvailable();
}

bool StreamProcessingManager::isRabbitMQAvailable() const {
  return rabbitMQEngine_ && rabbitMQEngine_->isAvailable();
}

bool StreamProcessingManager::isRedisAvailable() const {
  return redisEngine_ && redisEngine_->isAvailable();
}

bool StreamProcessingManager::createEngine(StreamType type, const json& engineConfig) {
  switch (type) {
    case StreamType::KAFKA: {
      KafkaEngine::KafkaConfig kafkaConfig;
      if (engineConfig.contains("brokers")) {
        kafkaConfig.brokers = engineConfig["brokers"];
      }
      if (engineConfig.contains("clientId")) {
        kafkaConfig.clientId = engineConfig["clientId"];
      }
      if (engineConfig.contains("groupId")) {
        kafkaConfig.groupId = engineConfig["groupId"];
      }
      if (engineConfig.contains("securityProtocol")) {
        kafkaConfig.securityProtocol = engineConfig["securityProtocol"];
      }
      if (engineConfig.contains("saslMechanism")) {
        kafkaConfig.saslMechanism = engineConfig["saslMechanism"];
      }
      if (engineConfig.contains("saslUsername")) {
        kafkaConfig.saslUsername = engineConfig["saslUsername"];
      }
      if (engineConfig.contains("saslPassword")) {
        kafkaConfig.saslPassword = engineConfig["saslPassword"];
      }
      
      kafkaEngine_ = std::make_unique<KafkaEngine>(kafkaConfig);
      return kafkaEngine_->initialize();
    }

    case StreamType::RABBITMQ: {
      RabbitMQEngine::RabbitMQConfig rabbitmqConfig;
      if (engineConfig.contains("host")) {
        rabbitmqConfig.host = engineConfig["host"];
      }
      if (engineConfig.contains("port")) {
        rabbitmqConfig.port = engineConfig["port"];
      }
      if (engineConfig.contains("vhost")) {
        rabbitmqConfig.vhost = engineConfig["vhost"];
      }
      if (engineConfig.contains("username")) {
        rabbitmqConfig.username = engineConfig["username"];
      }
      if (engineConfig.contains("password")) {
        rabbitmqConfig.password = engineConfig["password"];
      }
      
      rabbitMQEngine_ = std::make_unique<RabbitMQEngine>(rabbitmqConfig);
      return rabbitMQEngine_->initialize();
    }

    case StreamType::REDIS_STREAMS: {
      RedisStreamsEngine::RedisStreamsConfig redisConfig;
      if (engineConfig.contains("host")) {
        redisConfig.host = engineConfig["host"];
      }
      if (engineConfig.contains("port")) {
        redisConfig.port = engineConfig["port"];
      }
      if (engineConfig.contains("password")) {
        redisConfig.password = engineConfig["password"];
      }
      if (engineConfig.contains("streamName")) {
        redisConfig.streamName = engineConfig["streamName"];
      }
      if (engineConfig.contains("consumerGroup")) {
        redisConfig.consumerGroup = engineConfig["consumerGroup"];
      }
      if (engineConfig.contains("consumerName")) {
        redisConfig.consumerName = engineConfig["consumerName"];
      }
      
      redisEngine_ = std::make_unique<RedisStreamsEngine>(redisConfig);
      return redisEngine_->initialize();
    }
  }

  return false;
}

StreamProcessingManager::StreamMessage StreamProcessingManager::convertKafkaMessage(
    const KafkaEngine::KafkaMessage& msg) {
  StreamMessage streamMsg;
  streamMsg.id = std::to_string(msg.offset);
  streamMsg.key = msg.key;
  streamMsg.value = msg.value;
  streamMsg.headers = msg.headers;
  streamMsg.timestamp = msg.timestamp;
  streamMsg.source = msg.topic;
  streamMsg.metadata["partition"] = msg.partition;
  streamMsg.metadata["offset"] = msg.offset;
  return streamMsg;
}

StreamProcessingManager::StreamMessage StreamProcessingManager::convertRabbitMQMessage(
    const RabbitMQEngine::RabbitMQMessage& msg) {
  StreamMessage streamMsg;
  streamMsg.id = msg.deliveryTag;
  streamMsg.value = msg.body;
  streamMsg.headers = msg.headers;
  streamMsg.source = msg.exchange;
  streamMsg.metadata["routingKey"] = msg.routingKey;
  streamMsg.metadata["redelivered"] = msg.redelivered;
  return streamMsg;
}

StreamProcessingManager::StreamMessage StreamProcessingManager::convertRedisMessage(
    const RedisStreamsEngine::StreamEntry& entry,
    const std::string& streamName) {
  StreamMessage streamMsg;
  streamMsg.id = entry.id;
  streamMsg.source = streamName;
  
  // Convertir fields a value (asumiendo JSON)
  json valueJson;
  for (const auto& [key, value] : entry.fields) {
    valueJson[key] = value;
  }
  streamMsg.value = valueJson.dump();
  
  return streamMsg;
}

bool StreamProcessingManager::validateConfig(const StreamConfig& config) const {
  switch (config.streamType) {
    case StreamType::KAFKA:
      if (config.topic.empty()) {
        Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                     "Kafka topic cannot be empty");
        return false;
      }
      break;
    case StreamType::RABBITMQ:
      if (config.queue.empty()) {
        Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                     "RabbitMQ queue cannot be empty");
        return false;
      }
      break;
    case StreamType::REDIS_STREAMS:
      if (config.stream.empty()) {
        Logger::error(LogCategory::SYSTEM, "StreamProcessingManager",
                     "Redis stream name cannot be empty");
        return false;
      }
      break;
  }

  if (config.consumerGroup.empty()) {
    Logger::warning(LogCategory::SYSTEM, "StreamProcessingManager",
                    "Consumer group not specified");
  }

  return true;
}

std::string StreamProcessingManager::generateConsumerId() const {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::duration_cast<std::chrono::milliseconds>(
      now.time_since_epoch()).count();
  
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 9999);
  
  std::stringstream ss;
  ss << "consumer_" << time << "_" << std::setfill('0') << std::setw(4) << dis(gen);
  return ss.str();
}
