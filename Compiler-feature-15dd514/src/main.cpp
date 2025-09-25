#include "StreamingData.h"
#include <iostream>

int main() {
  Logger::initialize();
  Logger::info("MAIN", "Starting DataLake Synchronizer System :) ");

  std::cout << "Running... :)" << std::endl;

  StreamingData sd;
  sd.initialize();
  sd.run();

  return 0;
}
