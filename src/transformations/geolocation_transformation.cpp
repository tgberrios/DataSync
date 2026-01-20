#include "transformations/geolocation_transformation.h"
#include "core/logger.h"
#include <sstream>
#include <algorithm>

const double EARTH_RADIUS_KM = 6371.0;

GeolocationTransformation::GeolocationTransformation() = default;

bool GeolocationTransformation::validateConfig(const json& config) const {
  if (!config.contains("operation") || !config["operation"].is_string()) {
    Logger::error(LogCategory::TRANSFER, "GeolocationTransformation",
                  "Missing or invalid operation in config");
    return false;
  }
  
  std::string operation = config["operation"];
  std::vector<std::string> validOperations = {
    "distance", "point_in_polygon", "bounding_box"
  };
  
  if (std::find(validOperations.begin(), validOperations.end(), operation) == 
      validOperations.end()) {
    Logger::error(LogCategory::TRANSFER, "GeolocationTransformation",
                  "Invalid operation: " + operation);
    return false;
  }
  
  if (operation == "distance") {
    if (!config.contains("point1_column") || !config.contains("point2_column")) {
      Logger::error(LogCategory::TRANSFER, "GeolocationTransformation",
                    "distance operation requires point1_column and point2_column");
      return false;
    }
  } else if (operation == "point_in_polygon") {
    if (!config.contains("point_column") || !config.contains("polygon")) {
      Logger::error(LogCategory::TRANSFER, "GeolocationTransformation",
                    "point_in_polygon operation requires point_column and polygon");
      return false;
    }
  }
  
  return true;
}

std::vector<json> GeolocationTransformation::execute(
  const std::vector<json>& inputData,
  const json& config
) {
  if (inputData.empty()) {
    return inputData;
  }
  
  if (!validateConfig(config)) {
    Logger::error(LogCategory::TRANSFER, "GeolocationTransformation",
                  "Invalid config, returning input data unchanged");
    return inputData;
  }
  
  std::string operation = config["operation"];
  std::string targetColumn = config.value("target_column", "geolocation_result");
  
  std::vector<json> result;
  result.reserve(inputData.size());
  
  for (const auto& row : inputData) {
    json outputRow = row;
    
    if (operation == "distance") {
      std::string point1Col = config["point1_column"];
      std::string point2Col = config["point2_column"];
      
      if (row.contains(point1Col) && row.contains(point2Col)) {
        Point point1 = parsePoint(row[point1Col]);
        Point point2 = parsePoint(row[point2Col]);
        double distance = calculateDistance(
          point1.latitude, point1.longitude,
          point2.latitude, point2.longitude
        );
        outputRow[targetColumn] = distance;
      } else {
        outputRow[targetColumn] = json(nullptr);
      }
    } else if (operation == "point_in_polygon") {
      std::string pointCol = config["point_column"];
      std::vector<Point> polygon = parsePolygon(config["polygon"]);
      
      if (row.contains(pointCol) && !polygon.empty()) {
        Point point = parsePoint(row[pointCol]);
        bool inside = isPointInPolygon(point, polygon);
        outputRow[targetColumn] = inside;
      } else {
        outputRow[targetColumn] = json(nullptr);
      }
    } else if (operation == "bounding_box") {
      // Calculate bounding box for a set of points
      // Implementation would go here
    }
    
    result.push_back(outputRow);
  }
  
  Logger::info(LogCategory::TRANSFER, "GeolocationTransformation",
               "Applied " + operation + " to " + std::to_string(inputData.size()) + " rows");
  
  return result;
}

double GeolocationTransformation::calculateDistance(
  double lat1, double lon1,
  double lat2, double lon2
) {
  double dLat = toRadians(lat2 - lat1);
  double dLon = toRadians(lon2 - lon1);
  
  double a = std::sin(dLat / 2) * std::sin(dLat / 2) +
             std::cos(toRadians(lat1)) * std::cos(toRadians(lat2)) *
             std::sin(dLon / 2) * std::sin(dLon / 2);
  
  double c = 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
  
  return EARTH_RADIUS_KM * c;
}

bool GeolocationTransformation::isPointInPolygon(
  const Point& point,
  const std::vector<Point>& polygon
) {
  if (polygon.size() < 3) {
    return false;
  }
  
  bool inside = false;
  size_t j = polygon.size() - 1;
  
  for (size_t i = 0; i < polygon.size(); ++i) {
    if (((polygon[i].longitude > point.longitude) != (polygon[j].longitude > point.longitude)) &&
        (point.latitude < (polygon[j].latitude - polygon[i].latitude) * 
         (point.longitude - polygon[i].longitude) / 
         (polygon[j].longitude - polygon[i].longitude) + polygon[i].latitude)) {
      inside = !inside;
    }
    j = i;
  }
  
  return inside;
}

GeolocationTransformation::Point GeolocationTransformation::parsePoint(const json& pointData) {
  Point point = {0.0, 0.0};
  
  if (pointData.is_object()) {
    if (pointData.contains("latitude") && pointData.contains("longitude")) {
      point.latitude = pointData["latitude"].is_number() ? 
                       pointData["latitude"].get<double>() : 0.0;
      point.longitude = pointData["longitude"].is_number() ? 
                        pointData["longitude"].get<double>() : 0.0;
    } else if (pointData.contains("lat") && pointData.contains("lng")) {
      point.latitude = pointData["lat"].is_number() ? 
                       pointData["lat"].get<double>() : 0.0;
      point.longitude = pointData["lng"].is_number() ? 
                        pointData["lng"].get<double>() : 0.0;
    }
  } else if (pointData.is_array() && pointData.size() >= 2) {
    point.longitude = pointData[0].is_number() ? pointData[0].get<double>() : 0.0;
    point.latitude = pointData[1].is_number() ? pointData[1].get<double>() : 0.0;
  } else if (pointData.is_string()) {
    // Try to parse as "lat,lng" or "latitude,longitude"
    std::string str = pointData.get<std::string>();
    size_t comma = str.find(',');
    if (comma != std::string::npos) {
      try {
        point.latitude = std::stod(str.substr(0, comma));
        point.longitude = std::stod(str.substr(comma + 1));
      } catch (...) {
        Logger::warning(LogCategory::TRANSFER, "GeolocationTransformation",
                        "Failed to parse point string: " + str);
      }
    }
  }
  
  return point;
}

std::vector<GeolocationTransformation::Point> GeolocationTransformation::parsePolygon(
  const json& polygonData
) {
  std::vector<Point> polygon;
  
  if (polygonData.is_array()) {
    for (const auto& pointData : polygonData) {
      polygon.push_back(parsePoint(pointData));
    }
  }
  
  return polygon;
}

double GeolocationTransformation::toRadians(double degrees) {
  return degrees * M_PI / 180.0;
}
