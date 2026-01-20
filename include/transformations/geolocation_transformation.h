#ifndef GEOLOCATION_TRANSFORMATION_H
#define GEOLOCATION_TRANSFORMATION_H

#include "transformation_engine.h"
#include <string>
#include <cmath>

// Geolocation transformation: Distance calculations and polygon operations
class GeolocationTransformation : public Transformation {
public:
  struct Point {
    double latitude;
    double longitude;
  };
  
  GeolocationTransformation();
  ~GeolocationTransformation() override = default;
  
  std::vector<json> execute(
    const std::vector<json>& inputData,
    const json& config
  ) override;
  
  std::string getType() const override { return "geolocation"; }
  
  bool validateConfig(const json& config) const override;

private:
  // Calculate distance between two points (Haversine formula)
  double calculateDistance(
    double lat1, double lon1,
    double lat2, double lon2
  );
  
  // Check if point is inside polygon
  bool isPointInPolygon(
    const Point& point,
    const std::vector<Point>& polygon
  );
  
  // Parse point from JSON
  Point parsePoint(const json& pointData);
  
  // Parse polygon from JSON
  std::vector<Point> parsePolygon(const json& polygonData);
  
  // Convert degrees to radians
  double toRadians(double degrees);
};

#endif // GEOLOCATION_TRANSFORMATION_H
