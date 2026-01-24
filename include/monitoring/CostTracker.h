#ifndef COST_TRACKER_H
#define COST_TRACKER_H

#include "monitoring/ResourceTracker.h"
#include "core/logger.h"
#include "third_party/json.hpp"
#include <chrono>
#include <string>
#include <vector>

using json = nlohmann::json;

// CostTracker: Tracking de costos por operación y workflow
class CostTracker {
public:
  struct CostRecord {
    std::string id;
    std::string workflowId;
    std::string operationName;
    double computeCost;
    double storageCost;
    double networkCost;
    double totalCost;
    std::string currency;
    std::chrono::system_clock::time_point timestamp;
    json breakdown;
  };

  struct Budget {
    std::string id;
    std::string name;
    std::string scope; // "global", "workflow", "project"
    std::string scopeId;
    double amount;
    std::string period; // "daily", "weekly", "monthly"
    double currentSpend;
    std::chrono::system_clock::time_point periodStart;
    bool alertOnExceed;
    double alertThreshold; // percentage
  };

  struct CostEstimate {
    std::string resourceType; // "s3", "rds", "compute"
    double estimatedCost;
    std::string currency;
    std::string period; // "monthly", "yearly"
    json details;
  };

  explicit CostTracker(const std::string& connectionString);
  ~CostTracker() = default;

  // Calcular costo de operación basado en recursos
  CostRecord calculateOperationCost(const std::string& workflowId,
                                    const std::string& operationName,
                                    const ResourceTracker::ResourceMetrics& metrics);

  // Guardar costo
  bool saveCost(const CostRecord& cost);

  // Obtener resumen de costos
  json getCostSummary(const std::string& workflowId = "", int days = 30);

  // Obtener costos por workflow
  std::vector<CostRecord> getCostsByWorkflow(const std::string& workflowId, int days = 30);

  // Crear/actualizar budget
  bool setBudget(const Budget& budget);

  // Obtener budgets
  std::vector<Budget> getBudgets();

  // Verificar si se excede budget
  bool exceedsBudget(const std::string& budgetId);

  // Estimar costos de cloud resources
  std::vector<CostEstimate> estimateCloudCosts();

private:
  std::string connectionString_;
  std::unique_ptr<ResourceTracker> resourceTracker_;

  double calculateComputeCost(const ResourceTracker::ResourceMetrics& metrics);
  double calculateStorageCost(const ResourceTracker::ResourceMetrics& metrics);
  double calculateNetworkCost(const ResourceTracker::ResourceMetrics& metrics);
  bool saveBudgetToDatabase(const Budget& budget);
  bool saveEstimateToDatabase(const CostEstimate& estimate);
};

#endif // COST_TRACKER_H
