#ifndef LINEAGE_GRAPH_BUILDER_H
#define LINEAGE_GRAPH_BUILDER_H

#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>
#include <map>
#include <set>

using json = nlohmann::json;

// LineageGraphBuilder: Construye grafo unificado de lineage desde múltiples fuentes
class LineageGraphBuilder {
public:
  struct GraphNode {
    std::string id;
    std::string type;  // "table", "column", "transformation", "workflow"
    std::string label;
    std::string schema;
    std::string table;
    std::string column;
    std::string dbEngine;
    json metadata;
  };

  struct GraphEdge {
    std::string id;
    std::string sourceId;
    std::string targetId;
    std::string type;  // "sync", "transform", "join", "aggregate", etc.
    std::string label;
    double confidence{1.0};
    json metadata;
  };

  struct Graph {
    std::vector<GraphNode> nodes;
    std::vector<GraphEdge> edges;
    json metadata;
  };

  explicit LineageGraphBuilder(const std::string& connectionString);
  ~LineageGraphBuilder() = default;

  // Construir grafo completo desde todas las fuentes de lineage
  Graph buildCompleteGraph(
      const std::vector<std::string>& dbEngines = {},
      const std::vector<std::string>& schemas = {}
  );

  // Construir grafo para un recurso específico
  Graph buildGraphForResource(
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName = "",
      int maxDepth = 5
  );

  // Construir grafo desde workflows
  Graph buildGraphFromWorkflows(
      const std::vector<std::string>& workflowNames = {}
  );

  // Agregar nodos y edges desde lineage de base de datos
  void addDatabaseLineage(Graph& graph, const std::string& dbEngine);

  // Agregar nodos y edges desde transformation lineage
  void addTransformationLineage(Graph& graph);

  // Agregar nodos y edges desde workflows
  void addWorkflowLineage(Graph& graph);

private:
  std::string connectionString_;

  // Helper methods
  std::string generateNodeId(const std::string& type,
                             const std::string& schema,
                             const std::string& table,
                             const std::string& column = "");

  GraphNode createNode(const std::string& type,
                      const std::string& schema,
                      const std::string& table,
                      const std::string& column = "",
                      const std::string& dbEngine = "");

  GraphEdge createEdge(const std::string& sourceId,
                      const std::string& targetId,
                      const std::string& type,
                      const std::string& label = "");

  void addNodeIfNotExists(Graph& graph, const GraphNode& node);
  void addEdgeIfNotExists(Graph& graph, const GraphEdge& edge);

  void traverseLineageRecursive(
      Graph& graph,
      const std::string& currentNodeId,
      const std::string& schemaName,
      const std::string& tableName,
      const std::string& columnName,
      std::set<std::string>& visited,
      int depth,
      int maxDepth);
};

#endif // LINEAGE_GRAPH_BUILDER_H
