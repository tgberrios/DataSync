#ifndef LINEAGE_VISUALIZER_H
#define LINEAGE_VISUALIZER_H

#include "governance/LineageGraphBuilder.h"
#include "third_party/json.hpp"
#include <string>
#include <vector>

using json = nlohmann::json;

// LineageVisualizer: Genera datos para visualización de grafo de lineage
class LineageVisualizer {
public:
  struct VisualizationData {
    json nodes;  // Array de nodos para visualización
    json edges;  // Array de edges para visualización
    json layout;  // Información de layout
    json metadata;  // Metadata adicional
  };

  // Generar datos de visualización desde grafo
  static VisualizationData generateVisualizationData(
      const LineageGraphBuilder::Graph& graph
  );

  // Generar datos para react-flow
  static json generateReactFlowData(
      const LineageGraphBuilder::Graph& graph
  );

  // Generar datos para vis-network
  static json generateVisNetworkData(
      const LineageGraphBuilder::Graph& graph
  );

  // Filtrar grafo por criterios
  static LineageGraphBuilder::Graph filterGraph(
      const LineageGraphBuilder::Graph& graph,
      const std::vector<std::string>& nodeTypes = {},
      const std::vector<std::string>& edgeTypes = {},
      const std::vector<std::string>& schemas = {}
  );

  // Encontrar camino entre dos nodos
  static std::vector<std::string> findPath(
      const LineageGraphBuilder::Graph& graph,
      const std::string& sourceId,
      const std::string& targetId
  );

private:
  // Helper methods
  static json nodeToJSON(const LineageGraphBuilder::GraphNode& node);
  static json edgeToJSON(const LineageGraphBuilder::GraphEdge& edge);
};

#endif // LINEAGE_VISUALIZER_H
