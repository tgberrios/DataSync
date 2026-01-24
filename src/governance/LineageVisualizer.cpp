#include "governance/LineageVisualizer.h"
#include <algorithm>
#include <queue>
#include <map>

LineageVisualizer::VisualizationData LineageVisualizer::generateVisualizationData(
    const LineageGraphBuilder::Graph& graph) {
  
  VisualizationData data;

  // Convertir nodos a JSON
  json nodesJson = json::array();
  for (const auto& node : graph.nodes) {
    nodesJson.push_back(nodeToJSON(node));
  }

  // Convertir edges a JSON
  json edgesJson = json::array();
  for (const auto& edge : graph.edges) {
    edgesJson.push_back(edgeToJSON(edge));
  }

  data.nodes = nodesJson;
  data.edges = edgesJson;
  data.metadata = graph.metadata;

  return data;
}

json LineageVisualizer::generateReactFlowData(
    const LineageGraphBuilder::Graph& graph) {
  
  json flowData;
  flowData["nodes"] = json::array();
  flowData["edges"] = json::array();

  // Convertir nodos a formato react-flow
  for (size_t i = 0; i < graph.nodes.size(); ++i) {
    const auto& node = graph.nodes[i];
    json flowNode;
    flowNode["id"] = node.id;
    flowNode["type"] = "default";
    flowNode["data"] = json::object();
    flowNode["data"]["label"] = node.label;
    flowNode["data"]["type"] = node.type;
    flowNode["data"]["schema"] = node.schema;
    flowNode["data"]["table"] = node.table;
    flowNode["data"]["column"] = node.column;
    flowNode["data"]["dbEngine"] = node.dbEngine;
    flowNode["position"] = json::object();
    flowNode["position"]["x"] = (i % 10) * 200;
    flowNode["position"]["y"] = (i / 10) * 150;
    flowData["nodes"].push_back(flowNode);
  }

  // Convertir edges a formato react-flow
  for (const auto& edge : graph.edges) {
    json flowEdge;
    flowEdge["id"] = edge.id;
    flowEdge["source"] = edge.sourceId;
    flowEdge["target"] = edge.targetId;
    flowEdge["type"] = "smoothstep";
    flowEdge["label"] = edge.label;
    flowEdge["data"] = json::object();
    flowEdge["data"]["type"] = edge.type;
    flowEdge["data"]["confidence"] = edge.confidence;
    flowData["edges"].push_back(flowEdge);
  }

  return flowData;
}

json LineageVisualizer::generateVisNetworkData(
    const LineageGraphBuilder::Graph& graph) {
  
  json visData;
  visData["nodes"] = json::array();
  visData["edges"] = json::array();

  // Convertir nodos a formato vis-network
  for (const auto& node : graph.nodes) {
    json visNode;
    visNode["id"] = node.id;
    visNode["label"] = node.label;
    visNode["group"] = node.type;
    visNode["title"] = node.schema + "." + node.table;
    visData["nodes"].push_back(visNode);
  }

  // Convertir edges a formato vis-network
  for (const auto& edge : graph.edges) {
    json visEdge;
    visEdge["id"] = edge.id;
    visEdge["from"] = edge.sourceId;
    visEdge["to"] = edge.targetId;
    visEdge["label"] = edge.label;
    visEdge["arrows"] = "to";
    visData["edges"].push_back(visEdge);
  }

  return visData;
}

LineageGraphBuilder::Graph LineageVisualizer::filterGraph(
    const LineageGraphBuilder::Graph& graph,
    const std::vector<std::string>& nodeTypes,
    const std::vector<std::string>& edgeTypes,
    const std::vector<std::string>& schemas) {
  
  LineageGraphBuilder::Graph filtered;

  // Filtrar nodos
  for (const auto& node : graph.nodes) {
    bool include = true;

    if (!nodeTypes.empty()) {
      include = include && (std::find(nodeTypes.begin(), nodeTypes.end(), 
                                     node.type) != nodeTypes.end());
    }

    if (!schemas.empty() && !node.schema.empty()) {
      include = include && (std::find(schemas.begin(), schemas.end(), 
                                     node.schema) != schemas.end());
    }

    if (include) {
      filtered.nodes.push_back(node);
    }
  }

  // Filtrar edges (solo si ambos nodos están incluidos)
  std::set<std::string> includedNodeIds;
  for (const auto& node : filtered.nodes) {
    includedNodeIds.insert(node.id);
  }

  for (const auto& edge : graph.edges) {
    bool include = true;

    if (!edgeTypes.empty()) {
      include = include && (std::find(edgeTypes.begin(), edgeTypes.end(), 
                                     edge.type) != edgeTypes.end());
    }

    // Solo incluir si ambos nodos están en el grafo filtrado
    include = include && 
              (includedNodeIds.find(edge.sourceId) != includedNodeIds.end()) &&
              (includedNodeIds.find(edge.targetId) != includedNodeIds.end());

    if (include) {
      filtered.edges.push_back(edge);
    }
  }

  return filtered;
}

std::vector<std::string> LineageVisualizer::findPath(
    const LineageGraphBuilder::Graph& graph,
    const std::string& sourceId,
    const std::string& targetId) {
  
  std::vector<std::string> path;

  // BFS para encontrar camino
  std::queue<std::string> queue;
  std::map<std::string, std::string> parent;
  std::set<std::string> visited;

  queue.push(sourceId);
  visited.insert(sourceId);
  parent[sourceId] = "";

  while (!queue.empty()) {
    std::string current = queue.front();
    queue.pop();

    if (current == targetId) {
      // Reconstruir camino
      std::string node = targetId;
      while (!node.empty()) {
        path.push_back(node);
        node = parent[node];
      }
      std::reverse(path.begin(), path.end());
      return path;
    }

    // Buscar edges desde current
    for (const auto& edge : graph.edges) {
      if (edge.sourceId == current && 
          visited.find(edge.targetId) == visited.end()) {
        visited.insert(edge.targetId);
        parent[edge.targetId] = current;
        queue.push(edge.targetId);
      }
    }
  }

  return path;  // Vacío si no hay camino
}

json LineageVisualizer::nodeToJSON(const LineageGraphBuilder::GraphNode& node) {
  json nodeJson;
  nodeJson["id"] = node.id;
  nodeJson["type"] = node.type;
  nodeJson["label"] = node.label;
  nodeJson["schema"] = node.schema;
  nodeJson["table"] = node.table;
  nodeJson["column"] = node.column;
  nodeJson["dbEngine"] = node.dbEngine;
  nodeJson["metadata"] = node.metadata;
  return nodeJson;
}

json LineageVisualizer::edgeToJSON(const LineageGraphBuilder::GraphEdge& edge) {
  json edgeJson;
  edgeJson["id"] = edge.id;
  edgeJson["source"] = edge.sourceId;
  edgeJson["target"] = edge.targetId;
  edgeJson["type"] = edge.type;
  edgeJson["label"] = edge.label;
  edgeJson["confidence"] = edge.confidence;
  edgeJson["metadata"] = edge.metadata;
  return edgeJson;
}
