#include "governance/LineageGraphBuilder.h"
#include <pqxx/pqxx>
#include "core/logger.h"
#include <sstream>
#include <algorithm>

LineageGraphBuilder::LineageGraphBuilder(const std::string& connectionString)
    : connectionString_(connectionString) {
}

LineageGraphBuilder::Graph LineageGraphBuilder::buildCompleteGraph(
    const std::vector<std::string>& dbEngines,
    const std::vector<std::string>& schemas) {
  
  Graph graph;

  try {
    // Agregar lineage de bases de datos
    if (dbEngines.empty()) {
      // Todas las bases de datos
      addDatabaseLineage(graph, "mssql");
      addDatabaseLineage(graph, "mariadb");
      addDatabaseLineage(graph, "mongodb");
      addDatabaseLineage(graph, "oracle");
    } else {
      for (const auto& engine : dbEngines) {
        addDatabaseLineage(graph, engine);
      }
    }

    // Agregar transformation lineage
    addTransformationLineage(graph);

    // Agregar workflow lineage
    addWorkflowLineage(graph);

    Logger::info(LogCategory::GOVERNANCE, "LineageGraphBuilder",
                 "Built complete graph with " + std::to_string(graph.nodes.size()) + 
                 " nodes and " + std::to_string(graph.edges.size()) + " edges");
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageGraphBuilder",
                  "Error building complete graph: " + std::string(e.what()));
  }

  return graph;
}

LineageGraphBuilder::Graph LineageGraphBuilder::buildGraphForResource(
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    int maxDepth) {
  
  Graph graph;
  std::set<std::string> visited;

  // Agregar nodo inicial
  GraphNode initialNode = createNode("table", schemaName, tableName, columnName);
  graph.nodes.push_back(initialNode);
  visited.insert(initialNode.id);

  // Traverse lineage recursivamente
  traverseLineageRecursive(graph, initialNode.id, schemaName, tableName, columnName,
                          visited, 0, maxDepth);

  return graph;
}

LineageGraphBuilder::Graph LineageGraphBuilder::buildGraphFromWorkflows(
    const std::vector<std::string>& workflowNames) {
  
  Graph graph;
  addWorkflowLineage(graph);
  return graph;
}

void LineageGraphBuilder::addDatabaseLineage(Graph& graph, const std::string& dbEngine) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    std::string tableName;
    if (dbEngine == "mssql") {
      tableName = "metadata.mssql_lineage";
    } else if (dbEngine == "mariadb") {
      tableName = "metadata.mariadb_lineage";
    } else if (dbEngine == "mongodb") {
      tableName = "metadata.mongodb_lineage";
    } else if (dbEngine == "oracle") {
      tableName = "metadata.oracle_lineage";
    } else {
      return;
    }

    auto result = txn.exec(
        "SELECT schema_name, object_name, object_type, "
        "target_schema_name, target_object_name, target_object_type, "
        "relationship_type, column_name, target_column_name "
        "FROM " + tableName
    );

    for (const auto& row : result) {
      // Nodo fuente
      std::string sourceSchema = row["schema_name"].as<std::string>();
      std::string sourceTable = row["object_name"].as<std::string>();
      GraphNode sourceNode = createNode("table", sourceSchema, sourceTable, "", dbEngine);
      addNodeIfNotExists(graph, sourceNode);

      // Nodo destino
      std::string targetSchema = row["target_schema_name"].as<std::string>();
      std::string targetTable = row["target_object_name"].as<std::string>();
      GraphNode targetNode = createNode("table", targetSchema, targetTable, "", dbEngine);
      addNodeIfNotExists(graph, targetNode);

      // Edge
      std::string relType = row["relationship_type"].as<std::string>();
      GraphEdge edge = createEdge(sourceNode.id, targetNode.id, relType, relType);
      addEdgeIfNotExists(graph, edge);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageGraphBuilder",
                  "Error adding database lineage: " + std::string(e.what()));
  }
}

void LineageGraphBuilder::addTransformationLineage(Graph& graph) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec(
        "SELECT transformation_type, input_schemas, input_tables, "
        "output_schemas, output_tables, workflow_name "
        "FROM metadata.transformation_lineage"
    );

    for (const auto& row : result) {
      std::string transType = row["transformation_type"].as<std::string>();
      std::string workflowName = row["workflow_name"].as<std::string>();

      // Crear nodo de transformación
      GraphNode transNode = createNode("transformation", "", "", "", "");
      transNode.id = "trans_" + workflowName + "_" + transType;
      transNode.label = transType;
      transNode.metadata["workflow"] = workflowName;
      addNodeIfNotExists(graph, transNode);

      // Helper para parsear arrays
      auto parseArray = [](const std::string& arrayStr) -> std::vector<std::string> {
        std::vector<std::string> result;
        if (arrayStr.empty() || arrayStr == "{}") {
          return result;
        }
        std::string content = arrayStr;
        if (content.front() == '{' && content.back() == '}') {
          content = content.substr(1, content.length() - 2);
        }
        std::istringstream iss(content);
        std::string item;
        while (std::getline(iss, item, ',')) {
          if (!item.empty()) {
            if (item.front() == '"' && item.back() == '"') {
              item = item.substr(1, item.length() - 2);
            }
            result.push_back(item);
          }
        }
        return result;
      };

      // Conectar inputs
      if (!row["input_tables"].is_null()) {
        auto inputTables = parseArray(row["input_tables"].as<std::string>());
        for (const auto& inputTable : inputTables) {
          GraphNode inputNode = createNode("table", "", inputTable, "", "");
          addNodeIfNotExists(graph, inputNode);
          
          GraphEdge edge = createEdge(inputNode.id, transNode.id, "input", "input");
          addEdgeIfNotExists(graph, edge);
        }
      }

      // Conectar outputs
      if (!row["output_tables"].is_null()) {
        auto outputTables = parseArray(row["output_tables"].as<std::string>());
        for (const auto& outputTable : outputTables) {
          GraphNode outputNode = createNode("table", "", outputTable, "", "");
          addNodeIfNotExists(graph, outputNode);
          
          GraphEdge edge = createEdge(transNode.id, outputNode.id, "output", "output");
          addEdgeIfNotExists(graph, edge);
        }
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageGraphBuilder",
                  "Error adding transformation lineage: " + std::string(e.what()));
  }
}

void LineageGraphBuilder::addWorkflowLineage(Graph& graph) {
  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    auto result = txn.exec(
        "SELECT w.workflow_name, wt.task_name, wt.task_type, wt.task_config "
        "FROM metadata.workflows w "
        "JOIN metadata.workflow_tasks wt ON w.workflow_name = wt.workflow_name"
    );

    for (const auto& row : result) {
      std::string workflowName = row["workflow_name"].as<std::string>();
      std::string taskName = row["task_name"].as<std::string>();
      std::string taskType = row["task_type"].as<std::string>();

      // Crear nodo de workflow
      GraphNode workflowNode = createNode("workflow", "", workflowName, "", "");
      addNodeIfNotExists(graph, workflowNode);

      // Crear nodo de tarea
      GraphNode taskNode = createNode("task", "", taskName, "", "");
      taskNode.metadata["workflow"] = workflowName;
      taskNode.metadata["type"] = taskType;
      addNodeIfNotExists(graph, taskNode);

      // Edge workflow -> task
      GraphEdge edge = createEdge(workflowNode.id, taskNode.id, "contains", "contains");
      addEdgeIfNotExists(graph, edge);
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageGraphBuilder",
                  "Error adding workflow lineage: " + std::string(e.what()));
  }
}

std::string LineageGraphBuilder::generateNodeId(
    const std::string& type,
    const std::string& schema,
    const std::string& table,
    const std::string& column) {
  
  std::stringstream id;
  id << type << "_" << schema << "_" << table;
  if (!column.empty()) {
    id << "_" << column;
  }
  return id.str();
}

LineageGraphBuilder::GraphNode LineageGraphBuilder::createNode(
    const std::string& type,
    const std::string& schema,
    const std::string& table,
    const std::string& column,
    const std::string& dbEngine) {
  
  GraphNode node;
  node.type = type;
  node.schema = schema;
  node.table = table;
  node.column = column;
  node.dbEngine = dbEngine;
  node.id = generateNodeId(type, schema, table, column);

  // Generar label
  if (!column.empty()) {
    node.label = schema + "." + table + "." + column;
  } else if (!table.empty()) {
    node.label = schema + "." + table;
  } else {
    node.label = schema;
  }

  return node;
}

LineageGraphBuilder::GraphEdge LineageGraphBuilder::createEdge(
    const std::string& sourceId,
    const std::string& targetId,
    const std::string& type,
    const std::string& label) {
  
  GraphEdge edge;
  edge.id = sourceId + "_" + type + "_" + targetId;
  edge.sourceId = sourceId;
  edge.targetId = targetId;
  edge.type = type;
  edge.label = label.empty() ? type : label;
  return edge;
}

void LineageGraphBuilder::addNodeIfNotExists(Graph& graph, const GraphNode& node) {
  auto it = std::find_if(graph.nodes.begin(), graph.nodes.end(),
                         [&node](const GraphNode& n) { return n.id == node.id; });
  if (it == graph.nodes.end()) {
    graph.nodes.push_back(node);
  }
}

void LineageGraphBuilder::addEdgeIfNotExists(Graph& graph, const GraphEdge& edge) {
  auto it = std::find_if(graph.edges.begin(), graph.edges.end(),
                         [&edge](const GraphEdge& e) { return e.id == edge.id; });
  if (it == graph.edges.end()) {
    graph.edges.push_back(edge);
  }
}

void LineageGraphBuilder::traverseLineageRecursive(
    Graph& graph,
    const std::string& currentNodeId,
    const std::string& schemaName,
    const std::string& tableName,
    const std::string& columnName,
    std::set<std::string>& visited,
    int depth,
    int maxDepth) {
  
  if (depth >= maxDepth) {
    return;
  }

  try {
    pqxx::connection conn(connectionString_);
    pqxx::work txn(conn);

    // Buscar en todas las tablas de lineage (downstream)
    std::string query = 
        "SELECT DISTINCT target_schema_name, target_table_name, relationship_type "
        "FROM ("
        "  SELECT target_schema_name, target_table_name, relationship_type "
        "  FROM metadata.mssql_lineage "
        "  WHERE schema_name = $1 AND object_name = $2 "
        "  UNION "
        "  SELECT target_schema_name, target_table_name, relationship_type "
        "  FROM metadata.mariadb_lineage "
        "  WHERE schema_name = $1 AND object_name = $2"
        ") AS lineage";

    auto result = txn.exec_params(query, schemaName, tableName);
    
    for (const auto& row : result) {
      std::string targetSchema = row["target_schema_name"].as<std::string>();
      std::string targetTable = row["target_table_name"].as<std::string>();
      std::string relType = row["relationship_type"].as<std::string>();
      std::string targetKey = targetSchema + "." + targetTable;

      if (visited.find(targetKey) == visited.end()) {
        // Crear nodo destino
        GraphNode targetNode = createNode("table", targetSchema, targetTable, "", "");
        addNodeIfNotExists(graph, targetNode);
        visited.insert(targetKey);

        // Crear edge
        GraphEdge edge = createEdge(currentNodeId, targetNode.id, relType, relType);
        addEdgeIfNotExists(graph, edge);

        // Continuar recursivamente
        traverseLineageRecursive(graph, targetNode.id, targetSchema, targetTable, "",
                                visited, depth + 1, maxDepth);
      } else {
        // Nodo ya visitado, solo agregar edge si no existe
        std::string targetNodeId = generateNodeId("table", targetSchema, targetTable, "");
        GraphEdge edge = createEdge(currentNodeId, targetNodeId, relType, relType);
        addEdgeIfNotExists(graph, edge);
      }
    }

    txn.commit();
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "LineageGraphBuilder",
                  "Error traversing lineage recursively: " + std::string(e.what()));
  }
}
