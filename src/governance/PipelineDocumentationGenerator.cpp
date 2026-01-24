#include "governance/PipelineDocumentationGenerator.h"
#include <fstream>
#include <sstream>
#include <iomanip>

PipelineDocumentationGenerator::PipelineDocumentationGenerator(
    const std::string& connectionString)
    : connectionString_(connectionString) {
  metadataCollector_ = std::make_unique<PipelineMetadataCollector>(connectionString);
}

std::string PipelineDocumentationGenerator::generateDocumentation(
    const std::string& workflowName,
    OutputFormat format) {
  
  PipelineMetadataCollector::PipelineMetadata metadata = 
      metadataCollector_->collectWorkflowMetadata(workflowName);
  
  std::vector<json> transformations = 
      metadataCollector_->collectTransformations(workflowName);

  switch (format) {
    case OutputFormat::MARKDOWN:
      return generateMarkdown(metadata, transformations);
    case OutputFormat::HTML:
      return generateHTML(metadata, transformations);
    case OutputFormat::JSON: {
      json doc;
      doc["workflow_name"] = metadata.workflowName;
      doc["description"] = metadata.description;
      doc["tasks"] = metadata.tasks;
      doc["transformations"] = transformations;
      return doc.dump(2);
    }
    default:
      return generateMarkdown(metadata, transformations);
  }
}

std::string PipelineDocumentationGenerator::generateMarkdown(
    const PipelineMetadataCollector::PipelineMetadata& metadata,
    const std::vector<json>& transformations) {
  
  std::stringstream md;

  md << "# Pipeline Documentation: " << metadata.workflowName << "\n\n";

  if (!metadata.description.empty()) {
    md << "## Description\n\n" << metadata.description << "\n\n";
  }

  md << "## Overview\n\n";
  md << "- **Total Executions**: " << metadata.totalExecutions << "\n";
  md << "- **Successful**: " << metadata.successfulExecutions << "\n";
  md << "- **Failed**: " << metadata.failedExecutions << "\n";
  md << "- **Average Execution Time**: " << std::fixed << std::setprecision(2)
     << metadata.averageExecutionTimeMs << " ms\n";
  if (!metadata.schedule.empty()) {
    md << "- **Schedule**: " << metadata.schedule << "\n";
  }
  md << "\n";

  md << "## Tasks\n\n";
  for (const auto& task : metadata.tasks) {
    md << "### " << task << "\n";
    if (metadata.taskDescriptions.find(task) != metadata.taskDescriptions.end()) {
      md << metadata.taskDescriptions.at(task) << "\n";
    }
    if (metadata.dependencies.find(task) != metadata.dependencies.end()) {
      md << "**Dependencies**: ";
      const auto& deps = metadata.dependencies.at(task);
      for (size_t i = 0; i < deps.size(); ++i) {
        if (i > 0) md << ", ";
        md << deps[i];
      }
      md << "\n";
    }
    md << "\n";
  }

  if (!transformations.empty()) {
    md << "## Transformations\n\n";
    for (const auto& trans : transformations) {
      md << "### " << trans["type"].get<std::string>() << "\n";
      md << "- **Rows Processed**: " << trans["rows_processed"].get<int64_t>() << "\n";
      md << "- **Execution Time**: " << trans["execution_time_ms"].get<double>() << " ms\n";
      md << "\n";
    }
  }

  md << "## Flow Diagram\n\n";
  md << "```mermaid\n";
  md << generateFlowDiagram(metadata);
  md << "\n```\n";

  return md.str();
}

std::string PipelineDocumentationGenerator::generateHTML(
    const PipelineMetadataCollector::PipelineMetadata& metadata,
    const std::vector<json>& transformations) {
  
  std::stringstream html;

  html << "<!DOCTYPE html>\n<html><head><title>Pipeline: " 
       << metadata.workflowName << "</title></head><body>\n";
  html << "<h1>Pipeline Documentation: " << metadata.workflowName << "</h1>\n";

  if (!metadata.description.empty()) {
    html << "<h2>Description</h2><p>" << metadata.description << "</p>\n";
  }

  html << "<h2>Overview</h2><ul>\n";
  html << "<li><strong>Total Executions</strong>: " << metadata.totalExecutions << "</li>\n";
  html << "<li><strong>Successful</strong>: " << metadata.successfulExecutions << "</li>\n";
  html << "<li><strong>Failed</strong>: " << metadata.failedExecutions << "</li>\n";
  html << "<li><strong>Average Execution Time</strong>: " 
       << std::fixed << std::setprecision(2) << metadata.averageExecutionTimeMs 
       << " ms</li>\n";
  html << "</ul>\n";

  html << "<h2>Tasks</h2><ul>\n";
  for (const auto& task : metadata.tasks) {
    html << "<li><strong>" << task << "</strong>";
    if (metadata.taskDescriptions.find(task) != metadata.taskDescriptions.end()) {
      html << ": " << metadata.taskDescriptions.at(task);
    }
    html << "</li>\n";
  }
  html << "</ul>\n";

  html << "</body></html>\n";
  return html.str();
}

std::string PipelineDocumentationGenerator::generateFlowDiagram(
    const PipelineMetadataCollector::PipelineMetadata& metadata) {
  
  std::stringstream mermaid;

  mermaid << "graph TD\n";

  // Crear nodos para cada tarea
  for (const auto& task : metadata.tasks) {
    std::string nodeId = "T" + std::to_string(std::hash<std::string>{}(task));
    mermaid << "  " << nodeId << "[\"" << task << "\"]\n";
  }

  // Crear edges basados en dependencias
  for (const auto& [task, deps] : metadata.dependencies) {
    std::string taskId = "T" + std::to_string(std::hash<std::string>{}(task));
    for (const auto& dep : deps) {
      std::string depId = "T" + std::to_string(std::hash<std::string>{}(dep));
      mermaid << "  " << depId << " --> " << taskId << "\n";
    }
  }

  return mermaid.str();
}

bool PipelineDocumentationGenerator::exportToFile(
    const std::string& workflowName,
    const std::string& filePath,
    OutputFormat format) {
  
  try {
    std::string documentation = generateDocumentation(workflowName, format);
    
    std::ofstream file(filePath);
    if (!file.is_open()) {
      Logger::error(LogCategory::GOVERNANCE, "PipelineDocumentationGenerator",
                    "Failed to open file for writing: " + filePath);
      return false;
    }

    file << documentation;
    file.close();

    Logger::info(LogCategory::GOVERNANCE, "PipelineDocumentationGenerator",
                 "Exported documentation to: " + filePath);
    return true;
  } catch (const std::exception& e) {
    Logger::error(LogCategory::GOVERNANCE, "PipelineDocumentationGenerator",
                  "Error exporting documentation: " + std::string(e.what()));
    return false;
  }
}

std::string PipelineDocumentationGenerator::formatExecutionStatistics(
    const PipelineMetadataCollector::PipelineMetadata& metadata) {
  
  std::stringstream stats;
  stats << "Total: " << metadata.totalExecutions << ", "
        << "Success: " << metadata.successfulExecutions << ", "
        << "Failed: " << metadata.failedExecutions;
  return stats.str();
}
