#ifndef PIPELINE_DOCUMENTATION_GENERATOR_H
#define PIPELINE_DOCUMENTATION_GENERATOR_H

#include "governance/PipelineMetadataCollector.h"
#include "third_party/json.hpp"
#include "core/logger.h"
#include <string>
#include <vector>

using json = nlohmann::json;

// PipelineDocumentationGenerator: Genera documentación automática de pipelines
class PipelineDocumentationGenerator {
public:
  enum class OutputFormat {
    MARKDOWN,
    HTML,
    JSON
  };

  explicit PipelineDocumentationGenerator(
      const std::string& connectionString);
  ~PipelineDocumentationGenerator() = default;

  // Generar documentación de un workflow
  std::string generateDocumentation(
      const std::string& workflowName,
      OutputFormat format = OutputFormat::MARKDOWN
  );

  // Generar documentación en Markdown
  std::string generateMarkdown(
      const PipelineMetadataCollector::PipelineMetadata& metadata,
      const std::vector<json>& transformations
  );

  // Generar documentación en HTML
  std::string generateHTML(
      const PipelineMetadataCollector::PipelineMetadata& metadata,
      const std::vector<json>& transformations
  );

  // Generar diagrama de flujo (Mermaid syntax)
  std::string generateFlowDiagram(
      const PipelineMetadataCollector::PipelineMetadata& metadata
  );

  // Exportar documentación a archivo
  bool exportToFile(
      const std::string& workflowName,
      const std::string& filePath,
      OutputFormat format = OutputFormat::MARKDOWN
  );

private:
  std::string connectionString_;
  std::unique_ptr<PipelineMetadataCollector> metadataCollector_;

  // Helper methods
  std::string formatExecutionStatistics(
      const PipelineMetadataCollector::PipelineMetadata& metadata
  );
};

#endif // PIPELINE_DOCUMENTATION_GENERATOR_H
