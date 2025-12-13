import { BrowserRouter, Routes, Route } from "react-router-dom";
import { lazy, Suspense } from "react";
import Layout from "./components/Layout";
import { Container, Header } from "./components/shared/BaseComponents";

const LoadingFallback = () => (
  <Container>
    <Header>Loading...</Header>
  </Container>
);

const Dashboard = lazy(() => import("./components/Dashboard"));
const Catalog = lazy(() => import("./components/Catalog"));
const Monitor = lazy(() => import("./components/Monitor"));
const LiveChanges = lazy(() => import("./components/LiveChanges"));
const Quality = lazy(() => import("./components/Quality"));
const Governance = lazy(() => import("./components/Governance"));
const Security = lazy(() => import("./components/Security"));
const LogsViewer = lazy(() => import("./components/LogsViewer"));
const Config = lazy(() => import("./components/Config"));
const QueryPerformance = lazy(() => import("./components/QueryPerformance"));
const Maintenance = lazy(() => import("./components/Maintenance"));
const ColumnCatalog = lazy(() => import("./components/ColumnCatalog"));
const CatalogLocks = lazy(() => import("./components/CatalogLocks"));
const DataLineageMariaDB = lazy(() => import("./components/DataLineageMariaDB"));
const DataLineageMSSQL = lazy(() => import("./components/DataLineageMSSQL"));
const DataLineageMongoDB = lazy(() => import("./components/DataLineageMongoDB"));
const DataLineageOracle = lazy(() => import("./components/DataLineageOracle"));
const GovernanceCatalogMariaDB = lazy(
  () => import("./components/GovernanceCatalogMariaDB")
);
const GovernanceCatalogMSSQL = lazy(
  () => import("./components/GovernanceCatalogMSSQL")
);
const GovernanceCatalogMongoDB = lazy(
  () => import("./components/GovernanceCatalogMongoDB")
);
const GovernanceCatalogOracle = lazy(
  () => import("./components/GovernanceCatalogOracle")
);
const APICatalog = lazy(() => import("./components/APICatalog"));
const CustomJobs = lazy(() => import("./components/CustomJobs"));

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route
            index
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Dashboard />
              </Suspense>
            }
          />
          <Route
            path="catalog"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Catalog />
              </Suspense>
            }
          />
          <Route
            path="column-catalog"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <ColumnCatalog />
              </Suspense>
            }
          />
          <Route
            path="catalog-locks"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <CatalogLocks />
              </Suspense>
            }
          />
          <Route
            path="data-lineage-mariadb"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <DataLineageMariaDB />
              </Suspense>
            }
          />
          <Route
            path="data-lineage-mssql"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <DataLineageMSSQL />
              </Suspense>
            }
          />
          <Route
            path="data-lineage-mongodb"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <DataLineageMongoDB />
              </Suspense>
            }
          />
          <Route
            path="data-lineage-oracle"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <DataLineageOracle />
              </Suspense>
            }
          />
          <Route
            path="governance-catalog-mariadb"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <GovernanceCatalogMariaDB />
              </Suspense>
            }
          />
          <Route
            path="governance-catalog-mssql"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <GovernanceCatalogMSSQL />
              </Suspense>
            }
          />
          <Route
            path="governance-catalog-mongodb"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <GovernanceCatalogMongoDB />
              </Suspense>
            }
          />
          <Route
            path="governance-catalog-oracle"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <GovernanceCatalogOracle />
              </Suspense>
            }
          />
          <Route
            path="api-catalog"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <APICatalog />
              </Suspense>
            }
          />
          <Route
            path="custom-jobs"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <CustomJobs />
              </Suspense>
            }
          />
          <Route
            path="monitor"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Monitor />
              </Suspense>
            }
          />
          <Route
            path="query-performance"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <QueryPerformance />
              </Suspense>
            }
          />
          <Route
            path="maintenance"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Maintenance />
              </Suspense>
            }
          />
          <Route
            path="live-changes"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <LiveChanges />
              </Suspense>
            }
          />
          <Route
            path="quality"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Quality />
              </Suspense>
            }
          />
          <Route
            path="governance"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Governance />
              </Suspense>
            }
          />
          <Route
            path="security"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Security />
              </Suspense>
            }
          />
          <Route
            path="logs"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <LogsViewer />
              </Suspense>
            }
          />
          <Route
            path="config"
            element={
              <Suspense fallback={<LoadingFallback />}>
                <Config />
              </Suspense>
            }
          />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App