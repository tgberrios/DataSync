import { BrowserRouter, Routes, Route } from "react-router-dom";
import { lazy, Suspense } from "react";
import Layout from "./components/Layout";
import ProtectedRoute from "./components/ProtectedRoute";
import { Container, Header } from "./components/shared/BaseComponents";

const LoadingFallback = () => (
  <Container>
    <Header>Loading...</Header>
  </Container>
);

const Dashboard = lazy(() => import("./components/Dashboard"));
const Catalog = lazy(() => import("./components/Catalog"));
const UnifiedMonitor = lazy(() => import("./components/UnifiedMonitor"));
const Quality = lazy(() => import("./components/Quality"));
const Governance = lazy(() => import("./components/Governance"));
const Security = lazy(() => import("./components/Security"));
const LogsViewer = lazy(() => import("./components/LogsViewer"));
const Config = lazy(() => import("./components/Config"));
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
const UserManagement = lazy(() => import("./components/UserManagement"));
const Login = lazy(() => import("./components/Login"));

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/login" element={
          <Suspense fallback={<LoadingFallback />}>
            <Login />
          </Suspense>
        } />
        <Route path="/" element={<Layout />}>
          <Route
            index
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Dashboard />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="catalog"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Catalog />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="column-catalog"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <ColumnCatalog />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="catalog-locks"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <CatalogLocks />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="data-lineage-mariadb"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <DataLineageMariaDB />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="data-lineage-mssql"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <DataLineageMSSQL />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="data-lineage-mongodb"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <DataLineageMongoDB />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="data-lineage-oracle"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <DataLineageOracle />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="governance-catalog-mariadb"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <GovernanceCatalogMariaDB />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="governance-catalog-mssql"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <GovernanceCatalogMSSQL />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="governance-catalog-mongodb"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <GovernanceCatalogMongoDB />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="governance-catalog-oracle"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <GovernanceCatalogOracle />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="api-catalog"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <APICatalog />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="custom-jobs"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <CustomJobs />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="monitor"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <UnifiedMonitor />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="query-performance"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <UnifiedMonitor />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="maintenance"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Maintenance />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="live-changes"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <UnifiedMonitor />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="quality"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Quality />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="governance"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Governance />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="security"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Security />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="logs"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <LogsViewer />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="config"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <Config />
                </Suspense>
              </ProtectedRoute>
            }
          />
          <Route
            path="user-management"
            element={
              <ProtectedRoute>
                <Suspense fallback={<LoadingFallback />}>
                  <UserManagement />
                </Suspense>
              </ProtectedRoute>
            }
          />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App