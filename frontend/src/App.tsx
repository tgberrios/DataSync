import { BrowserRouter, Routes, Route } from 'react-router-dom'
import Layout from './components/Layout'
import Dashboard from './components/Dashboard'
import Catalog from './components/Catalog'
import Monitor from './components/Monitor'
import LiveChanges from './components/LiveChanges'
import Quality from './components/Quality'
import Governance from './components/Governance'
import Security from './components/Security'
import LogsViewer from './components/LogsViewer'
import Config from './components/Config'
import QueryPerformance from './components/QueryPerformance'
import Maintenance from './components/Maintenance'
import ColumnCatalog from './components/ColumnCatalog'
import CatalogLocks from './components/CatalogLocks'
import DataLineageMariaDB from './components/DataLineageMariaDB'
import DataLineageMSSQL from './components/DataLineageMSSQL'
import DataLineageMongoDB from './components/DataLineageMongoDB'
import DataLineageOracle from './components/DataLineageOracle'
import GovernanceCatalogMariaDB from './components/GovernanceCatalogMariaDB'
import GovernanceCatalogMSSQL from './components/GovernanceCatalogMSSQL'
import GovernanceCatalogMongoDB from './components/GovernanceCatalogMongoDB'
import GovernanceCatalogOracle from './components/GovernanceCatalogOracle'

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="catalog" element={<Catalog />} />
          <Route path="column-catalog" element={<ColumnCatalog />} />
          <Route path="catalog-locks" element={<CatalogLocks />} />
          <Route path="data-lineage-mariadb" element={<DataLineageMariaDB />} />
          <Route path="data-lineage-mssql" element={<DataLineageMSSQL />} />
          <Route path="data-lineage-mongodb" element={<DataLineageMongoDB />} />
          <Route path="data-lineage-oracle" element={<DataLineageOracle />} />
          <Route path="governance-catalog-mariadb" element={<GovernanceCatalogMariaDB />} />
          <Route path="governance-catalog-mssql" element={<GovernanceCatalogMSSQL />} />
          <Route path="governance-catalog-mongodb" element={<GovernanceCatalogMongoDB />} />
          <Route path="governance-catalog-oracle" element={<GovernanceCatalogOracle />} />
          <Route path="monitor" element={<Monitor />} />
          <Route path="query-performance" element={<QueryPerformance />} />
          <Route path="maintenance" element={<Maintenance />} />
          <Route path="live-changes" element={<LiveChanges />} />
          <Route path="quality" element={<Quality />} />
          <Route path="governance" element={<Governance />} />
          <Route path="security" element={<Security />} />
          <Route path="logs" element={<LogsViewer />} />
          <Route path="config" element={<Config />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

export default App