import React, { useState, useEffect, useCallback, useRef, useMemo } from 'react';
import { useLocation } from 'react-router-dom';
import styled, { keyframes } from 'styled-components';
import { theme } from '../theme/theme';
import { Container, Header, ErrorMessage, LoadingOverlay } from './shared/BaseComponents';
import { monitorApi, queryPerformanceApi, dashboardApi } from '../services/api';
import { extractApiError } from '../utils/errorHandler';

const fadeIn = keyframes`
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
`;

const slideDown = keyframes`
  from {
    max-height: 0;
    opacity: 0;
  }
  to {
    max-height: 1000px;
    opacity: 1;
  }
`;

const slideUp = keyframes`
  from {
    max-height: 1000px;
    opacity: 1;
  }
  to {
    max-height: 0;
    opacity: 0;
  }
`;

const MainLayout = styled.div`
  display: grid;
  grid-template-columns: 1fr 500px;
  gap: ${theme.spacing.lg};
  height: calc(100vh - 200px);
  min-height: 600px;
`;

const TreePanel = styled.div`
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.lg};
  padding: ${theme.spacing.lg};
  overflow-y: auto;
  box-shadow: ${theme.shadows.md};
  
  &::-webkit-scrollbar {
    width: 8px;
  }
  
  &::-webkit-scrollbar-track {
    background: ${theme.colors.background.secondary};
    border-radius: ${theme.borderRadius.sm};
  }
  
  &::-webkit-scrollbar-thumb {
    background: ${theme.colors.border.medium};
    border-radius: ${theme.borderRadius.sm};
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
`;

const DetailsPanel = styled.div`
  background: ${theme.colors.background.main};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.lg};
  padding: ${theme.spacing.lg};
  overflow-y: auto;
  box-shadow: ${theme.shadows.md};
  
  &::-webkit-scrollbar {
    width: 8px;
  }
  
  &::-webkit-scrollbar-track {
    background: ${theme.colors.background.secondary};
    border-radius: ${theme.borderRadius.sm};
  }
  
  &::-webkit-scrollbar-thumb {
    background: ${theme.colors.border.medium};
    border-radius: ${theme.borderRadius.sm};
    
    &:hover {
      background: ${theme.colors.primary.main};
    }
  }
`;

const TabContainer = styled.div`
  display: flex;
  gap: ${theme.spacing.sm};
  margin-bottom: ${theme.spacing.md};
  border-bottom: 2px solid ${theme.colors.border.light};
  padding-bottom: ${theme.spacing.sm};
`;

const Tab = styled.button<{ $active: boolean }>`
  padding: ${theme.spacing.sm} ${theme.spacing.md};
  border: none;
  background: ${props => props.$active ? theme.colors.primary.main : 'transparent'};
  color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.secondary};
  border-radius: ${theme.borderRadius.md} ${theme.borderRadius.md} 0 0;
  cursor: pointer;
  font-weight: ${props => props.$active ? '600' : '500'};
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${props => props.$active ? theme.colors.primary.dark : theme.colors.background.secondary};
    color: ${props => props.$active ? theme.colors.text.white : theme.colors.text.primary};
  }
`;

const StatsContainer = styled.div`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: ${theme.spacing.md};
  margin-bottom: ${theme.spacing.lg};
`;

const StatCard = styled.div`
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  padding: ${theme.spacing.md};
  text-align: center;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    transform: translateY(-2px);
    box-shadow: ${theme.shadows.md};
    border-color: ${theme.colors.primary.main};
  }
`;

const StatValue = styled.div`
  font-size: 2em;
  font-weight: bold;
  color: ${theme.colors.primary.main};
  margin-bottom: ${theme.spacing.xs};
`;

const StatLabel = styled.div`
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-weight: 500;
`;

const TreeContainer = styled.div`
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', 'Roboto', 'Oxygen', 'Ubuntu', 'Cantarell', sans-serif;
  font-size: 0.95em;
`;

const TreeNode = styled.div`
  user-select: none;
  animation: ${fadeIn} 0.3s ease-out;
  margin-bottom: 2px;
`;

const TreeContent = styled.div<{ $level: number; $isExpanded?: boolean; $nodeType?: 'database' | 'schema' | 'table' | 'query' }>`
  display: flex;
  align-items: center;
  padding: ${props => props.$level === 0 ? '12px 8px' : props.$level === 1 ? '10px 8px' : '8px 8px'};
  padding-left: ${props => props.$level * 24 + 8}px;
  cursor: pointer;
  border-radius: ${theme.borderRadius.md};
  transition: all ${theme.transitions.normal};
  position: relative;
  margin: 2px 0;
  
  ${props => {
    if (props.$nodeType === 'database') {
      return `
        background: linear-gradient(135deg, ${theme.colors.primary.light}08 0%, ${theme.colors.primary.main}05 100%);
        border-left: 3px solid ${theme.colors.primary.main};
        font-weight: 600;
      `;
    }
    if (props.$nodeType === 'schema') {
      return `
        background: ${theme.colors.background.secondary};
        border-left: 2px solid ${theme.colors.border.medium};
      `;
    }
    return `
      border-left: 1px solid ${theme.colors.border.light};
    `;
  }}
  
  &:hover {
    background: ${props => {
      if (props.$nodeType === 'database') {
        return `linear-gradient(135deg, ${theme.colors.primary.light}15 0%, ${theme.colors.primary.main}10 100%)`;
      }
      return theme.colors.background.secondary;
    }};
    transform: translateX(2px);
    box-shadow: ${theme.shadows.sm};
  }
`;

const ExpandIconContainer = styled.div<{ $isExpanded: boolean }>`
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 20px;
  height: 20px;
  margin-right: 8px;
  border-radius: ${theme.borderRadius.sm};
  background: ${props => props.$isExpanded 
    ? `linear-gradient(135deg, ${theme.colors.primary.main} 0%, ${theme.colors.primary.light} 100%)`
    : theme.colors.background.secondary
  };
  color: ${props => props.$isExpanded ? theme.colors.text.white : theme.colors.primary.main};
  font-size: 0.7em;
  font-weight: bold;
  transition: all ${theme.transitions.normal};
  flex-shrink: 0;
  
  &:hover {
    transform: scale(1.1);
    box-shadow: ${theme.shadows.sm};
  }
  
  svg {
    transition: transform ${theme.transitions.normal};
    transform: ${props => props.$isExpanded ? 'rotate(0deg)' : 'rotate(-90deg)'};
  }
`;

const NodeLabel = styled.span<{ $isDatabase?: boolean; $isSchema?: boolean }>`
  font-weight: ${props => props.$isDatabase ? '700' : props.$isSchema ? '600' : '500'};
  font-size: ${props => props.$isDatabase ? '1.05em' : props.$isSchema ? '0.98em' : '0.92em'};
  color: ${props => {
    if (props.$isDatabase) return theme.colors.primary.main;
    if (props.$isSchema) return theme.colors.text.primary;
    return theme.colors.text.secondary;
  }};
  margin-right: 12px;
  transition: color ${theme.transitions.normal};
  flex: 1;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
`;

const CountBadge = styled.span`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.8em;
  font-weight: 500;
  background: linear-gradient(135deg, ${theme.colors.background.secondary} 0%, ${theme.colors.background.tertiary} 100%);
  color: ${theme.colors.text.secondary};
  border: 1px solid ${theme.colors.border.light};
  margin-left: auto;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: linear-gradient(135deg, ${theme.colors.primary.light}10 0%, ${theme.colors.primary.main}08 100%);
    border-color: ${theme.colors.primary.main};
    color: ${theme.colors.primary.main};
    transform: translateY(-1px);
  }
`;

const ExpandableContent = styled.div<{ $isExpanded: boolean; $level: number }>`
  overflow: hidden;
  animation: ${props => props.$isExpanded ? slideDown : slideUp} 0.3s ease-out;
  padding-left: ${props => props.$level * 24 + 36}px;
`;

const QueryItem = styled.div<{ $selected: boolean }>`
  padding: 10px 8px;
  margin: 4px 0;
  border-radius: ${theme.borderRadius.md};
  border-left: 2px solid ${props => props.$selected ? theme.colors.primary.main : theme.colors.border.light};
  background: ${props => props.$selected ? theme.colors.primary.light + '15' : 'transparent'};
  cursor: pointer;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${theme.colors.background.secondary};
    border-left-color: ${theme.colors.primary.main};
  }
`;

const QueryPreview = styled.div`
  font-size: 0.85em;
  color: ${theme.colors.text.secondary};
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  margin-top: 4px;
  font-family: 'Courier New', monospace;
`;

const KillButton = styled.button`
  padding: 4px 8px;
  border: none;
  border-radius: ${theme.borderRadius.sm};
  background: ${theme.colors.status.error.bg};
  color: ${theme.colors.status.error.text};
  cursor: pointer;
  font-size: 0.75em;
  font-weight: 500;
  transition: all ${theme.transitions.normal};
  
  &:hover {
    background: ${theme.colors.status.error.text};
    color: ${theme.colors.background.main};
    transform: translateY(-1px);
    box-shadow: ${theme.shadows.sm};
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const StatusBadge = styled.span<{ $status: string }>`
  padding: 4px 10px;
  border-radius: ${theme.borderRadius.md};
  font-size: 0.75em;
  font-weight: 500;
  display: inline-block;
  margin-left: 8px;
  background-color: ${props => {
    switch (props.$status) {
      case 'active': return theme.colors.status.success.bg;
      case 'idle in transaction': return theme.colors.status.warning.bg;
      case 'idle in transaction (aborted)': return theme.colors.status.error.bg;
      case 'FULL_LOAD': return theme.colors.status.info.bg;
      case 'LISTENING_CHANGES': return theme.colors.status.success.bg;
      case 'ERROR': return theme.colors.status.error.bg;
      case 'EXCELLENT': return theme.colors.status.success.bg;
      case 'GOOD': return theme.colors.status.info.bg;
      case 'FAIR': return theme.colors.status.warning.bg;
      case 'POOR': return theme.colors.status.error.bg;
      default: return theme.colors.background.secondary;
    }
  }};
  color: ${props => {
    switch (props.$status) {
      case 'active': return theme.colors.status.success.text;
      case 'idle in transaction': return theme.colors.status.warning.text;
      case 'idle in transaction (aborted)': return theme.colors.status.error.text;
      case 'FULL_LOAD': return theme.colors.status.info.text;
      case 'LISTENING_CHANGES': return theme.colors.status.success.text;
      case 'ERROR': return theme.colors.status.error.text;
      case 'EXCELLENT': return theme.colors.status.success.text;
      case 'GOOD': return theme.colors.status.info.text;
      case 'FAIR': return theme.colors.status.warning.text;
      case 'POOR': return theme.colors.status.error.text;
      default: return theme.colors.text.secondary;
    }
  }};
`;

const DetailsCard = styled.div`
  background: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  padding: ${theme.spacing.lg};
  margin-bottom: ${theme.spacing.md};
`;

const DetailsTitle = styled.h3`
  font-size: 1.1em;
  font-weight: 600;
  color: ${theme.colors.text.primary};
  margin: 0 0 ${theme.spacing.md} 0;
  padding-bottom: ${theme.spacing.sm};
  border-bottom: 2px solid ${theme.colors.border.light};
`;

const DetailsGrid = styled.div`
  display: grid;
  grid-template-columns: 180px 1fr;
  gap: ${theme.spacing.md};
  font-size: 0.9em;
`;

const DetailLabel = styled.div`
  color: ${theme.colors.text.secondary};
  font-weight: 500;
`;

const DetailValue = styled.div`
  color: ${theme.colors.text.primary};
  word-break: break-word;
`;

const QueryText = styled.pre`
  margin: ${theme.spacing.md} 0 0 0;
  padding: ${theme.spacing.md};
  background-color: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  overflow-x: auto;
  font-size: 0.85em;
  border: 1px solid ${theme.colors.border.light};
  font-family: 'Courier New', monospace;
  white-space: pre-wrap;
  word-wrap: break-word;
`;

const EmptyState = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
  
  &::before {
    content: '📊';
    font-size: 3em;
    display: block;
    margin-bottom: ${theme.spacing.md};
    opacity: 0.5;
  }
`;

const EmptyDetails = styled.div`
  padding: 60px 40px;
  text-align: center;
  color: ${theme.colors.text.secondary};
  animation: ${fadeIn} 0.5s ease-out;
  
  &::before {
    content: '👈';
    font-size: 3em;
    display: block;
    margin-bottom: ${theme.spacing.md};
    opacity: 0.5;
  }
`;

const ChartContainer = styled.div`
  width: 100%;
  margin-top: 20px;
  padding: 25px;
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.lg};
  box-shadow: ${theme.shadows.md};
  color: ${theme.colors.text.primary};
  overflow: visible;
  position: relative;
  animation: ${fadeIn} 0.6s cubic-bezier(0.4, 0, 0.2, 1);
  transition: box-shadow 0.3s ease, transform 0.3s ease;
  
  &:hover {
    box-shadow: ${theme.shadows.lg};
    transform: translateY(-2px);
  }
`;

const ChartTitle = styled.div`
  font-weight: bold;
  margin-bottom: 20px;
  color: ${theme.colors.text.primary};
  font-size: 12px;
  text-transform: uppercase;
  letter-spacing: 1px;
  display: flex;
  gap: 24px;
  flex-wrap: wrap;
`;

const LegendItem = styled.label<{ $lineStyle: string; $active: boolean }>`
  display: inline-flex;
  align-items: center;
  gap: 8px;
  font-size: 10px;
  color: ${(props) => (props.$active ? theme.colors.text.primary : theme.colors.text.secondary)};
  transition: all 0.3s ease;
  padding: 6px 10px;
  border-radius: ${theme.borderRadius.sm};
  cursor: pointer;
  user-select: none;
  border: 1px solid ${(props) => (props.$active ? theme.colors.border.medium : "transparent")};
  background: ${(props) => (props.$active ? theme.colors.background.secondary : "transparent")};
  
  &:hover {
    background: ${theme.colors.background.secondary};
    color: ${theme.colors.text.primary};
    transform: translateY(-1px);
    border-color: ${theme.colors.border.medium};
  }
`;

const Checkbox = styled.input`
  width: 14px;
  height: 14px;
  cursor: pointer;
  accent-color: ${theme.colors.status.info.text};
  
  &:checked {
    accent-color: ${theme.colors.status.info.text};
  }
`;

const ShowAllButton = styled.button`
  margin-left: auto;
  padding: 6px 12px;
  font-size: 10px;
  background: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.medium};
  border-radius: ${theme.borderRadius.sm};
  color: ${theme.colors.text.primary};
  cursor: pointer;
  transition: all 0.3s ease;
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-weight: 500;
  
  &:hover {
    background: ${theme.colors.primary.main};
    color: ${theme.colors.text.white};
    border-color: ${theme.colors.primary.main};
    transform: translateY(-1px);
  }
  
  &:active {
    transform: translateY(0);
  }
`;

const LegendLine = styled.span<{ $color: string; $active: boolean }>`
  display: inline-block;
  width: 24px;
  height: 0;
  opacity: ${(props) => (props.$active ? 1 : 0.4)};
  transition: opacity 0.3s ease;
  border-top: 2px solid;
  border-color: ${(props) => props.$color};
`;

const ChartPath = styled.path`
  transition: d 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94);
  will-change: d;
  opacity: 1;
  vector-effect: non-scaling-stroke;
`;

const ChartAreaPath = styled.path`
  transition: d 0.6s cubic-bezier(0.25, 0.46, 0.45, 0.94);
  will-change: d;
  opacity: 1;
`;

const SVGChart = styled.svg`
  width: 100%;
  height: 100%;
  display: block;
  min-height: 400px;
`;

const ChartArea = styled.div`
  width: 100%;
  height: 500px;
  min-height: 400px;
  position: relative;
  background: ${theme.colors.background.main};
  border-radius: ${theme.borderRadius.md};
  padding: 20px;
  overflow: visible;
`;

const GridLine = styled.line`
  stroke: ${theme.colors.border.light};
  stroke-width: 1;
  opacity: 0.2;
`;

const AxisLabel = styled.text`
  font-size: 10px;
  fill: ${theme.colors.text.secondary};
  font-family: system-ui, -apple-system, sans-serif;
`;

const AxisLine = styled.line`
  stroke: ${theme.colors.border.medium};
  stroke-width: 1.5;
  opacity: 0.6;
`;

const SystemResourcesChart: React.FC<{
  datasets: Array<{ data: number[]; symbol: string; name: string }>;
  labels: string[];
}> = ({ datasets, labels }) => {
  const chartRef = useRef<HTMLDivElement>(null);
  const [dimensions, setDimensions] = useState({ width: 800, height: 460 });

  useEffect(() => {
    const updateDimensions = () => {
      if (chartRef.current) {
        const rect = chartRef.current.getBoundingClientRect();
        setDimensions({
          width: Math.max(rect.width, 800),
          height: Math.max(rect.height, 460),
        });
      }
    };

    updateDimensions();
    const resizeObserver = new ResizeObserver(updateDimensions);
    if (chartRef.current) {
      resizeObserver.observe(chartRef.current);
    }
    window.addEventListener("resize", updateDimensions);
    return () => {
      resizeObserver.disconnect();
      window.removeEventListener("resize", updateDimensions);
    };
  }, []);

  if (datasets.length === 0 || datasets[0].data.length === 0) {
    return (
      <ChartArea ref={chartRef}>
        <div style={{ textAlign: "center", padding: "100px 0", color: theme.colors.text.secondary }}>
          No data available
        </div>
      </ChartArea>
    );
  }

  const padding = { top: 20, right: 40, bottom: 40, left: 60 };
  const chartWidth = dimensions.width - padding.left - padding.right;
  const chartHeight = dimensions.height - padding.top - padding.bottom;

  const allValues = datasets.flatMap((d) => d.data);
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);
  const range = max - min || 1;

  const colorMap: Record<string, string> = {
    "CPU Usage (%)": "#1976d2",
    "Memory (%)": "#0d47a1",
    "Network (IOPS)": "#1565c0",
    "Throughput (RPS)": "#424242",
  };
  const lineWidth = 2;

  const normalizeY = (value: number) => {
    return chartHeight - ((value - min) / range) * chartHeight;
  };

  const getX = (index: number) => {
    return (index / (labels.length - 1)) * chartWidth;
  };

  const createPath = (data: number[]): string => {
    if (data.length === 0) return "";
    if (data.length === 1) {
      const x = getX(0);
      const y = normalizeY(data[0]);
      return `M ${x} ${y}`;
    }

    const points = data.map((value, index) => ({
      x: getX(index),
      y: normalizeY(value),
    }));

    let path = `M ${points[0].x} ${points[0].y}`;

    for (let i = 0; i < points.length - 1; i++) {
      const current = points[i];
      const next = points[i + 1];
      const afterNext = points[i + 2];

      if (afterNext) {
        const cp1x = current.x + (next.x - current.x) / 2;
        const cp1y = current.y;
        const cp2x = next.x - (afterNext.x - next.x) / 2;
        const cp2y = next.y;
        path += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${next.x} ${next.y}`;
      } else {
        const cp1x = current.x + (next.x - current.x) / 2;
        const cp1y = current.y;
        const cp2x = next.x;
        const cp2y = next.y;
        path += ` C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${next.x} ${next.y}`;
      }
    }

    return path;
  };

  const yAxisLabels: number[] = [];
  const yAxisCount = 8;
  for (let i = 0; i <= yAxisCount; i++) {
    const value = max - (range / yAxisCount) * i;
    yAxisLabels.push(value);
  }

  const xAxisLabels: string[] = [];
  const xAxisCount = 6;
  for (let i = 0; i <= xAxisCount; i++) {
    const idx = Math.floor((i / xAxisCount) * (labels.length - 1));
    xAxisLabels.push(labels[idx] || "");
  }

  return (
    <ChartArea ref={chartRef}>
      <SVGChart viewBox={`0 0 ${dimensions.width} ${dimensions.height}`} preserveAspectRatio="none">
        <defs>
          {datasets.map((dataset, idx) => {
            const color = colorMap[dataset.name] || theme.colors.status.info.text;
            return (
              <linearGradient key={idx} id={`gradient-${idx}`} x1="0%" y1="0%" x2="0%" y2="100%">
                <stop offset="0%" stopColor={color} stopOpacity="0.15" />
                <stop offset="100%" stopColor={color} stopOpacity="0" />
              </linearGradient>
            );
          })}
        </defs>
        <g transform={`translate(${padding.left}, ${padding.top})`}>
          {yAxisLabels.map((value, i) => {
            const y = normalizeY(value);
            return (
              <g key={`y-axis-${i}`}>
                <GridLine x1={0} y1={y} x2={chartWidth} y2={y} />
                <AxisLabel x={-10} y={y + 4} textAnchor="end">
                  {value.toFixed(1)}
                </AxisLabel>
              </g>
            );
          })}

          {xAxisLabels.map((label, i) => {
            const x = (i / xAxisCount) * chartWidth;
            return (
              <g key={`x-axis-${i}`}>
                <AxisLabel x={x} y={chartHeight + 20} textAnchor="middle">
                  {label.length > 5 ? label.substring(0, 5) : label}
                </AxisLabel>
              </g>
            );
          })}

          <AxisLine x1={0} y1={0} x2={0} y2={chartHeight} />
          <AxisLine x1={0} y1={chartHeight} x2={chartWidth} y2={chartHeight} />

          {datasets.map((dataset, idx) => {
            const path = createPath(dataset.data);
            const areaPath = path + ` L ${getX(dataset.data.length - 1)} ${chartHeight} L ${getX(0)} ${chartHeight} Z`;
            const color = colorMap[dataset.name] || theme.colors.status.info.text;
            const pathKey = `${dataset.name}-${idx}`;

            return (
              <g key={pathKey}>
                <ChartAreaPath
                  as="path"
                  d={areaPath}
                  fill={`url(#gradient-${idx})`}
                />
                <ChartPath
                  as="path"
                  d={path}
                  fill="none"
                  stroke={color}
                  strokeWidth={lineWidth}
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </g>
            );
          })}
        </g>
      </SVGChart>
    </ChartArea>
  );
};

const RecentEventsList = styled.div`
  display: flex;
  flex-direction: column;
  gap: ${theme.spacing.sm};
`;

const RecentEventCard = styled.div`
  background: ${theme.colors.background.secondary};
  border: 1px solid ${theme.colors.border.light};
  border-radius: ${theme.borderRadius.md};
  padding: ${theme.spacing.md};
  transition: all ${theme.transitions.normal};
  animation: ${fadeIn} 0.3s ease-out;
  
  &:hover {
    border-color: ${theme.colors.primary.main};
    box-shadow: ${theme.shadows.sm};
    transform: translateX(4px);
  }
`;

const EventHeader = styled.div`
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: ${theme.spacing.sm};
`;

const EventTime = styled.div`
  font-size: 0.75em;
  color: ${theme.colors.text.secondary};
  font-family: 'Courier New', monospace;
`;

const EventInfo = styled.div`
  display: grid;
  grid-template-columns: 100px 1fr;
  gap: ${theme.spacing.sm};
  font-size: 0.9em;
  margin-top: ${theme.spacing.xs};
`;

const EventLabel = styled.div`
  color: ${theme.colors.text.secondary};
  font-weight: 500;
`;

const EventValue = styled.div`
  color: ${theme.colors.text.primary};
`;

const SectionTitle = styled.h3`
  font-size: 1.1em;
  font-weight: 600;
  color: ${theme.colors.text.primary};
  margin: 0 0 ${theme.spacing.md} 0;
  padding-bottom: ${theme.spacing.sm};
  border-bottom: 2px solid ${theme.colors.border.light};
`;

const UnifiedMonitor: React.FC = () => {
  const location = useLocation();
  const getInitialTab = (): 'monitor' | 'live' | 'performance' | 'system' => {
    if (location.pathname.includes('live-changes')) return 'live';
    if (location.pathname.includes('query-performance')) return 'performance';
    return 'monitor';
  };
  const [activeTab, setActiveTab] = useState<'monitor' | 'live' | 'performance' | 'system'>(getInitialTab());
  
  useEffect(() => {
    setActiveTab(getInitialTab());
  }, [location.pathname]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [selectedItem, setSelectedItem] = useState<any | null>(null);
  
  const [activeQueries, setActiveQueries] = useState<any[]>([]);
  const [processingLogs, setProcessingLogs] = useState<any[]>([]);
  const [processingStats, setProcessingStats] = useState<any>({});
  const [queryPerformance, setQueryPerformance] = useState<any[]>([]);
  const [performanceMetrics, setPerformanceMetrics] = useState<any>({});
  
  const [resourceHistory, setResourceHistory] = useState<{
    timestamp: string[];
    cpuUsage: number[];
    memoryPercentage: number[];
    network: number[];
    throughput: number[];
  }>({
    timestamp: [],
    cpuUsage: [],
    memoryPercentage: [],
    network: [],
    throughput: [],
  });
  const [visibleLines, setVisibleLines] = useState<{
    cpu: boolean;
    memory: boolean;
    network: boolean;
    throughput: boolean;
  }>({
    cpu: true,
    memory: true,
    network: true,
    throughput: true,
  });
  const [systemResources, setSystemResources] = useState<any>({});
  
  const isMountedRef = useRef(true);

  const extractSchemaTable = useCallback((query: string) => {
    if (!query) return { schema: 'N/A', table: 'N/A' };
    
    const schemaMatch = query.match(/(?:FROM|JOIN|INTO|UPDATE)\s+(?:(\w+)\.)?(\w+)/i);
    if (schemaMatch) {
      return {
        schema: schemaMatch[1] || 'public',
        table: schemaMatch[2] || 'N/A'
      };
    }
    return { schema: 'N/A', table: 'N/A' };
  }, []);

  const fetchMonitorData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      const [queries, logs, stats] = await Promise.all([
        monitorApi.getActiveQueries(),
        monitorApi.getProcessingLogs(1, 100),
        monitorApi.getProcessingStats()
      ]);
      if (isMountedRef.current) {
        const enrichedQueries = (queries || []).map((q: any) => {
          const { schema, table } = extractSchemaTable(q.query || '');
          return {
            ...q,
            schema_name: q.schema_name || schema,
            table_name: q.table_name || table
          };
        });
        setActiveQueries(enrichedQueries);
        setProcessingLogs(logs.data || []);
        setProcessingStats(stats || {});
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [extractSchemaTable]);

  const fetchPerformanceData = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      const [queries, metrics] = await Promise.all([
        queryPerformanceApi.getQueries({ page: 1, limit: 100 }),
        queryPerformanceApi.getMetrics()
      ]);
      if (isMountedRef.current) {
        setQueryPerformance(queries.data || []);
        setPerformanceMetrics(metrics || {});
      }
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, []);

  const fetchSystemResources = useCallback(async () => {
    if (!isMountedRef.current) return;
    try {
      const dashboardData = await dashboardApi.getDashboardStats();
      if (isMountedRef.current) {
        setSystemResources(dashboardData.systemResources || {});
        const now = new Date();
        const timeLabel = `${now.getHours().toString().padStart(2, "0")}:${now.getMinutes().toString().padStart(2, "0")}:${now.getSeconds().toString().padStart(2, "0")}`;
        const networkValue = dashboardData.metricsCards?.currentIops || 0;
        const throughputValue = dashboardData.metricsCards?.currentThroughput?.avgRps || 0;
        
        setResourceHistory((prev) => {
          const maxPoints = 60;
          return {
            timestamp: [...prev.timestamp, timeLabel].slice(-maxPoints),
            cpuUsage: [
              ...prev.cpuUsage,
              parseFloat(dashboardData.systemResources?.cpuUsage || "0") || 0,
            ].slice(-maxPoints),
            memoryPercentage: [
              ...prev.memoryPercentage,
              parseFloat(dashboardData.systemResources?.memoryPercentage || "0") || 0,
            ].slice(-maxPoints),
            network: [
              ...prev.network,
              networkValue,
            ].slice(-maxPoints),
            throughput: [
              ...prev.throughput,
              throughputValue,
            ].slice(-maxPoints),
          };
        });
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching system resources:", err);
      }
    }
  }, []);

  const fetchSystemLogsHistory = useCallback(async () => {
    try {
      if (!isMountedRef.current) return;
      const logs = await dashboardApi.getSystemLogs(60);
      if (isMountedRef.current && logs.length > 0) {
        setResourceHistory({
          timestamp: logs.map((log: any) => log.timestamp),
          cpuUsage: logs.map((log: any) => log.cpuUsage),
          memoryPercentage: logs.map((log: any) => log.memoryPercentage),
          network: logs.map((log: any) => log.network),
          throughput: logs.map((log: any) => log.throughput),
        });
      }
    } catch (err) {
      if (isMountedRef.current) {
        console.error("Error fetching system logs history:", err);
      }
    }
  }, []);

  useEffect(() => {
    isMountedRef.current = true;
    setLoading(true);
    
    const loadData = async () => {
      await Promise.all([fetchMonitorData(), fetchPerformanceData()]);
      if (activeTab === 'system') {
        await fetchSystemLogsHistory();
      }
      if (isMountedRef.current) {
        setLoading(false);
      }
    };
    
    loadData();
    
    const interval = setInterval(() => {
      if (isMountedRef.current) {
        fetchMonitorData();
        if (activeTab === 'performance') {
          fetchPerformanceData();
        }
        if (activeTab === 'system') {
          fetchSystemResources();
        }
      }
    }, 5000);
    
    const systemInterval = activeTab === 'system' ? setInterval(fetchSystemResources, 10000) : null;
    
    return () => {
      isMountedRef.current = false;
      clearInterval(interval);
      if (systemInterval) {
        clearInterval(systemInterval);
      }
    };
  }, [fetchMonitorData, fetchPerformanceData, fetchSystemResources, fetchSystemLogsHistory, activeTab]);

  const treeData = useMemo(() => {
    if (activeTab === 'monitor') {
      const databases = new Map<string, any[]>();
      activeQueries.forEach(query => {
        const db = query.datname || 'Unknown';
        if (!databases.has(db)) {
          databases.set(db, []);
        }
        databases.get(db)!.push(query);
      });
      return databases;
    } else if (activeTab === 'live') {
      const databases = new Map<string, Map<string, any[]>>();
      processingLogs.forEach(log => {
        const db = log.db_engine || 'Unknown';
        const schema = log.schema_name || 'public';
        if (!databases.has(db)) {
          databases.set(db, new Map());
        }
        const schemas = databases.get(db)!;
        if (!schemas.has(schema)) {
          schemas.set(schema, []);
        }
        schemas.get(schema)!.push(log);
      });
      return databases;
    } else {
      const databases = new Map<string, any[]>();
      queryPerformance.forEach(query => {
        const db = query.dbname || 'Unknown';
        if (!databases.has(db)) {
          databases.set(db, []);
        }
        databases.get(db)!.push(query);
      });
      return databases;
    }
  }, [activeTab, activeQueries, processingLogs, queryPerformance]);

  const toggleNode = useCallback((key: string) => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }, []);

  const formatTime = useCallback((ms: number | string | null | undefined) => {
    if (!ms) return 'N/A';
    const numMs = Number(ms);
    if (isNaN(numMs)) return 'N/A';
    if (numMs < 1) return `${(numMs * 1000).toFixed(2)}μs`;
    if (numMs < 1000) return `${numMs.toFixed(2)}ms`;
    return `${(numMs / 1000).toFixed(2)}s`;
  }, []);

  const formatDate = useCallback((dateString: string) => {
    return new Date(dateString).toLocaleString();
  }, []);

  const handleKillQuery = useCallback(async (pid: number) => {
    if (!confirm(`Are you sure you want to kill query with PID ${pid}?`)) {
      return;
    }
    try {
      const response = await fetch(`/api/monitor/queries/${pid}/kill`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('authToken')}`
        }
      });
      
      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.error || error.details || 'Failed to kill query');
      }
      
      alert(`Query with PID ${pid} has been terminated.`);
      fetchMonitorData();
    } catch (err) {
      if (isMountedRef.current) {
        setError(extractApiError(err));
      }
    }
  }, [fetchMonitorData]);

  const renderTree = () => {
    if (treeData.size === 0) {
      return (
        <EmptyState>
          No data available for {activeTab}
        </EmptyState>
      );
    }

    const entries = Array.from(treeData.entries() as any) as [string, any][];
    return entries.map(([dbName, data]) => {
      const dbKey = `db-${dbName}`;
      const isDbExpanded = expandedNodes.has(dbKey);
      
      let items: any[];
      if (activeTab === 'live') {
        const schemas = data as Map<string, any[]>;
        items = Array.from(schemas.values()).flat();
      } else {
        items = data as any[];
      }
      
      const totalItems = items.length;

      return (
        <TreeNode key={dbKey}>
          <TreeContent
            $level={0}
            $isExpanded={isDbExpanded}
            $nodeType="database"
            onClick={() => toggleNode(dbKey)}
          >
            <ExpandIconContainer $isExpanded={isDbExpanded}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="3">
                <polyline points="9 18 15 12 9 6"/>
              </svg>
            </ExpandIconContainer>
            <NodeLabel $isDatabase>{dbName}</NodeLabel>
            <CountBadge>{totalItems}</CountBadge>
          </TreeContent>
          <ExpandableContent $isExpanded={isDbExpanded} $level={0}>
            {isDbExpanded && items.map((item: any, idx: number) => {
              const isSelected = selectedItem === item;
              
              return (
                <QueryItem
                  key={idx}
                  $selected={isSelected}
                  onClick={() => setSelectedItem(item)}
                >
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: '8px' }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      {activeTab === 'monitor' && (
                        <>
                          <div style={{ fontWeight: 500, color: theme.colors.text.primary }}>
                            PID: {item.pid}
                          </div>
                          <div style={{ fontSize: '0.85em', color: theme.colors.text.secondary, marginTop: '4px' }}>
                            Schema: {item.schema_name || 'N/A'} | Table: {item.table_name || 'N/A'}
                          </div>
                          <QueryPreview>{item.query?.substring(0, 60)}...</QueryPreview>
                        </>
                      )}
                      {activeTab === 'live' && (
                        <>
                          <div style={{ fontWeight: 500, color: theme.colors.text.primary }}>
                            {item.table_name || 'N/A'}
                          </div>
                          <QueryPreview>{item.db_engine} - {formatDate(item.processed_at)}</QueryPreview>
                        </>
                      )}
                      {activeTab === 'performance' && (
                        <>
                          <div style={{ fontWeight: 500, color: theme.colors.text.primary }}>
                            {formatTime(item.mean_time_ms || item.query_duration_ms)}
                          </div>
                          <QueryPreview>{item.query_text?.substring(0, 60)}...</QueryPreview>
                        </>
                      )}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      {activeTab === 'monitor' && (
                        <>
                          <StatusBadge $status={item.state}>{item.state}</StatusBadge>
                          <KillButton
                            onClick={(e: React.MouseEvent) => {
                              e.stopPropagation();
                              handleKillQuery(item.pid);
                            }}
                            title="Kill Query"
                          >
                            Kill
                          </KillButton>
                        </>
                      )}
                      {activeTab === 'live' && (
                        <StatusBadge $status={item.status}>{item.status}</StatusBadge>
                      )}
                      {activeTab === 'performance' && (
                        <StatusBadge $status={item.performance_tier || 'N/A'}>
                          {item.performance_tier || 'N/A'}
                        </StatusBadge>
                      )}
                    </div>
                  </div>
                </QueryItem>
              );
            })}
          </ExpandableContent>
        </TreeNode>
      );
    });
  };

  const renderDetails = () => {
    if (activeTab === 'live') {
      const recentEvents = [...processingLogs]
        .sort((a, b) => new Date(b.processed_at || 0).getTime() - new Date(a.processed_at || 0).getTime())
        .slice(0, 20);

      if (recentEvents.length === 0) {
        return (
          <EmptyDetails>
            No recent events available
          </EmptyDetails>
        );
      }

      return (
        <>
          <SectionTitle>Current Changes</SectionTitle>
          <RecentEventsList>
            {recentEvents.map((event, idx) => (
              <RecentEventCard key={idx}>
                <EventHeader>
                  <StatusBadge $status={event.status}>{event.status}</StatusBadge>
                  <EventTime>{formatDate(event.processed_at)}</EventTime>
                </EventHeader>
                <EventInfo>
                  <EventLabel>Database:</EventLabel>
                  <EventValue>{event.db_engine || 'N/A'}</EventValue>
                  
                  <EventLabel>Schema:</EventLabel>
                  <EventValue>{event.schema_name || 'N/A'}</EventValue>
                  
                  <EventLabel>Table:</EventLabel>
                  <EventValue>{event.table_name || 'N/A'}</EventValue>
                  
                  <EventLabel>Strategy:</EventLabel>
                  <EventValue>{event.pk_strategy || 'N/A'}</EventValue>
                  
                  {event.new_pk && (
                    <>
                      <EventLabel>Last PK:</EventLabel>
                      <EventValue>{event.new_pk}</EventValue>
                    </>
                  )}
                  
                  {event.record_count !== null && event.record_count !== undefined && (
                    <>
                      <EventLabel>Records:</EventLabel>
                      <EventValue>{event.record_count.toLocaleString()}</EventValue>
                    </>
                  )}
                </EventInfo>
              </RecentEventCard>
            ))}
          </RecentEventsList>
        </>
      );
    }

    if (!selectedItem) {
      return (
        <EmptyDetails>
          Select an item to view details
        </EmptyDetails>
      );
    }

    if (activeTab === 'monitor') {
      return (
        <DetailsCard>
          <DetailsTitle>Query Details</DetailsTitle>
          <DetailsGrid>
            <DetailLabel>PID:</DetailLabel>
            <DetailValue>{selectedItem.pid}</DetailValue>
            
            <DetailLabel>User:</DetailLabel>
            <DetailValue>{selectedItem.usename}</DetailValue>
            
            <DetailLabel>Database:</DetailLabel>
            <DetailValue>{selectedItem.datname}</DetailValue>
            
            <DetailLabel>State:</DetailLabel>
            <DetailValue>
              <StatusBadge $status={selectedItem.state}>{selectedItem.state}</StatusBadge>
            </DetailValue>
            
            <DetailLabel>Duration:</DetailLabel>
            <DetailValue>{selectedItem.duration}</DetailValue>
            
            <DetailLabel>Application:</DetailLabel>
            <DetailValue>{selectedItem.application_name || '-'}</DetailValue>
            
            <DetailLabel>Client Address:</DetailLabel>
            <DetailValue>{selectedItem.client_addr || '-'}</DetailValue>
            
            <DetailLabel>Started At:</DetailLabel>
            <DetailValue>{formatDate(selectedItem.query_start)}</DetailValue>
            
            <DetailLabel>Wait Event:</DetailLabel>
            <DetailValue>
              {selectedItem.wait_event_type ? `${selectedItem.wait_event_type} (${selectedItem.wait_event})` : 'None'}
            </DetailValue>
          </DetailsGrid>
          <QueryText>{selectedItem.query || 'N/A'}</QueryText>
        </DetailsCard>
      );
    } else {
      return (
        <DetailsCard>
          <DetailsTitle>Query Performance Details</DetailsTitle>
          <DetailsGrid>
            <DetailLabel>Query ID:</DetailLabel>
            <DetailValue>{selectedItem.queryid || 'N/A'}</DetailValue>
            
            <DetailLabel>Database:</DetailLabel>
            <DetailValue>{selectedItem.dbname || 'N/A'}</DetailValue>
            
            <DetailLabel>Performance Tier:</DetailLabel>
            <DetailValue>
              <StatusBadge $status={selectedItem.performance_tier || 'N/A'}>
                {selectedItem.performance_tier || 'N/A'}
              </StatusBadge>
            </DetailValue>
            
            <DetailLabel>Operation Type:</DetailLabel>
            <DetailValue>{selectedItem.operation_type || 'N/A'}</DetailValue>
            
            <DetailLabel>Mean Time:</DetailLabel>
            <DetailValue>{formatTime(selectedItem.mean_time_ms)}</DetailValue>
            
            <DetailLabel>Total Time:</DetailLabel>
            <DetailValue>{formatTime(selectedItem.total_time_ms)}</DetailValue>
            
            <DetailLabel>Calls:</DetailLabel>
            <DetailValue>{selectedItem.calls?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Efficiency Score:</DetailLabel>
            <DetailValue>
              {selectedItem.query_efficiency_score ? `${Number(selectedItem.query_efficiency_score).toFixed(2)}%` : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Cache Hit Ratio:</DetailLabel>
            <DetailValue>
              {selectedItem.cache_hit_ratio ? `${Number(selectedItem.cache_hit_ratio).toFixed(2)}%` : 'N/A'}
            </DetailValue>
            
            <DetailLabel>Rows Returned:</DetailLabel>
            <DetailValue>{selectedItem.rows_returned?.toLocaleString() || 'N/A'}</DetailValue>
            
            <DetailLabel>Long Running:</DetailLabel>
            <DetailValue>{selectedItem.is_long_running ? 'Yes' : 'No'}</DetailValue>
            
            <DetailLabel>Blocking:</DetailLabel>
            <DetailValue>{selectedItem.is_blocking ? 'Yes' : 'No'}</DetailValue>
            
            <DetailLabel>Captured At:</DetailLabel>
            <DetailValue>{formatDate(selectedItem.captured_at)}</DetailValue>
          </DetailsGrid>
          <QueryText>{selectedItem.query_text || 'N/A'}</QueryText>
        </DetailsCard>
      );
    }
  };

  const renderSystemResources = () => {
    return (
      <DetailsPanel>
        <ChartContainer>
          <ChartTitle>
            <LegendItem
              $lineStyle="solid"
              $active={visibleLines.cpu}
            >
              <Checkbox
                type="checkbox"
                checked={visibleLines.cpu}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => {
                  setVisibleLines((prev) => ({ ...prev, cpu: e.target.checked }));
                }}
              />
              <LegendLine $color="#1976d2" $active={visibleLines.cpu} />
              CPU Usage (%)
            </LegendItem>
            <LegendItem
              $lineStyle="solid"
              $active={visibleLines.memory}
            >
              <Checkbox
                type="checkbox"
                checked={visibleLines.memory}
                onChange={(e) => {
                  setVisibleLines((prev) => ({ ...prev, memory: e.target.checked }));
                }}
              />
              <LegendLine $color="#0d47a1" $active={visibleLines.memory} />
              Memory (%)
            </LegendItem>
            <LegendItem
              $lineStyle="solid"
              $active={visibleLines.network}
            >
              <Checkbox
                type="checkbox"
                checked={visibleLines.network}
                onChange={(e) => {
                  setVisibleLines((prev) => ({ ...prev, network: e.target.checked }));
                }}
              />
              <LegendLine $color="#1565c0" $active={visibleLines.network} />
              Network (IOPS)
            </LegendItem>
            <LegendItem
              $lineStyle="solid"
              $active={visibleLines.throughput}
            >
              <Checkbox
                type="checkbox"
                checked={visibleLines.throughput}
                onChange={(e) => {
                  setVisibleLines((prev) => ({ ...prev, throughput: e.target.checked }));
                }}
              />
              <LegendLine $color="#424242" $active={visibleLines.throughput} />
              Throughput (RPS)
            </LegendItem>
            <ShowAllButton
              onClick={() => {
                const allVisible = Object.values(visibleLines).every((v) => v);
                setVisibleLines({
                  cpu: !allVisible,
                  memory: !allVisible,
                  network: !allVisible,
                  throughput: !allVisible,
                });
              }}
            >
              {Object.values(visibleLines).every((v) => v) ? "Clear All" : "Show All"}
            </ShowAllButton>
          </ChartTitle>
          <SystemResourcesChart
            datasets={[
              ...(visibleLines.cpu
                ? [
                    {
                      data: resourceHistory.cpuUsage,
                      symbol: "●",
                      name: "CPU Usage (%)",
                    },
                  ]
                : []),
              ...(visibleLines.memory
                ? [
                    {
                      data: resourceHistory.memoryPercentage,
                      symbol: "▲",
                      name: "Memory (%)",
                    },
                  ]
                : []),
              ...(visibleLines.network
                ? [
                    {
                      data: resourceHistory.network,
                      symbol: "◆",
                      name: "Network (IOPS)",
                    },
                  ]
                : []),
              ...(visibleLines.throughput
                ? [
                    {
                      data: resourceHistory.throughput,
                      symbol: "■",
                      name: "Throughput (RPS)",
                    },
                  ]
                : []),
            ]}
            labels={resourceHistory.timestamp}
          />
        </ChartContainer>
        
        <StatsContainer>
          <StatCard>
            <StatValue>{systemResources.cpuUsage || '0.0'}%</StatValue>
            <StatLabel>CPU Usage</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{systemResources.memoryPercentage || '0.0'}%</StatValue>
            <StatLabel>Memory</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{systemResources.memoryUsed || '0.0'} GB</StatValue>
            <StatLabel>Memory Used</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{systemResources.memoryTotal || '0.0'} GB</StatValue>
            <StatLabel>Memory Total</StatLabel>
          </StatCard>
        </StatsContainer>
      </DetailsPanel>
    );
  };

  const getStats = () => {
    if (activeTab === 'monitor') {
      return (
        <StatsContainer>
          <StatCard>
            <StatValue>{activeQueries.length}</StatValue>
            <StatLabel>Active Queries</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.total || 0}</StatValue>
            <StatLabel>Total Events</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.listeningChanges || 0}</StatValue>
            <StatLabel>Listening</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.errors || 0}</StatValue>
            <StatLabel>Errors</StatLabel>
          </StatCard>
        </StatsContainer>
      );
    } else if (activeTab === 'live') {
      return (
        <StatsContainer>
          <StatCard>
            <StatValue>{processingStats.total || 0}</StatValue>
            <StatLabel>Total Events</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.last24h || 0}</StatValue>
            <StatLabel>Last 24h</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.listeningChanges || 0}</StatValue>
            <StatLabel>Listening</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.fullLoad || 0}</StatValue>
            <StatLabel>Full Load</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{processingStats.errors || 0}</StatValue>
            <StatLabel>Errors</StatLabel>
          </StatCard>
        </StatsContainer>
      );
    } else {
      return (
        <StatsContainer>
          <StatCard>
            <StatValue>{performanceMetrics.total_queries || 0}</StatValue>
            <StatLabel>Total Queries</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{performanceMetrics.excellent_count || 0}</StatValue>
            <StatLabel>Excellent</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{performanceMetrics.good_count || 0}</StatValue>
            <StatLabel>Good</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{performanceMetrics.fair_count || 0}</StatValue>
            <StatLabel>Fair</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>{performanceMetrics.poor_count || 0}</StatValue>
            <StatLabel>Poor</StatLabel>
          </StatCard>
          <StatCard>
            <StatValue>
              {performanceMetrics.avg_efficiency 
                ? `${Number(performanceMetrics.avg_efficiency).toFixed(1)}%` 
                : 'N/A'}
            </StatValue>
            <StatLabel>Avg Efficiency</StatLabel>
          </StatCard>
        </StatsContainer>
      );
    }
  };

  if (loading && activeQueries.length === 0 && processingLogs.length === 0 && queryPerformance.length === 0) {
    return (
      <Container>
        <Header>Monitor</Header>
        <LoadingOverlay>Loading monitor data...</LoadingOverlay>
      </Container>
    );
  }

  return (
    <Container>
      <Header>Monitor</Header>
      
      {error && <ErrorMessage>{error}</ErrorMessage>}
      
      <TabContainer>
        <Tab $active={activeTab === 'monitor'} onClick={() => setActiveTab('monitor')}>
          Monitor
        </Tab>
        <Tab $active={activeTab === 'live'} onClick={() => setActiveTab('live')}>
          Live Changes
        </Tab>
        <Tab $active={activeTab === 'performance'} onClick={() => setActiveTab('performance')}>
          Query Performance
        </Tab>
        <Tab $active={activeTab === 'system'} onClick={() => setActiveTab('system')}>
          System Resources
        </Tab>
      </TabContainer>
      
      {activeTab !== 'system' && getStats()}
      
      {activeTab === 'system' ? (
        renderSystemResources()
      ) : (
        <MainLayout>
          <TreePanel>
            <TreeContainer>
              {renderTree()}
            </TreeContainer>
          </TreePanel>
          
          <DetailsPanel>
            {renderDetails()}
          </DetailsPanel>
        </MainLayout>
      )}
    </Container>
  );
};

export default UnifiedMonitor;
