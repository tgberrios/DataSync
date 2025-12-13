export const theme = {
  colors: {
    primary: {
      main: "#0d1b2a",
      light: "#1e3a5f",
      dark: "#2d4a6f",
      hover: "#1e3a5f",
    },
    background: {
      main: "#ffffff",
      secondary: "#f5f5f5",
      tertiary: "#fafafa",
      dark: "#1a1a1a",
      sidebar: "#1a1a1a",
    },
    text: {
      primary: "#333333",
      secondary: "#666666",
      light: "#888888",
      white: "#ffffff",
    },
    border: {
      light: "#eeeeee",
      medium: "#dddddd",
      dark: "#333333",
    },
    status: {
      error: {
        bg: "#ffebee",
        text: "#c62828",
      },
      success: {
        bg: "#e8f5e9",
        text: "#2e7d32",
      },
      warning: {
        bg: "#fff3e0",
        text: "#ef6c00",
      },
      info: {
        bg: "#e3f2fd",
        text: "#1976d2",
      },
      skip: {
        bg: "#f5f5f5",
        text: "#666666",
      },
    },
    gradient: {
      primary: "linear-gradient(135deg, #f5f5f5 0%, #ffffff 50%, #f5f5f5 100%)",
      sidebar: "linear-gradient(180deg, #1a1a1a 0%, #1a1a1a 100%)",
      navHover:
        "linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.3) 100%)",
      navActive:
        "linear-gradient(90deg, #252525 0%, rgba(10, 25, 41, 0.5) 100%)",
      shimmer:
        "linear-gradient(90deg, transparent, rgba(10, 25, 41, 0.1), transparent)",
    },
  },
  spacing: {
    xs: "5px",
    sm: "10px",
    md: "15px",
    lg: "20px",
    xl: "25px",
    xxl: "30px",
  },
  borderRadius: {
    sm: "4px",
    md: "6px",
    lg: "8px",
  },
  shadows: {
    sm: "0 2px 4px rgba(0, 0, 0, 0.03)",
    md: "0 2px 8px rgba(0, 0, 0, 0.05)",
    lg: "0 4px 12px rgba(0, 0, 0, 0.08)",
    xl: "0 8px 32px rgba(0, 0, 0, 0.2)",
  },
  transitions: {
    fast: "0.15s ease",
    normal: "0.2s ease",
    slow: "0.3s ease",
  },
  fonts: {
    primary: "monospace",
  },
  breakpoints: {
    sm: "640px",
    md: "768px",
    lg: "1024px",
    xl: "1280px",
  },
  zIndex: {
    dropdown: 100,
    modal: 1000,
    overlay: 999,
  },
} as const;

export type Theme = typeof theme;
