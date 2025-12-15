import { describe, it, expect } from "@jest/globals";
import {
  sanitizeSearch,
  validatePage,
  validateLimit,
  validateBoolean,
  validateIdentifier,
  validateEnum,
} from "../server-utils/validation.js";

describe("Validation Functions", () => {
  describe("sanitizeSearch", () => {
    it("should return empty string for null/undefined", () => {
      expect(sanitizeSearch(null)).toBe("");
      expect(sanitizeSearch(undefined)).toBe("");
    });

    it("should trim and limit string length", () => {
      expect(sanitizeSearch("  test  ")).toBe("test");
      const longString = "a".repeat(200);
      expect(sanitizeSearch(longString, 100).length).toBe(100);
    });
  });

  describe("validatePage", () => {
    it("should return default min value for invalid input", () => {
      expect(validatePage(null, 1)).toBe(1);
      expect(validatePage("invalid", 1)).toBe(1);
    });

    it("should parse string numbers correctly", () => {
      expect(validatePage("5", 1)).toBe(5);
      expect(validatePage("0", 1)).toBe(1); // Should respect min
    });
  });

  describe("validateLimit", () => {
    it("should respect min and max bounds", () => {
      expect(validateLimit(0, 1, 100)).toBe(1);
      expect(validateLimit(200, 1, 100)).toBe(100);
      expect(validateLimit(50, 1, 100)).toBe(50);
    });
  });

  describe("validateBoolean", () => {
    it("should parse string booleans correctly", () => {
      expect(validateBoolean("true")).toBe(true);
      expect(validateBoolean("false")).toBe(false);
      expect(validateBoolean("1")).toBe(true);
      expect(validateBoolean("0")).toBe(false);
    });

    it("should return default for invalid input", () => {
      expect(validateBoolean("invalid", true)).toBe(true);
      expect(validateBoolean(null, false)).toBe(false);
    });
  });

  describe("validateIdentifier", () => {
    it("should return null for invalid input", () => {
      expect(validateIdentifier(null)).toBeNull();
      expect(validateIdentifier("")).toBeNull();
    });

    it("should return lowercase trimmed string", () => {
      expect(validateIdentifier("  TestSchema  ")).toBe("testschema");
    });

    it("should respect max length", () => {
      const longId = "a".repeat(200);
      expect(validateIdentifier(longId, 100)).toBeNull();
    });
  });

  describe("validateEnum", () => {
    const allowedValues = ["OPTION1", "OPTION2", "OPTION3"];

    it("should return valid value if in allowed list", () => {
      expect(validateEnum("OPTION1", allowedValues, "DEFAULT")).toBe("OPTION1");
    });

    it("should return default for invalid value", () => {
      expect(validateEnum("INVALID", allowedValues, "DEFAULT")).toBe("DEFAULT");
      expect(validateEnum(null, allowedValues, "DEFAULT")).toBe("DEFAULT");
    });
  });
});
