import request from "supertest";
import { describe, it, expect, beforeAll } from "@jest/globals";

process.env.NODE_ENV = "test";
process.env.JWT_SECRET = "test-secret-key-for-testing";
process.env.DEFAULT_ADMIN_PASSWORD = "admin123";
process.env.POSTGRES_HOST = process.env.POSTGRES_HOST || "localhost";
process.env.POSTGRES_PORT = process.env.POSTGRES_PORT || "5432";
process.env.POSTGRES_DATABASE = process.env.POSTGRES_DATABASE || "DataLake";
process.env.POSTGRES_USER = process.env.POSTGRES_USER || "postgres";
process.env.POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || "";

let app;
let authToken;

beforeAll(async () => {
  const serverModule = await import("../server.js");
  app = serverModule.default;

  // Get auth token
  const loginResponse = await request(app)
    .post("/api/auth/login")
    .send({ username: "admin", password: "admin123" });
  authToken = loginResponse.body.token;
});

describe("Catalog Endpoints", () => {
  describe("GET /api/catalog", () => {
    it("should require authentication", async () => {
      const response = await request(app).get("/api/catalog");
      expect(response.status).toBe(401);
    });

    it("should return catalog data with valid auth", async () => {
      const response = await request(app)
        .get("/api/catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .query({ page: 1, limit: 10 });

      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty("data");
      expect(response.body).toHaveProperty("pagination");
      expect(Array.isArray(response.body.data)).toBe(true);
    });

    it("should validate page parameter", async () => {
      const response = await request(app)
        .get("/api/catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .query({ page: "invalid", limit: 10 });

      expect(response.status).toBe(200);
      expect(response.body.pagination.currentPage).toBe(1); // Should default to 1
    });

    it("should validate limit parameter", async () => {
      const response = await request(app)
        .get("/api/catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .query({ page: 1, limit: 500 }); // Exceeds max

      expect(response.status).toBe(200);
      expect(response.body.pagination.limit).toBeLessThanOrEqual(100);
    });
  });

  describe("POST /api/catalog", () => {
    it("should require authentication", async () => {
      const response = await request(app)
        .post("/api/catalog")
        .send({
          schema_name: "test",
          table_name: "test",
          db_engine: "PostgreSQL",
        });

      expect(response.status).toBe(401);
    });

    it("should validate required fields", async () => {
      const response = await request(app)
        .post("/api/catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .send({ schema_name: "test" }); // Missing required fields

      expect(response.status).toBe(400);
      expect(response.body.error).toBeDefined();
    });

    it("should validate db_engine enum", async () => {
      const response = await request(app)
        .post("/api/catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .send({
          schema_name: "test",
          table_name: "test",
          db_engine: "INVALID_ENGINE",
          connection_string: "postgresql://localhost/test",
        });

      expect(response.status).toBe(400);
    });
  });
});
