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

  const loginResponse = await request(app)
    .post("/api/auth/login")
    .send({ username: "admin", password: "admin123" });
  authToken = loginResponse.body.token;
});

describe("API Catalog Endpoints", () => {
  describe("GET /api/api-catalog", () => {
    it("should require authentication", async () => {
      const response = await request(app).get("/api/api-catalog");
      expect(response.status).toBe(401);
    });

    it("should return API catalog data", async () => {
      const response = await request(app)
        .get("/api/api-catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .query({ page: 1, limit: 10 });

      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty("data");
      expect(response.body).toHaveProperty("pagination");
    });

    it("should validate filters", async () => {
      const response = await request(app)
        .get("/api/api-catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .query({
          api_type: "INVALID_TYPE",
          status: "INVALID_STATUS",
        });

      // Should still return 200 but filter out invalid values
      expect(response.status).toBe(200);
    });
  });

  describe("POST /api/api-catalog", () => {
    it("should require authentication", async () => {
      const response = await request(app).post("/api/api-catalog").send({});
      expect(response.status).toBe(401);
    });

    it("should validate required fields", async () => {
      const response = await request(app)
        .post("/api/api-catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .send({ api_name: "test" }); // Missing required fields

      expect(response.status).toBe(400);
      expect(response.body.error).toBeDefined();
    });

    it("should validate api_type enum", async () => {
      const response = await request(app)
        .post("/api/api-catalog")
        .set("Authorization", `Bearer ${authToken}`)
        .send({
          api_name: "test-api",
          api_type: "INVALID",
          base_url: "https://api.example.com",
          endpoint: "/test",
          http_method: "GET",
          auth_type: "NONE",
          target_db_engine: "PostgreSQL",
          target_connection_string: "postgresql://localhost/test",
          target_schema: "public",
          target_table: "test",
        });

      expect(response.status).toBe(400);
    });
  });
});
