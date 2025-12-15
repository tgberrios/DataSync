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

describe("Custom Jobs Endpoints", () => {
  describe("GET /api/custom-jobs", () => {
    it("should require authentication", async () => {
      const response = await request(app).get("/api/custom-jobs");
      expect(response.status).toBe(401);
    });

    it("should return custom jobs data", async () => {
      const response = await request(app)
        .get("/api/custom-jobs")
        .set("Authorization", `Bearer ${authToken}`)
        .query({ page: 1, limit: 10 });

      expect(response.status).toBe(200);
      expect(response.body).toHaveProperty("data");
      expect(response.body).toHaveProperty("pagination");
    });
  });

  describe("POST /api/custom-jobs", () => {
    it("should require authentication", async () => {
      const response = await request(app).post("/api/custom-jobs").send({});
      expect(response.status).toBe(401);
    });

    it("should validate required fields", async () => {
      const response = await request(app)
        .post("/api/custom-jobs")
        .set("Authorization", `Bearer ${authToken}`)
        .send({ job_name: "test-job" }); // Missing required fields

      expect(response.status).toBe(400);
      expect(response.body.error).toBeDefined();
    });

    it("should validate db_engine enums", async () => {
      const response = await request(app)
        .post("/api/custom-jobs")
        .set("Authorization", `Bearer ${authToken}`)
        .send({
          job_name: "test-job",
          source_db_engine: "INVALID",
          target_db_engine: "PostgreSQL",
          target_schema: "public",
          target_table: "test",
        });

      expect(response.status).toBe(400);
    });
  });
});
