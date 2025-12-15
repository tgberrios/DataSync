import request from "supertest";
import { describe, it, expect, beforeAll } from "@jest/globals";

// Mock environment variables before importing server
process.env.NODE_ENV = "test";
process.env.JWT_SECRET = "test-secret-key-for-testing";
process.env.DEFAULT_ADMIN_PASSWORD = "admin123";
process.env.POSTGRES_HOST = process.env.POSTGRES_HOST || "localhost";
process.env.POSTGRES_PORT = process.env.POSTGRES_PORT || "5432";
process.env.POSTGRES_DATABASE = process.env.POSTGRES_DATABASE || "DataLake";
process.env.POSTGRES_USER = process.env.POSTGRES_USER || "postgres";
process.env.POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || "";

let app;

beforeAll(async () => {
  const serverModule = await import("../server.js");
  app = serverModule.default;
});

describe("Authentication Endpoints", () => {
  describe("POST /api/auth/login", () => {
    it("should return 400 if username is missing", async () => {
      const response = await request(app)
        .post("/api/auth/login")
        .send({ password: "password123" });

      expect(response.status).toBe(400);
      expect(response.body.error).toContain("required");
    });

    it("should return 400 if password is missing", async () => {
      const response = await request(app)
        .post("/api/auth/login")
        .send({ username: "admin" });

      expect(response.status).toBe(400);
      expect(response.body.error).toContain("required");
    });

    it("should return 401 with invalid credentials", async () => {
      const response = await request(app)
        .post("/api/auth/login")
        .send({ username: "invalid", password: "invalid" });

      expect(response.status).toBe(401);
      expect(response.body.error).toBeDefined();
    });

    it("should login successfully with valid credentials", async () => {
      const response = await request(app)
        .post("/api/auth/login")
        .send({ username: "admin", password: "admin123" });

      expect(response.status).toBe(200);
      expect(response.body.token).toBeDefined();
      expect(response.body.user).toBeDefined();
      expect(response.body.user.username).toBe("admin");
    });
  });

  describe("GET /api/auth/me", () => {
    let authToken;

    beforeAll(async () => {
      const loginResponse = await request(app)
        .post("/api/auth/login")
        .send({ username: "admin", password: "admin123" });
      authToken = loginResponse.body.token;
    });

    it("should return 401 without token", async () => {
      const response = await request(app).get("/api/auth/me");

      expect(response.status).toBe(401);
    });

    it("should return user info with valid token", async () => {
      const response = await request(app)
        .get("/api/auth/me")
        .set("Authorization", `Bearer ${authToken}`);

      expect(response.status).toBe(200);
      expect(response.body.user).toBeDefined();
      expect(response.body.user.username).toBe("admin");
    });
  });
});
