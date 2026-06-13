import express from "express";
import pg from "pg";

const { Pool } = pg;

const app = express();

const PORT = Number(process.env.PORT ?? 8080);
const DATABASE_URL =
  process.env.DATABASE_URL ??
  "postgres://db_user:test1234@postgres-db:5432/ids_testdb";

const pool = new Pool({
  connectionString: DATABASE_URL,
  max: Number(process.env.DB_POOL_SIZE ?? 10),
});

app.use(express.json());

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_events (
      id BIGSERIAL PRIMARY KEY,
      event_type TEXT NOT NULL,
      source TEXT,
      message TEXT,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS api_counters (
      name TEXT PRIMARY KEY,
      value BIGINT NOT NULL DEFAULT 0,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
  `);

  await pool.query(`
    INSERT INTO api_counters (name, value)
    VALUES ('requests', 0)
    ON CONFLICT (name) DO NOTHING;
  `);
}

async function logEvent(eventType, source, message) {
  await pool.query(
    `
    INSERT INTO api_events (event_type, source, message)
    VALUES ($1, $2, $3)
    `,
    [eventType, source, message]
  );
}

async function incrementCounter(name) {
  await pool.query(
    `
    INSERT INTO api_counters (name, value, updated_at)
    VALUES ($1, 1, CURRENT_TIMESTAMP)
    ON CONFLICT (name)
    DO UPDATE SET
      value = api_counters.value + 1,
      updated_at = CURRENT_TIMESTAMP
    `,
    [name]
  );
}

app.get("/", async (_req, res) => {
  try {
    await incrementCounter("root_requests");

    res.json({
      service: "api-service",
      message: "API service is up",
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: "Database error",
      message: error.message,
    });
  }
});

app.get("/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");

    res.json({
      status: "ok",
      database: "connected",
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      status: "error",
      database: "unavailable",
      message: error.message,
    });
  }
});

app.get("/api/data", async (req, res) => {
  try {
    await incrementCounter("data_requests");

    const id = req.query.id ?? randomInt(1, 9999);

    const result = await pool.query(
      `
      SELECT id, event_type, source, message, created_at
      FROM api_events
      ORDER BY created_at DESC
      LIMIT 5
      `
    );

    res.json({
      id,
      data: [1, 2, 3, 4, 5],
      recentEvents: result.rows,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to get data",
      message: error.message,
    });
  }
});

app.post("/api/events", async (req, res) => {
  const eventType = req.body?.eventType ?? "generic_event";
  const source = req.body?.source ?? "unknown";
  const message = req.body?.message ?? "No message";

  try {
    await logEvent(eventType, source, message);
    await incrementCounter("event_posts");

    res.status(201).json({
      ok: true,
      eventType,
      source,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: "Failed to add event",
      message: error.message,
    });
  }
});

app.get("/api/random-db", async (_req, res) => {
  try {
    const action = Math.random();

    if (action < 0.4) {
      await logEvent(
        "random_read_write",
        "api-service",
        `Generated random event ${Date.now()}`
      );
    } else {
      await incrementCounter("random_db_requests");
    }

    const counters = await pool.query(`
      SELECT name, value, updated_at
      FROM api_counters
      ORDER BY name
    `);

    res.json({
      ok: true,
      counters: counters.rows,
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    res.status(500).json({
      error: "Random DB operation failed",
      message: error.message,
    });
  }
});

app.use((req, res) => {
  res.status(404).json({
    error: "not found",
    path: req.path,
    timestamp: new Date().toISOString(),
  });
});

initDb()
  .then(() => {
    app.listen(PORT, "0.0.0.0", () => {
      console.log(`[API-service] [INFO] listening on port ${PORT}`);
      console.log(`[API-service] [INFO] connected to database`);
    });
  })
  .catch((error) => {
    console.error("[API-service] [ERROR] failed to initialize database:", error);
    process.exit(1);
  });