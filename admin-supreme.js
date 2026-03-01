import { createClient } from "@libsql/client";
export default {
  async fetch(request, env, ctx) {
    try { 
      if (!env.TURSO_DATABASE_URL || !env.TURSO_AUTH_TOKEN) {
        return json({ 
          error: "Internal Configuration Error", 
          details: "Missing Turso Environment Variables" 
        }, 500);
      }
      const db = createClient({
        url: env.TURSO_DATABASE_URL,
        authToken: env.TURSO_AUTH_TOKEN,
      });

      const url = new URL(request.url);
      const method = request.method;
const animeIdMatch = url.pathname.match(/^\/admin\/anime\/([^\/]+)$/);
// DELETE anime
if (method === "DELETE" && animeIdMatch) {
  const id = animeIdMatch[1];
  return await deleteAnime(db, id);
}
// CREATE anime
if (method === "POST" && url.pathname === "/admin/anime") {
  return await createAnime(request, db);
}
// GET all anime IDs
if (method === "GET" && url.pathname === "/admin/anime-ids") {
  return await getAllAnimeIds(db);
}
// GET single anime
if (method === "GET" && animeIdMatch) {
  const id = animeIdMatch[1];
  return await getAnime(db, id);
}
// UPDATE anime
if (method === "PUT" && animeIdMatch) {
  const id = animeIdMatch[1];
  return await updateAnime(request, db, id);
}
      return json({ error: "Not Found" }, 404);
    } catch (err) {
      console.error(err);
      return json({ error: "Internal Server Error" }, 500);
    }
  }
};
async function getAllAnimeIds(db) {
  try {
    const result = await db.execute({
      sql: `SELECT id FROM anime_info`
    });

    if (!result || !Array.isArray(result.rows)) {
      return json(
        { error: "Unexpected database response format" },
        500
      );
    }

    const ids = result.rows.map(row => row.id);

    return json({
      success: true,
      total: ids.length,
      data: ids
    });

  } catch (err) {
    console.error("Failed to fetch all anime IDs:", err);

    return json(
      {
        error: "Failed to fetch all anime IDs",
        details: err.message
      },
      500
    );
  }
}
async function getAnime(db, id) {
  const animeResult = await db.execute({
    sql: `SELECT * FROM anime_info WHERE id = ?`,
    args: [id]
  });

  const anime = animeResult.rows[0];

  if (!anime) {
    return json({ error: "Anime not found" }, 404);
  }

  // Get episodes
  const episodesResult = await db.execute({
    sql: `
      SELECT episode_number, episode_title, quality, language,
             server_name, stream_url, download_url
      FROM streaming_link
      WHERE anime_id = ?
      ORDER BY episode_number ASC
    `,
    args: [id]
  });

  const episodes = episodesResult.rows;

  return json({
    anime_info: anime,
    streaming_links: episodes
  });
}
async function deleteAnime(db, id) {
  const result = await db.execute({
    sql: `DELETE FROM anime_info WHERE id = ?`,
    args: [id]
  });

  if (result.rowsAffected === 0) {
    return json({ error: "Anime not found" }, 404);
  }

  return json({ success: true });
}

async function createAnime(request, db) {
  const body = await request.json();
  const { anime_info, streaming_links } = body;

  if (!anime_info?.id) {
    return json({ error: "Anime ID required" }, 400);
  }

  try {
    await db.execute({sql:"BEGIN"});

    const fields = Object.keys(anime_info);
    const placeholders = fields.map(() => "?").join(", ");

        await db.execute({
      sql: `
        INSERT INTO anime_info (${fields.join(", ")})
        VALUES (${placeholders})
      `,
      args: fields.map(f => anime_info[f])
    });


    for (const ep of streaming_links || []) {
      await db.execute({
        sql: `
          INSERT INTO streaming_link
          (anime_id, episode_number, episode_title,
           quality, language, server_name,
           stream_url, download_url)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        `,
        args: [
          anime_info.id,
          ep.episode_number,
          ep.episode_title || "",
          ep.quality || "",
          ep.language || "",
          ep.server_name || "",
          ep.stream_url,
          ep.download_url || ""
        ]
      });
    }

    await db.execute({sql:"COMMIT"});
    return json({ success: true });

  } catch (err) {
    await db.execute({sql:"ROLLBACK"});
    return json({ error: "Creation failed" }, 500);
  }
}
async function updateAnime(request, db, id) {
  const body = await request.json();
  const { anime_info, streaming_links } = body;

  if (!anime_info || !streaming_links) {
    return json({ error: "Invalid payload" }, 400);
  }
anime_info.id = id;
const integerFields = [
  "mal_id", "year", "episodes", "total_seasons",
  "popularity", "rank", "scored_by",
  "members", "favorites"
];

const realFields = ["rating"];

for (const field of integerFields) {
  if (anime_info[field] !== undefined) {
    anime_info[field] = Number(anime_info[field]) || null;
  }
}

for (const field of realFields) {
  if (anime_info[field] !== undefined) {
    anime_info[field] = parseFloat(anime_info[field]) || null;
  }
}
  // Basic Validation
  for (const ep of streaming_links) {
    if (!Number.isInteger(ep.episode_number) || ep.episode_number < 1) {
      return json({ error: "Invalid episode number" }, 400);
    }
    if (!ep.stream_url) {
      return json({ error: "Stream URL required" }, 400);
    }
  }

  try {
    await db.execute({sql:"BEGIN"});
    
    /* ================= UPDATE ANIME INFO ================= */

    const fields = Object.keys(anime_info)
      .filter(f => f !== "created_at");

    const setClause = fields.map(f => `${f} = ?`).join(", ");

    const values = fields.map(f => anime_info[f]);

    await db.execute({
  sql: `
      UPDATE anime_info
      SET ${setClause},
          updated_at = CURRENT_TIMESTAMP
      WHERE id = ?
    `,   args: [...values, id]
});

    /* ================= SMART DIFF EPISODES ================= */

    const existingResult = await db.execute({
  sql: `SELECT * FROM streaming_link WHERE anime_id = ?`,
  args: [id]
    });

const existing = existingResult.rows;

    const existingMap = new Map();
    existing.forEach(e => {
      existingMap.set(e.episode_number, e);
    });

    const incomingMap = new Map();
    streaming_links.forEach(e => {
      incomingMap.set(e.episode_number, e);
    });

    // 1️⃣ DELETE removed episodes
    for (const [epNum, row] of existingMap.entries()) {
  if (!incomingMap.has(epNum)) {
    await db.execute({
      sql: `
        DELETE FROM streaming_link
        WHERE anime_id = ? AND episode_number = ?
      `,
      args: [id, epNum]
    });
  }
}
    // 2️⃣ INSERT or UPDATE
    for (const [epNum, ep] of incomingMap.entries()) {
      const existingRow = existingMap.get(epNum);

      if (!existingRow) {
        await db.execute({
          sql: `
            INSERT INTO streaming_link
            (anime_id, episode_number, episode_title,
             quality, language, server_name,
             stream_url, download_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          `,
          args: [
            id,
            ep.episode_number,
            ep.episode_title || "",
            ep.quality || "",
            ep.language || "",
            ep.server_name || "",
            ep.stream_url,
            ep.download_url || ""
          ]
        });
      } else {
        if (isEpisodeChanged(existingRow, ep)) {
          await db.execute({
            sql: `
              UPDATE streaming_link
              SET episode_title = ?,
                  quality = ?,
                  language = ?,
                  server_name = ?,
                  stream_url = ?,
                  download_url = ?
              WHERE anime_id = ?
              AND episode_number = ?
            `,
            args: [
              ep.episode_title || "",
              ep.quality || "",
              ep.language || "",
              ep.server_name || "",
              ep.stream_url,
              ep.download_url || "",
              id,
              epNum
            ]
          });
        }
      }
    }

    // ✅ CLOSE TRY BLOCK PROPERLY
    await db.execute({sql:"COMMIT"});
    return json({ success: true });

  } catch (err) {
    await db.execute({sql:"ROLLBACK"});
    console.error(err);
    return json({ error: "Update failed" }, 500);
  }
}

function isEpisodeChanged(oldRow, newRow) {
  return (
    oldRow.episode_title !== (newRow.episode_title || "") ||
    oldRow.quality !== (newRow.quality || "") ||
    oldRow.language !== (newRow.language || "") ||
    oldRow.server_name !== (newRow.server_name || "") ||
    oldRow.stream_url !== newRow.stream_url ||
    oldRow.download_url !== (newRow.download_url || "")
  );
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type"
    }
  });
}
