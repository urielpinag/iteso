export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Solo se permite GET
    if (request.method !== "GET") {
      return Response.json(
        { ok: false, error: "Método no permitido. Usa GET." },
        { status: 405 }
      );
    }

    // Comprobación del binding D1
    let database = "sin binding";
    try {
      const row = await env.DB.prepare("SELECT 1 AS ok").first();
      database = row && row.ok === 1 ? "D1 conectada" : "D1 sin respuesta";
    } catch (error) {
      database = `D1 error: ${error.message}`;
    }

    return Response.json({
      ok: true,
      method: request.method,
      path: url.pathname,
      message: "Hola desde Cloudflare Workers 👋 (respuesta GET)",
      database,
      timestamp: new Date().toISOString(),
    });
  },
};
