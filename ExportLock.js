export class ExportLock {

  constructor(state) {
    this.state = state;
  }

  async fetch(request) {

    const url = new URL(request.url);

    if (url.pathname === "/lock") {

      const locked = await this.state.storage.get("locked");

      if (locked) {
        return new Response("LOCKED", { status: 423 });
      }

      await this.state.storage.put("locked", true, {
  expirationTtl: 60 * 5
});

      return new Response("OK");
    }

    if (url.pathname === "/unlock") {
      await this.state.storage.delete("locked");
      return new Response("OK");
    }
 
    if (url.pathname === "/status") {
  const locked = await this.state.storage.get("locked");
  return new Response(JSON.stringify({ locked: !!locked }), {
    headers: { "content-type": "application/json" }
  });
}
    return new Response("Unknown", { status: 404 });
  }
}