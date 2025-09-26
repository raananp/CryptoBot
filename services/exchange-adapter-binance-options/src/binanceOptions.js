// binanceOptions.js
// Node 20+: built-in fetch

/**
 * Returns native Binance Options symbols like:
 *   BTC-251226-70000-C
 *   ETH-250926-3200-P
 *
 * It tries eapi (Options) first, then vapi fallback.
 */
export async function fetchBinanceOptionSymbols() {
    const bases = (process.env.BINANCE_BASE_COINS || "BTC,ETH")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);
  
    const out = new Set();
  
    // eapi first
    for (const base of bases) {
      const u = new URL("https://eapi.binance.com/eapi/v1/optionInfo");
      u.searchParams.set("underlying", `${base}USDT`);
      const ok = await fetchAndCollect(u.toString(), out, "eapi");
      if (!ok) {
        // fallback to vapi
        const v = new URL("https://vapi.binance.com/vapi/v1/optionInfo");
        v.searchParams.set("underlying", base);
        await fetchAndCollect(v.toString(), out, "vapi");
      }
    }
  
    return Array.from(out);
  }
  
  async function fetchAndCollect(url, set, tag) {
    try {
      const res = await fetch(url);
      const text = await res.text();
  
      if (!res.ok) {
        console.error(`[binance-options] ${tag} fetch failed ${res.status} ${text.slice(0, 160)}`);
        return false;
      }
      let json;
      try {
        json = JSON.parse(text);
      } catch (e) {
        console.error(`[binance-options] ${tag} JSON parse error: ${e.message}. Raw: ${text.slice(0, 120)}`);
        return false;
      }
  
      // eapi shape: { data: { optionSymbols:[{symbol: "..."}] } }
      // vapi shape: { data: { options: [{symbol: "..."}] } }  (older)
      const list =
        json?.data?.optionSymbols ??
        json?.data?.options ??
        [];
  
      for (const it of list) {
        const sym = it?.symbol || it?.s;
        if (sym) set.add(sym);
      }
      return true;
    } catch (err) {
      console.error(`[binance-options] ${tag} fetch error`, err.message);
      return false;
    }
  }