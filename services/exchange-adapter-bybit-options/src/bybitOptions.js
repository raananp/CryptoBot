// bybitOptions.js
// Uses Node 20+ built-in fetch (no node-fetch import)

const MONTH_MAP = {
    JAN: "01", FEB: "02", MAR: "03", APR: "04",
    MAY: "05", JUN: "06", JUL: "07", AUG: "08",
    SEP: "09", OCT: "10", NOV: "11", DEC: "12",
  };
  
  /**
   * Convert Bybit native option symbol (e.g. BTC-30JUN24-70000-C)
   * to BASE-YYYY-MM-DD-STRIKE-C|P
   */
  export function canonicalizeBybit(symbol = "") {
    try {
      const [base, dMonYY, strike, cp] = symbol.split("-");
      const day = dMonYY.slice(0, 2);
      const mon = dMonYY.slice(2, 5).toUpperCase();
      const yy = dMonYY.slice(5, 7);
      const month = MONTH_MAP[mon] || "01";
      const year = "20" + yy;
      return `${base}-${year}-${month}-${day}-${strike}-${cp}`;
    } catch {
      return symbol;
    }
  }
  
  /**
   * Discover Bybit option symbols via REST v5.
   * ENV:
   *   BYBIT_REST_URL=https://api.bybit.com/v5/market/instruments-info
   *   BYBIT_BASE_COINS=BTC,ETH
   */
  export async function fetchBybitOptionSymbols() {
    const REST = process.env.BYBIT_REST_URL || "https://api.bybit.com/v5/market/instruments-info";
    const bases = (process.env.BYBIT_BASE_COINS || "BTC,ETH")
      .split(",")
      .map(s => s.trim())
      .filter(Boolean);
  
    const out = new Set();
  
    for (const base of bases) {
      let cursor = "";
      // paginate just in case
      for (let page = 0; page < 50; page++) {
        const u = new URL(REST);
        u.searchParams.set("category", "option");
        u.searchParams.set("baseCoin", base);
        if (cursor) u.searchParams.set("cursor", cursor);
  
        const res = await fetch(u.toString(), { method: "GET" });
        const text = await res.text();
        if (!res.ok) {
          console.error(`[bybit-options] REST ${res.status} ${text.slice(0, 180)}`);
          break;
        }
  
        let json;
        try {
          json = JSON.parse(text);
        } catch (e) {
          console.error(`[bybit-options] JSON parse error for base=${base}: ${e.message}`);
          break;
        }
  
        const list = json?.result?.list || [];
        for (const inst of list) {
          const sym = inst?.symbol || inst?.s;
          if (sym) out.add(sym);
        }
  
        cursor = json?.result?.nextPageCursor || "";
        if (!cursor) break;
      }
    }
  
    return Array.from(out);
  }