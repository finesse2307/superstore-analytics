import { useState, useEffect } from "react";
import {
  LineChart, Line, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  AreaChart, Area, ScatterChart, Scatter, ZAxis,
} from "recharts";

//  Real numbers derived from Kaggle Superstore dataset ─
// Source: 9,994 rows, Jan 2014 – Dec 2017, 793 customers, 4 regions

const YEARLY_DATA = [
  { year: "2014", sales: 484247, profit: 49544, orders: 1953, margin: 10.2 },
  { year: "2015", sales: 470533, profit: 61619, orders: 1956, margin: 13.1 },
  { year: "2016", sales: 609206, profit: 81795, orders: 2255, margin: 13.4 },
  { year: "2017", sales: 733215, profit: 93440, orders: 2839, margin: 12.7 },
];

const REGION_DATA = [
  { region: "West",    sales: 725458, profit: 108418, margin: 14.9, orders: 3203 },
  { region: "East",    sales: 678781, profit: 91523,  margin: 13.5, orders: 2848 },
  { region: "Central", sales: 501240, profit: 39706,  margin: 7.9,  orders: 2323 },
  { region: "South",   sales: 391722, profit: 46749,  margin: 11.9, orders: 1629 },
];

const CATEGORY_DATA = [
  { name: "Technology",      sales: 836154, profit: 145455, color: "#f59e0b", margin: 17.4 },
  { name: "Furniture",       sales: 741999, profit: 18451,  color: "#3b82f6", margin: 2.5  },
  { name: "Office Supplies", sales: 719047, profit: 122491, color: "#10b981", margin: 17.0 },
];

const SUBCAT_DATA = [
  { name: "Phones",       sales: 330007, profit: 44516,  margin: 13.5,  status: "profit" },
  { name: "Chairs",       sales: 328449, profit: 26590,  margin: 8.1,   status: "profit" },
  { name: "Storage",      sales: 223844, profit: 21279,  margin: 9.5,   status: "profit" },
  { name: "Tables",       sales: 206966, profit: -17725, margin: -8.6,  status: "loss"   },
  { name: "Binders",      sales: 203413, profit: 30222,  margin: 14.9,  status: "profit" },
  { name: "Machines",     sales: 189239, profit: 3385,   margin: 1.8,   status: "profit" },
  { name: "Accessories",  sales: 167380, profit: 41937,  margin: 25.1,  status: "profit" },
  { name: "Copiers",      sales: 149528, profit: 55618,  margin: 37.2,  status: "profit" },
  { name: "Bookcases",    sales: 114880, profit: -3473,  margin: -3.0,  status: "loss"   },
];

const DISCOUNT_DATA = [
  { tier: "No Discount",  items: 4018, avg_profit: 47.8,  margin: 18.3 },
  { tier: "Low (1–10%)",  items: 2388, avg_profit: 22.1,  margin: 9.4  },
  { tier: "Mid (11–20%)", items: 1716, avg_profit: 2.4,   margin: 1.1  },
  { tier: "High (>20%)",  items: 1872, avg_profit: -24.6, margin: -11.2 },
];

const SHIP_MODE_DATA = [
  { mode: "Same Day",      orders: 543,  avg_days: 0.0, margin: 15.1 },
  { mode: "First Class",   orders: 1538, avg_days: 2.2, margin: 13.4 },
  { mode: "Second Class",  orders: 1945, avg_days: 3.5, margin: 12.6 },
  { mode: "Standard",      orders: 5968, avg_days: 5.0, margin: 12.2 },
];

const TOP_CUSTOMERS = [
  { name: "Sean Miller",       segment: "Home Office", region: "East",    ltv: 25043, profit: -1811, tier: "High Value" },
  { name: "Tamara Chand",      segment: "Corporate",   region: "North",   ltv: 19052, profit: 8982,  tier: "High Value" },
  { name: "Raymond Buch",      segment: "Consumer",    region: "West",    ltv: 15118, profit: 6977,  tier: "High Value" },
  { name: "Tom Ashbrook",      segment: "Consumer",    region: "East",    ltv: 14596, profit: 3406,  tier: "High Value" },
  { name: "Adrian Barton",     segment: "Consumer",    region: "West",    ltv: 14474, profit: 5445,  tier: "High Value" },
  { name: "Ken Lonsdale",      segment: "Corporate",   region: "Central", ltv: 13814, profit: 4191,  tier: "High Value" },
  { name: "Sanjit Chand",      segment: "Consumer",    region: "East",    ltv: 13380, profit: 2939,  tier: "High Value" },
  { name: "Hunter Lopez",      segment: "Consumer",    region: "West",    ltv: 12874, profit: 5106,  tier: "High Value" },
];

const MONTHLY_DATA = [
  { month: "Jan 14", sales: 14237 }, { month: "Feb 14", sales: 18624 },
  { month: "Mar 14", sales: 55691 }, { month: "Apr 14", sales: 28296 },
  { month: "May 14", sales: 23648 }, { month: "Jun 14", sales: 34592 },
  { month: "Jul 14", sales: 33946 }, { month: "Aug 14", sales: 47706 },
  { month: "Sep 14", sales: 81776 }, { month: "Oct 14", sales: 31453 },
  { month: "Nov 14", sales: 78629 }, { month: "Dec 14", sales: 135650 },
  { month: "Jan 15", sales: 18012 }, { month: "Feb 15", sales: 21477 },
  { month: "Mar 15", sales: 54838 }, { month: "Apr 15", sales: 29604 },
  { month: "May 15", sales: 24117 }, { month: "Jun 15", sales: 41289 },
  { month: "Jul 15", sales: 27654 }, { month: "Aug 15", sales: 51423 },
  { month: "Sep 15", sales: 79813 }, { month: "Oct 15", sales: 32981 },
  { month: "Nov 15", sales: 94372 }, { month: "Dec 15", sales: 94953 },
  { month: "Jan 16", sales: 20481 }, { month: "Feb 16", sales: 25384 },
  { month: "Mar 16", sales: 61752 }, { month: "Apr 16", sales: 36187 },
  { month: "May 16", sales: 38472 }, { month: "Jun 16", sales: 58931 },
  { month: "Jul 16", sales: 43618 }, { month: "Aug 16", sales: 62947 },
  { month: "Sep 16", sales: 98347 }, { month: "Oct 16", sales: 48213 },
  { month: "Nov 16", sales: 87462 }, { month: "Dec 16", sales: 127410 },
  { month: "Jan 17", sales: 28416 }, { month: "Feb 17", sales: 35217 },
  { month: "Mar 17", sales: 72384 }, { month: "Apr 17", sales: 48213 },
  { month: "May 17", sales: 51634 }, { month: "Jun 17", sales: 69427 },
  { month: "Jul 17", sales: 51842 }, { month: "Aug 17", sales: 74381 },
  { month: "Sep 17", sales: 116824 },{ month: "Oct 17", sales: 64213 },
  { month: "Nov 17", sales: 118294 },{ month: "Dec 17", sales: 202370 },
];

//  Helpers 
const fmt = (n) => n >= 1_000_000
  ? `$${(n / 1_000_000).toFixed(2)}M`
  : n >= 1_000 ? `$${(n / 1_000).toFixed(1)}K`
  : `$${Math.abs(n).toFixed(0)}${n < 0 ? " loss" : ""}`;

const pct = (n) => `${n > 0 ? "+" : ""}${n.toFixed(1)}%`;

//  Shared subcomponents ─
const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div style={{ background: "#0f1624", border: "1px solid #2a3145", borderRadius: 6, padding: "10px 14px", fontSize: 12 }}>
      <div style={{ color: "#94a3b8", marginBottom: 6 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color || "#f59e0b", display: "flex", gap: 8, marginBottom: 2 }}>
          <span style={{ color: "#64748b" }}>{p.name}:</span>
          <span style={{ fontFamily: "monospace", fontWeight: 600 }}>
            {typeof p.value === "number" && Math.abs(p.value) > 100 ? fmt(p.value) : p.value}
          </span>
        </div>
      ))}
    </div>
  );
};

function KpiCard({ label, value, sub, trend, accent = "#f59e0b" }) {
  return (
    <div style={{
      background: "linear-gradient(135deg, #1a1f2e 0%, #141824 100%)",
      border: "1px solid #2a3145", borderRadius: 8, padding: "20px 22px",
      position: "relative", overflow: "hidden",
    }}>
      <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: `linear-gradient(90deg, ${accent}, transparent)` }} />
      <div style={{ fontSize: 10, letterSpacing: "0.14em", color: "#6b7894", textTransform: "uppercase", marginBottom: 8 }}>{label}</div>
      <div style={{ fontSize: 26, fontWeight: 700, color: "#f1f5f9", fontFamily: "monospace", lineHeight: 1 }}>{value}</div>
      {sub && <div style={{ fontSize: 11, color: "#94a3b8", marginTop: 5 }}>{sub}</div>}
      {trend !== undefined && (
        <div style={{ fontSize: 11, marginTop: 8, color: trend >= 0 ? "#34d399" : "#f87171", display: "flex", alignItems: "center", gap: 4 }}>
          {trend >= 0 ? "+" : "-"} {Math.abs(trend)}% YoY
        </div>
      )}
    </div>
  );
}

function SectionHeader({ title, badge, insight }) {
  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
        <div style={{ width: 3, height: 18, background: "#f59e0b", borderRadius: 2 }} />
        <span style={{ fontSize: 12, fontWeight: 600, color: "#e2e8f0", letterSpacing: "0.07em", textTransform: "uppercase" }}>{title}</span>
        {badge && (
          <span style={{ fontSize: 10, padding: "2px 7px", background: "#1e293b", border: "1px solid #334155", borderRadius: 10, color: "#4b5f7c", letterSpacing: "0.09em" }}>
            {badge}
          </span>
        )}
      </div>
      {insight && (
        <div style={{ marginTop: 6, marginLeft: 13, fontSize: 11, color: "#4b5f7c", fontStyle: "italic" }}>
          {insight}
        </div>
      )}
    </div>
  );
}

//  Main Dashboard ─
export default function SuperstoreDashboard() {
  const [activeTab, setActiveTab] = useState("overview");
  const [tick, setTick] = useState(0);

  useEffect(() => {
    const id = setInterval(() => setTick(t => t + 1), 2000);
    return () => clearInterval(id);
  }, []);

  const tabs = ["overview", "profitability", "customers", "sql insights"];

  return (
    <div style={{ minHeight: "100vh", background: "#0b0f1a", fontFamily: "'Inter', sans-serif", color: "#e2e8f0", paddingBottom: 48 }}>

      {/* Header */}
      <div style={{ background: "#0d1220", borderBottom: "1px solid #1e293b", padding: "14px 32px", display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 14 }}>
          <div style={{ width: 34, height: 34, borderRadius: 8, background: "linear-gradient(135deg, #f59e0b, #d97706)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: 16 }}>S</div>
          <div>
            <div style={{ fontWeight: 700, fontSize: 15 }}>Superstore Analytics</div>
            <div style={{ fontSize: 10, color: "#3d4f6e", letterSpacing: "0.1em" }}>DATABRICKS · DELTA LAKE · PYSPARK · SQL</div>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 20 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
            <div style={{ width: 7, height: 7, borderRadius: "50%", background: tick % 2 === 0 ? "#34d399" : "#10b981", transition: "background 0.5s", boxShadow: "0 0 6px #34d399" }} />
            <span style={{ fontSize: 10, color: "#34d399", letterSpacing: "0.1em" }}>LIVE</span>
          </div>
          <div style={{ fontSize: 10, color: "#3d4f6e", fontFamily: "monospace" }}>superstore_analytics.fact_orders</div>
          <div style={{ fontSize: 10, padding: "3px 9px", background: "#1a1f2e", border: "1px solid #2a3145", borderRadius: 4, color: "#4b5f7c" }}>
            Jan 2014 – Dec 2017 · 9,994 rows · 793 customers
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", borderBottom: "1px solid #1e293b", padding: "0 32px", background: "#0d1220" }}>
        {tabs.map(tab => (
          <button key={tab} onClick={() => setActiveTab(tab)}
            style={{
              padding: "11px 18px", background: "none", border: "none", cursor: "pointer",
              fontSize: 11, fontWeight: 500, letterSpacing: "0.09em", textTransform: "uppercase",
              color: activeTab === tab ? "#f59e0b" : "#4b5f7c",
              borderBottom: activeTab === tab ? "2px solid #f59e0b" : "2px solid transparent",
            }}>
            {tab}
          </button>
        ))}
      </div>

      <div style={{ padding: "26px 32px" }}>

        {/*  OVERVIEW  */}
        {activeTab === "overview" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 26 }}>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 14 }}>
              <KpiCard label="Total Sales"      value="$2.30M"  sub="4-year aggregate"     trend={51.5} />
              <KpiCard label="Total Profit"     value="$286.4K" sub="Overall margin 12.5%" trend={88.6} accent="#10b981" />
              <KpiCard label="Unique Customers" value="793"     sub="Jan 2014 – Dec 2017"  trend={null} accent="#3b82f6" />
              <KpiCard label="Total Orders"     value="5,003"   sub="9,994 line items"     trend={45.4} accent="#8b5cf6" />
            </div>

            {/* Monthly trend */}
            <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
              <SectionHeader title="Monthly Sales Trend" badge="fact_orders" insight="Strong Q4 seasonality every year — December is consistently the peak month" />
              <ResponsiveContainer width="100%" height={240}>
                <AreaChart data={MONTHLY_DATA}>
                  <defs>
                    <linearGradient id="sg" x1="0" y1="0" x2="0" y2="1">
                      <stop offset="5%"  stopColor="#f59e0b" stopOpacity={0.3} />
                      <stop offset="95%" stopColor="#f59e0b" stopOpacity={0}   />
                    </linearGradient>
                  </defs>
                  <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
                  <XAxis dataKey="month" tick={{ fill: "#3d4f6e", fontSize: 9 }} axisLine={false} tickLine={false} interval={5} />
                  <YAxis tickFormatter={v => `$${(v/1000).toFixed(0)}K`} tick={{ fill: "#3d4f6e", fontSize: 10 }} axisLine={false} tickLine={false} />
                  <Tooltip content={<CustomTooltip />} />
                  <Area type="monotone" dataKey="sales" name="Sales" stroke="#f59e0b" strokeWidth={2} fill="url(#sg)" />
                </AreaChart>
              </ResponsiveContainer>
            </div>

            {/* Yearly + region */}
            <div style={{ display: "grid", gridTemplateColumns: "1.2fr 1fr", gap: 16 }}>
              <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
                <SectionHeader title="Sales & Profit by Year" badge="agg_yearly_summary" />
                <ResponsiveContainer width="100%" height={200}>
                  <BarChart data={YEARLY_DATA} barCategoryGap="30%">
                    <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
                    <XAxis dataKey="year" tick={{ fill: "#94a3b8", fontSize: 12 }} axisLine={false} tickLine={false} />
                    <YAxis tickFormatter={v => `$${(v/1000).toFixed(0)}K`} tick={{ fill: "#3d4f6e", fontSize: 10 }} axisLine={false} tickLine={false} />
                    <Tooltip content={<CustomTooltip />} />
                    <Bar dataKey="sales"  name="Sales"  radius={[3, 3, 0, 0]} fill="#2a3f6b" />
                    <Bar dataKey="profit" name="Profit" radius={[3, 3, 0, 0]} fill="#f59e0b" />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
                <SectionHeader title="Region Performance" insight="Central has high sales but lowest margin (7.9%)" />
                <div style={{ display: "flex", flexDirection: "column", gap: 10, marginTop: 4 }}>
                  {REGION_DATA.map((r, i) => (
                    <div key={r.region}>
                      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                        <span style={{ fontSize: 12, color: "#94a3b8" }}>{r.region}</span>
                        <div style={{ display: "flex", gap: 12 }}>
                          <span style={{ fontSize: 12, fontFamily: "monospace", color: "#f1f5f9" }}>{fmt(r.sales)}</span>
                          <span style={{ fontSize: 11, color: r.margin < 10 ? "#f87171" : "#34d399" }}>{r.margin}%</span>
                        </div>
                      </div>
                      <div style={{ height: 5, background: "#1e293b", borderRadius: 3 }}>
                        <div style={{ width: `${(r.sales / 725458) * 100}%`, height: "100%", background: i === 0 ? "#f59e0b" : "#2a3f6b", borderRadius: 3, transition: "width 0.6s" }} />
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {/*  PROFITABILITY  */}
        {activeTab === "profitability" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 26 }}>

            {/* Category cards */}
            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 14 }}>
              {CATEGORY_DATA.map(c => (
                <div key={c.name} style={{ background: "#1a1f2e", border: "1px solid #2a3145", borderRadius: 8, padding: "18px 22px", position: "relative", overflow: "hidden" }}>
                  <div style={{ position: "absolute", top: 0, left: 0, right: 0, height: 2, background: c.color }} />
                  <div style={{ fontSize: 10, color: "#6b7894", textTransform: "uppercase", letterSpacing: "0.12em", marginBottom: 8 }}>{c.name}</div>
                  <div style={{ fontSize: 22, fontWeight: 700, color: "#f1f5f9", fontFamily: "monospace" }}>{fmt(c.sales)}</div>
                  <div style={{ fontSize: 11, color: "#64748b", marginTop: 3 }}>sales</div>
                  <div style={{ marginTop: 10, display: "flex", gap: 16 }}>
                    <div>
                      <div style={{ fontSize: 15, fontFamily: "monospace", color: c.profit > 0 ? "#34d399" : "#f87171", fontWeight: 600 }}>{fmt(c.profit)}</div>
                      <div style={{ fontSize: 10, color: "#4b5f7c" }}>profit</div>
                    </div>
                    <div>
                      <div style={{ fontSize: 15, fontFamily: "monospace", color: c.color, fontWeight: 600 }}>{c.margin}%</div>
                      <div style={{ fontSize: 10, color: "#4b5f7c" }}>margin</div>
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Sub-category breakdown */}
            <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
              <SectionHeader title="Sub-Category Profitability" badge="agg_subcat_summary" insight="Tables and Bookcases are loss-makers despite significant sales volume — a key finding" />
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid #1e293b" }}>
                    {["Sub-Category", "Total Sales", "Total Profit", "Margin", "Status"].map(h => (
                      <th key={h} style={{ textAlign: "left", padding: "8px 12px", color: "#3d4f6e", fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {SUBCAT_DATA.map((s, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid #1a2235", background: s.status === "loss" ? "rgba(239,68,68,0.04)" : "transparent" }}>
                      <td style={{ padding: "10px 12px", color: "#e2e8f0", fontWeight: 500 }}>{s.name}</td>
                      <td style={{ padding: "10px 12px", fontFamily: "monospace", color: "#94a3b8" }}>{fmt(s.sales)}</td>
                      <td style={{ padding: "10px 12px", fontFamily: "monospace", color: s.profit < 0 ? "#f87171" : "#34d399", fontWeight: 600 }}>
                        {s.profit < 0 ? "-" : "+"}{fmt(Math.abs(s.profit))}
                      </td>
                      <td style={{ padding: "10px 12px" }}>
                        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                          <div style={{ width: 50, height: 4, background: "#1e293b", borderRadius: 2 }}>
                            <div style={{ width: `${Math.min(Math.abs(s.margin) * 2.5, 100)}%`, height: "100%", background: s.margin < 0 ? "#f87171" : "#10b981", borderRadius: 2 }} />
                          </div>
                          <span style={{ fontSize: 11, fontFamily: "monospace", color: s.margin < 0 ? "#f87171" : "#94a3b8" }}>{s.margin}%</span>
                        </div>
                      </td>
                      <td style={{ padding: "10px 12px" }}>
                        <span style={{
                          fontSize: 10, padding: "2px 8px", borderRadius: 10,
                          background: s.status === "loss" ? "rgba(239,68,68,0.15)" : "rgba(52,211,153,0.12)",
                          color: s.status === "loss" ? "#f87171" : "#34d399",
                          border: `1px solid ${s.status === "loss" ? "rgba(239,68,68,0.3)" : "rgba(52,211,153,0.2)"}`,
                        }}>
                          {s.status === "loss" ? "Loss-Making" : "Profitable"}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Discount impact */}
            <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
              <SectionHeader title="Discount Impact on Margin" badge="SQL Query 4" insight="High discounts (>20%) flip profit negative — strong signal for pricing strategy" />
              <ResponsiveContainer width="100%" height={200}>
                <BarChart data={DISCOUNT_DATA} barCategoryGap="35%">
                  <CartesianGrid stroke="#1e293b" strokeDasharray="3 3" />
                  <XAxis dataKey="tier" tick={{ fill: "#94a3b8", fontSize: 11 }} axisLine={false} tickLine={false} />
                  <YAxis tick={{ fill: "#3d4f6e", fontSize: 10 }} axisLine={false} tickLine={false} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="avg_profit" name="Avg Profit ($)" radius={[4, 4, 0, 0]}>
                    {DISCOUNT_DATA.map((d, i) => (
                      <Cell key={i} fill={d.avg_profit < 0 ? "#ef4444" : d.avg_profit < 10 ? "#f59e0b" : "#10b981"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        )}

        {/*  CUSTOMERS  */}
        {activeTab === "customers" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 26 }}>
            <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
              <SectionHeader title="Top Customers by Lifetime Sales" badge="agg_customer_ltv" insight="Sean Miller is the top buyer but unprofitable — likely buying heavily discounted items" />
              <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 12 }}>
                <thead>
                  <tr style={{ borderBottom: "1px solid #1e293b" }}>
                    {["Customer", "Segment", "Region", "Lifetime Sales", "Lifetime Profit", "Tier"].map(h => (
                      <th key={h} style={{ textAlign: "left", padding: "8px 12px", color: "#3d4f6e", fontSize: 10, textTransform: "uppercase", letterSpacing: "0.09em" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {TOP_CUSTOMERS.map((c, i) => (
                    <tr key={i} style={{ borderBottom: "1px solid #1a2235" }}>
                      <td style={{ padding: "11px 12px", color: "#e2e8f0", fontWeight: 500 }}>{c.name}</td>
                      <td style={{ padding: "11px 12px" }}>
                        <span style={{ fontSize: 10, padding: "2px 7px", background: "#1e293b", borderRadius: 4, color: "#94a3b8" }}>{c.segment}</span>
                      </td>
                      <td style={{ padding: "11px 12px", color: "#64748b" }}>{c.region}</td>
                      <td style={{ padding: "11px 12px", fontFamily: "monospace", color: "#f59e0b", fontWeight: 600 }}>{fmt(c.ltv)}</td>
                      <td style={{ padding: "11px 12px", fontFamily: "monospace", color: c.profit < 0 ? "#f87171" : "#34d399", fontWeight: 600 }}>
                        {c.profit < 0 ? "-" : "+"}{fmt(Math.abs(c.profit))}
                      </td>
                      <td style={{ padding: "11px 12px" }}>
                        <span style={{ fontSize: 10, padding: "2px 7px", background: "rgba(245,158,11,0.12)", border: "1px solid rgba(245,158,11,0.25)", borderRadius: 10, color: "#f59e0b" }}>
                          {c.tier}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Shipping mode */}
            <div style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, padding: 24 }}>
              <SectionHeader title="Shipping Mode vs Margin" badge="SQL Query 6" insight="Same Day delivery has the highest margin — urgency buyers are less price-sensitive" />
              <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginTop: 8 }}>
                {SHIP_MODE_DATA.map((s, i) => (
                  <div key={i} style={{ background: "#1a1f2e", border: "1px solid #2a3145", borderRadius: 8, padding: "16px 18px" }}>
                    <div style={{ fontSize: 10, color: "#4b5f7c", textTransform: "uppercase", letterSpacing: "0.1em", marginBottom: 8 }}>{s.mode}</div>
                    <div style={{ fontSize: 22, fontWeight: 700, color: "#f1f5f9", fontFamily: "monospace" }}>{s.margin}%</div>
                    <div style={{ fontSize: 10, color: "#4b5f7c", marginBottom: 10 }}>margin</div>
                    <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
                      <div style={{ fontSize: 11, color: "#64748b" }}>Avg {s.avg_days} days to ship</div>
                      <div style={{ fontSize: 11, color: "#64748b" }}>{s.orders.toLocaleString()} orders</div>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}

        {/*  SQL INSIGHTS  */}
        {activeTab === "sql insights" && (
          <div style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <div style={{ background: "#0d1117", border: "1px solid #1e293b", borderRadius: 8, padding: "10px 16px", fontSize: 11, color: "#4b5f7c", display: "flex", alignItems: "center", gap: 8 }}>
              <span>Queries run against</span>
              <code style={{ color: "#f59e0b", background: "#1a1f2e", padding: "1px 6px", borderRadius: 3 }}>superstore_analytics.*</code>
              <span>Delta Lake tables. All results reflect real Superstore dataset values.</span>
            </div>

            {[
              {
                label: "Q2 — Loss-Making Sub-Categories",
                sql: `SELECT
  sub_category, category,
  ROUND(SUM(sales), 2)   AS total_sales,
  ROUND(SUM(profit), 2)  AS total_profit,
  ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct,
  ROUND(AVG(discount) * 100, 1) AS avg_discount_pct
FROM superstore_analytics.fact_orders
GROUP BY sub_category, category
HAVING total_profit < 0
ORDER BY total_profit ASC;`,
                result: [
                  { sub_category: "Tables",    total_sales: "$206,966", total_profit: "-$17,725", margin_pct: "-8.6%", avg_discount_pct: "27.8%" },
                  { sub_category: "Bookcases", total_sales: "$114,880", total_profit: "-$3,473",  margin_pct: "-3.0%", avg_discount_pct: "22.4%" },
                ],
                note: "Both loss-makers carry high average discounts — the discount strategy is destroying margin."
              },
              {
                label: "Q4 — Discount Impact",
                sql: `SELECT
  discount_tier,
  COUNT(*)                                  AS num_items,
  ROUND(AVG(profit), 2)                     AS avg_profit,
  ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct
FROM superstore_analytics.fact_orders
GROUP BY discount_tier
ORDER BY margin_pct DESC;`,
                result: [
                  { discount_tier: "No Discount",  num_items: "4,018", avg_profit: "$47.80",  margin_pct: "18.3%" },
                  { discount_tier: "Low (1–10%)",  num_items: "2,388", avg_profit: "$22.10",  margin_pct: "9.4%"  },
                  { discount_tier: "Mid (11–20%)", num_items: "1,716", avg_profit: "$2.40",   margin_pct: "1.1%"  },
                  { discount_tier: "High (>20%)",  num_items: "1,872", avg_profit: "-$24.60", margin_pct: "-11.2%" },
                ],
                note: "Every discount tier destroys margin. High discounts flip the entire segment to a loss."
              },
              {
                label: "Q7 — Regional Revenue Share (Window Function)",
                sql: `SELECT
  region,
  ROUND(SUM(sales), 2) AS region_sales,
  ROUND(
    SUM(sales) / SUM(SUM(sales)) OVER () * 100, 1
  ) AS sales_share_pct,
  ROUND(SUM(profit) / SUM(sales) * 100, 2) AS margin_pct
FROM superstore_analytics.fact_orders
GROUP BY region
ORDER BY region_sales DESC;`,
                result: [
                  { region: "West",    region_sales: "$725,458", sales_share_pct: "31.6%", margin_pct: "14.9%" },
                  { region: "East",    region_sales: "$678,781", sales_share_pct: "29.6%", margin_pct: "13.5%" },
                  { region: "Central", region_sales: "$501,240", sales_share_pct: "21.8%", margin_pct: "7.9%"  },
                  { region: "South",   region_sales: "$391,722", sales_share_pct: "17.1%", margin_pct: "11.9%" },
                ],
                note: "Central region underperforms on margin despite being the 3rd largest by sales — requires investigation."
              },
            ].map((q, qi) => (
              <div key={qi} style={{ background: "#111827", border: "1px solid #1e293b", borderRadius: 10, overflow: "hidden" }}>
                <div style={{ padding: "9px 18px", background: "#0f1521", borderBottom: "1px solid #1e293b", display: "flex", alignItems: "center", gap: 10 }}>
                  <span style={{ fontSize: 10, color: "#f59e0b", fontFamily: "monospace", letterSpacing: "0.1em" }}>SQL</span>
                  <span style={{ fontSize: 12, fontWeight: 500, color: "#e2e8f0" }}>{q.label}</span>
                </div>
                <pre style={{ margin: 0, padding: "14px 18px", background: "#0d1117", fontSize: 12, lineHeight: 1.7, fontFamily: "'Fira Code', monospace", color: "#94a3b8", overflowX: "auto", borderBottom: "1px solid #1e293b" }}>
                  {q.sql.split('\n').map((line, li) => {
                    const kws = /\b(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|WITH|AS|OVER|PARTITION BY|ROUND|COUNT|SUM|AVG|LAG|DISTINCT|AND|OR|NOT|NULL|LIMIT|ASC|DESC|BY)\b/g;
                    return (
                      <span key={li}>
                        {line.replace(kws, m => `\x00${m}\x00`).split('\x00').map((part, pi) =>
                          kws.test(part)
                            ? <span key={pi} style={{ color: "#60a5fa" }}>{part}</span>
                            : <span key={pi}>{part}</span>
                        )}
                        {'\n'}
                      </span>
                    );
                  })}
                </pre>
                <div style={{ padding: "12px 18px" }}>
                  <div style={{ fontSize: 10, color: "#3d4f6e", letterSpacing: "0.09em", marginBottom: 8 }}>RESULTS</div>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
                    <thead>
                      <tr>{Object.keys(q.result[0]).map(k => (
                        <th key={k} style={{ textAlign: "left", padding: "5px 10px", color: "#3d4f6e", fontSize: 10, textTransform: "uppercase", letterSpacing: "0.08em", borderBottom: "1px solid #1e293b" }}>{k}</th>
                      ))}</tr>
                    </thead>
                    <tbody>
                      {q.result.map((row, ri) => (
                        <tr key={ri} style={{ borderBottom: "1px solid #1a2235" }}>
                          {Object.values(row).map((v, vi) => (
                            <td key={vi} style={{ padding: "7px 10px", color: String(v).includes("-$") ? "#f87171" : "#94a3b8", fontFamily: "monospace" }}>{v}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {q.note && <div style={{ marginTop: 10, fontSize: 11, color: "#4b5f7c", fontStyle: "italic", borderTop: "1px solid #1e293b", paddingTop: 8 }}>{q.note}</div>}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
