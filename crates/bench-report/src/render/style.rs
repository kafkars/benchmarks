//! The HTML page's only stylesheet.
//!
//! Inlined rather than linked because the page is a single self-contained
//! document: no stylesheet request, no font request, no script. Colors come
//! from CSS custom properties with a `prefers-color-scheme` override, so the
//! page is legible in either theme without asking the reader to choose.

/// The page's only stylesheet, inlined.
pub(super) const STYLE: &str = "\
:root{color-scheme:light dark;--ink:#16191d;--muted:#5b6470;--rule:#d6dae0;--bg:#ffffff;\
--panel:#f5f7f9;--good:#1a7f4b;--bad:#b3261e;--flat:#6b7280;}\
@media (prefers-color-scheme:dark){:root{--ink:#e6e9ee;--muted:#9aa4b2;--rule:#333a44;\
--bg:#14171c;--panel:#1c2028;--good:#4ade80;--bad:#f87171;--flat:#9aa4b2;}}\
body{margin:0 auto;padding:2rem 1.25rem;max-width:60rem;background:var(--bg);color:var(--ink);\
font:16px/1.55 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;}\
h1{font-size:1.6rem;margin:0 0 .5rem;}h2{font-size:1.15rem;margin:2rem 0 .5rem;}\
p{margin:.5rem 0;}code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.9em;}\
.banner{border-left:4px solid var(--bad);background:var(--panel);padding:.6rem .8rem;\
color:var(--muted);}\
.facts{display:grid;grid-template-columns:max-content 1fr;gap:.15rem .75rem;margin:1rem 0;}\
.facts dt{color:var(--muted);}.facts dd{margin:0;}\
table{border-collapse:collapse;width:100%;margin:.5rem 0;font-size:.92rem;}\
th,td{border-bottom:1px solid var(--rule);padding:.35rem .5rem;text-align:left;\
vertical-align:middle;}\
th{color:var(--muted);font-weight:600;}\
th.n,td.n{text-align:right;font-variant-numeric:tabular-nums;}\
td.pass{color:var(--good);}td.fail{color:var(--bad);}\
ul{margin:.5rem 0;padding-left:1.2rem;}li{margin:.2rem 0;}\
.bar{display:block;}\
.bar .axis{stroke:var(--rule);stroke-width:1;}\
.bar .tick{stroke:var(--muted);stroke-width:1;stroke-dasharray:2 2;}\
.bar .parity{stroke:var(--ink);stroke-width:1;}\
.bar .interval.good{fill:var(--good);}.bar .interval.bad{fill:var(--bad);}\
.bar .interval.flat{fill:var(--flat);}\
.bar .point.good{fill:var(--good);}.bar .point.bad{fill:var(--bad);}\
.bar .point.flat{fill:var(--flat);}\
";
