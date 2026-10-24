"""Motion for the dashboard: one small kit used on every page.

The pieces (README, "Dashboard"): job colours with a soft glow, a
liquid fill, a breathing glow while something runs, a spring entrance, a
completion moment (flare, round ripple, check mark), an even exit, honest
progress that never claims to be done early, and numbers that count up.

Motion always means something: breathing only while a job runs, a ripple
only when one finishes, a spring only for something new. Every animation
stops for visitors whose system asks for reduced motion.

The CSS is injected once per page (inject_motion_css). The job bubbles and
the count-up need JavaScript; they run in one inline component
(job_bubbles), which keeps its state in the browser between the 5-second
checks and across pages, so the liquid rises smoothly instead of jumping.
"""

import math
import statistics
from datetime import datetime, timezone
from urllib.parse import quote

import pandas as pd
import streamlit as st

from dashboard_common import DashboardBusy, read_sql
from dashboard_style import DARK, job_color


# Each job's colour (dashboard_style's palette) and the letter in its bubble.
JOB_LETTERS = {
    "catalog_daily": "C",
    "players_hourly": "P",
    "refresh_hourly": "R",
    "reviews_daily": "V",
}
GLOW_ALPHA = (0.45, 0.18)  # inner and outer glow, tuned for the dark page
FAILED_COLOR = DARK.fall

# Bubbles show running jobs and those that finished this long ago, so a
# run shorter than the 5-second check (players_hourly takes about a second)
# still appears, as a short fill and a check mark.
RECENT_SECONDS = 20
CHECK_SECONDS = 3
DEFAULT_EXPECTED_SECONDS = 60


def hex_to_rgba(color, alpha):
    color = color.lstrip("#")
    red, green, blue = (int(color[i:i + 2], 16) for i in (0, 2, 4))
    return f"rgba({red},{green},{blue},{alpha})"


def job_style(job_name):
    """CSS custom properties for a job: its colour and two glow shades."""
    color = job_color(job_name)
    inner, outer = GLOW_ALPHA
    return (
        f"--c:{color};--g:{hex_to_rgba(color, inner)};"
        f"--g2:{hex_to_rgba(color, outer)}"
    )


MOTION_CSS = """
:root {
  --m-spring: cubic-bezier(.34, 1.56, .64, 1);
  --m-ease: cubic-bezier(.65, 0, .35, 1);
  --m-border: #2e2a26;
}
@keyframes m-spring {
  0% { transform: scale(0); }
  55% { transform: scale(1.12); }
  75% { transform: scale(.95); }
  100% { transform: scale(1); }
}
@keyframes m-breathe {
  0%, 100% { filter: drop-shadow(0 0 3px var(--g)) drop-shadow(0 0 8px var(--g2)); }
  50% { filter: drop-shadow(0 0 6px var(--g)) drop-shadow(0 0 15px var(--g2)); }
}
@keyframes m-flare {
  35% { filter: drop-shadow(0 0 9px var(--g)) drop-shadow(0 0 24px var(--g)); }
  100% { filter: drop-shadow(0 0 3px var(--g)) drop-shadow(0 0 8px var(--g2)); }
}
@keyframes m-vanish {
  30% { transform: scale(1.08); opacity: 1; }
  100% { transform: scale(0); opacity: 0; }
}
@keyframes m-ripple {
  from { transform: scale(1); opacity: .9; stroke-width: 2.5; }
  to { transform: scale(1.9); opacity: 0; stroke-width: .5; }
}
@keyframes m-drift { to { transform: translateX(-40px); } }

.m-wave { animation: m-drift 2.2s linear infinite; }
.m-wave2 { animation: m-drift 3.4s linear infinite reverse; }
.m-glow { filter: drop-shadow(0 0 3px var(--g)) drop-shadow(0 0 8px var(--g2)); }
.m-live { animation: m-breathe 2.4s ease-in-out infinite; }

.m-dot {
  display: inline-block; width: .55rem; height: .55rem; border-radius: 50%;
  background: var(--c); margin-right: .45rem; vertical-align: .05rem;
}

/* Job bubbles: a fixed layer under Streamlit's top bar, right side. */
#m-bubbles {
  position: fixed; top: 4.1rem; right: 1.1rem; z-index: 1000;
  display: flex; flex-direction: row-reverse; align-items: center;
  pointer-events: none;
}
#m-bubbles .m-slot {
  width: 54px; height: 50px; display: flex; justify-content: center;
  align-items: center; transition: width .5s var(--m-ease);
}
#m-bubbles .m-slot.m-gone { width: 0; }
#m-bubbles .m-bub {
  width: 42px; height: 42px; pointer-events: auto; cursor: default;
  transform-origin: 50% 50%; animation: m-spring .7s var(--m-spring) both;
}
#m-bubbles .m-bub svg { overflow: visible; display: block; }
#m-bubbles .m-bub.m-done .m-glow { animation: m-flare .9s ease-out forwards; }
#m-bubbles .m-bub.m-out { animation: m-vanish .5s cubic-bezier(.55, 0, .75, 0) forwards; }
#m-bubbles .m-ripple {
  transform-box: fill-box; transform-origin: center;
  animation: m-ripple 1s ease-out forwards;
}

/* Liquid gauge tiles, matching st.metric(border=True). */
.m-tile {
  position: relative; border: 1px solid var(--m-border); border-radius: 5.6px;
  padding: 15px; min-height: 108px; box-sizing: border-box;
}
.m-tile .m-label { font-size: 14px; line-height: 1.6; color: #ededed;
  padding-right: 40px; }
.m-tile .m-value { font-size: 36px; line-height: 52px; margin-top: 2px;
  color: #ededed; white-space: nowrap; }
.m-tile .m-note { font-size: 12px; color: #8c867e; }
.m-tile .m-gauge { position: absolute; top: 12px; right: 12px; }
.m-gauge { display: block; }
.m-gauges { display: flex; flex-wrap: wrap; gap: 1.1rem 1.4rem; margin: .25rem 0 1rem; }
.m-gauges .m-item { display: flex; align-items: center; gap: .6rem; min-width: 11.5rem; }
.m-gauges .m-name { font-size: 13px; color: #ededed; }
.m-gauges .m-age { font-size: 12px; color: #8c867e; }

@media (prefers-reduced-motion: reduce) {
  .m-wave, .m-wave2, .m-live,
  #m-bubbles .m-bub, #m-bubbles .m-bub.m-out, #m-bubbles .m-ripple,
  #m-bubbles .m-bub.m-done .m-glow { animation: none; }
  #m-bubbles .m-slot { transition: none; }
}
"""


def inject_motion_css():
    st.html(f"<style>{MOTION_CSS}</style>")


def dot(job_name, live=False):
    """A small dot in the job's colour; it breathes while the job runs."""
    live_class = " m-live" if live else ""
    return (
        f"<span class='m-dot{live_class}' style='{job_style(job_name)}'"
        " aria-hidden='true'></span>"
    )


def wave_path(amplitude):
    """A sine wave twice as wide as a gauge (it drifts by one period)."""
    points = " ".join(
        f"L{x} {6 + amplitude * math.sin(x / 40 * 2 * math.pi):.2f}"
        for x in range(0, 125, 5)
    )
    return f"M0 6 {points} L120 60 L0 60 Z"


GAUGE_SVG_CSS = (
    "@keyframes d{to{transform:translateX(-40px)}}"
    "@keyframes r{from{transform:translateY(46px)}}"
    ".w{animation:d 2.2s linear infinite}"
    ".w2{animation:d 3.4s linear infinite reverse}"
    ".l{animation:r 1.4s cubic-bezier(.65,0,.35,1) both}"
    "@media (prefers-reduced-motion:reduce){.w,.w2,.l{animation:none}}"
)


def liquid_gauge(fraction, color, size=44, label=None):
    """A circle filled with gently moving liquid up to `fraction`, as an
    <img> holding a self-contained SVG: st.html's sanitiser removes inline
    <svg>, but keeps images, and CSS inside an SVG image still animates.
    The level rises from empty on load. `label` (optional) sits in the
    middle, in the colour above the liquid and white where it is covered.
    """
    fraction = 0.0 if pd.isna(fraction) else max(0.0, min(1.0, fraction))
    level = 40 - fraction * 44
    text = ""

    if label:
        text = (
            f"<text x='22' y='26.5' text-anchor='middle' font-size='12'"
            f" font-family='sans-serif' font-weight='600' fill='{color}'>"
            f"{label}</text><text x='22' y='26.5' text-anchor='middle'"
            f" font-size='12' font-family='sans-serif' font-weight='600'"
            f" fill='#fff' clip-path='url(#t)'>{label}</text>"
        )

    svg = (
        f"<svg xmlns='http://www.w3.org/2000/svg' width='{size}'"
        f" height='{size}' viewBox='-4 -4 52 52'><style>{GAUGE_SVG_CSS}"
        f"</style><defs><clipPath id='c'><circle cx='22' cy='22' r='18.5'/>"
        f"</clipPath><clipPath id='t'><rect x='0' y='{level + 6:.2f}'"
        f" width='44' height='44'/></clipPath></defs>"
        f"<circle cx='22' cy='22' r='18.5' fill='#141414' stroke='{color}'"
        f" stroke-opacity='.3' stroke-width='2'/>"
        f"<g clip-path='url(#c)'><g transform='translate(0,{level:.2f})'>"
        f"<g class='l'><path class='w2' d='{wave_path(2.2)}' fill='{color}'"
        f" fill-opacity='.45'/><path class='w' d='{wave_path(1.6)}'"
        f" fill='{color}'/></g></g></g>"
        f"<circle cx='22' cy='22' r='18.5' fill='none' stroke='{color}'"
        f" stroke-width='2'/>{text}</svg>"
    )
    glow = (
        f"--g:{hex_to_rgba(color, GLOW_ALPHA[0])};"
        f"--g2:{hex_to_rgba(color, GLOW_ALPHA[1])}"
    )
    return (
        f"<img class='m-gauge m-glow' style='{glow}' width='{size}'"
        f" height='{size}' alt='{round(fraction * 100)}%'"
        f" src='data:image/svg+xml,{quote(svg)}'>"
    )


def gauge_tile(label, value_text, fraction, color, note=None, help_text=None):
    """A bordered tile like st.metric(border=True), with a liquid gauge."""
    title = f" title='{help_text}'" if help_text else ""
    note_html = f"<div class='m-note'>{note}</div>" if note else ""
    return (
        f"<div class='m-tile'{title}><div><div class='m-label'>{label}</div>"
        f"<div class='m-value' data-testid='stMetricValue'>{value_text}</div>"
        f"{note_html}</div>"
        f"{liquid_gauge(fraction, color, size=34)}</div>"
    )


# ---- Job bubbles ------------------------------------------------------------

def expected_seconds(history):
    """Median duration of each job's last successful runs, in seconds."""
    expected = {}

    for job_name, group in history.groupby("job_name"):
        durations = [
            value for value in group["duration_seconds"]
            if value is not None and not pd.isna(value) and value > 0
        ]

        if durations:
            expected[job_name] = float(statistics.median(durations))

    return expected


def bubble_runs(runs, expected, now):
    """The data the bubbles component draws: one entry per running or
    recently finished run. `now` is naive UTC, like the stored times."""
    entries = []

    for run in runs.itertuples():
        finished = None if pd.isna(run.finished_at) else run.finished_at
        end = finished or now
        entries.append({
            "id": int(run.run_id),
            "job": run.job_name,
            "letter": JOB_LETTERS.get(run.job_name, run.job_name[:1].upper()),
            "style": job_style(run.job_name),
            "color": job_color(run.job_name),
            "status": run.status,
            "elapsed": max(0.0, (end - run.started_at).total_seconds()),
            "expected": expected.get(
                run.job_name, DEFAULT_EXPECTED_SECONDS
            ),
        })

    return entries


@st.cache_data(ttl=CHECK_SECONDS, show_spinner=False)
def _active_runs():
    # Shared by every visitor: at most one small query per check.
    return read_sql(
        """
        SELECT run_id, job_name, status, started_at, finished_at
        FROM pipeline_runs
        WHERE status = 'RUNNING'
           OR finished_at >= UTC_TIMESTAMP() - INTERVAL %s SECOND
        ORDER BY run_id
        """,
        (RECENT_SECONDS,)
    )


def running_jobs():
    """Jobs with a run in progress now (the bubbles' shared 3-second
    query), for the sidebar's breathing dots."""
    runs = _active_runs()
    return set(runs.loc[runs["status"] == "RUNNING", "job_name"])


@st.cache_data(ttl=300, show_spinner=False)
def _run_history():
    return read_sql(
        """
        SELECT job_name,
               TIMESTAMPDIFF(MICROSECOND, started_at, finished_at) / 1e6
                   AS duration_seconds
        FROM (
            SELECT job_name, started_at, finished_at,
                   ROW_NUMBER() OVER (
                       PARTITION BY job_name ORDER BY run_id DESC
                   ) AS position
            FROM pipeline_runs
            WHERE status = 'SUCCESS'
              AND started_at >= UTC_TIMESTAMP() - INTERVAL 7 DAY
        ) AS recent
        WHERE position <= 10
        """
    )


BUBBLES_JS = r"""
const R = 18.5;
const DONE_HOLD_MS = 1500;
const reduced = () =>
  window.matchMedia("(prefers-reduced-motion: reduce)").matches;

function wavePath(amp) {
  let d = "M0 6";
  for (let x = 0; x <= 120; x += 5)
    d += " L" + x + " " + (6 + amp * Math.sin(x / 40 * 2 * Math.PI)).toFixed(2);
  return d + " L120 60 L0 60 Z";
}

function createMotion() {
  const layer = document.createElement("div");
  layer.id = "m-bubbles";
  layer.setAttribute("aria-live", "polite");
  document.body.appendChild(layer);
  const bubbles = new Map();
  const finished = new Set();

  function make(run) {
    const id = "mb" + run.id;
    const slot = document.createElement("div");
    slot.className = "m-slot";
    slot.innerHTML =
      '<div class="m-bub" style="' + run.style + '">' +
      '<svg width="42" height="42" viewBox="0 0 44 44">' +
      '<defs><clipPath id="' + id + 'c"><circle cx="22" cy="22" r="' + R + '"/></clipPath>' +
      '<clipPath id="' + id + 't"><rect class="m-tclip" x="0" y="50" width="44" height="44"/></clipPath></defs>' +
      '<g class="m-glow m-live">' +
      '<circle cx="22" cy="22" r="' + R + '" fill="#141414" stroke="var(--c)" stroke-opacity=".3" stroke-width="2"/>' +
      '<g clip-path="url(#' + id + 'c)"><g class="m-lv" transform="translate(0,44)">' +
      '<path class="m-wave2" d="' + wavePath(2.2) + '" fill="var(--c)" fill-opacity=".45"/>' +
      '<path class="m-wave" d="' + wavePath(1.6) + '" fill="var(--c)"/></g></g>' +
      '<circle cx="22" cy="22" r="' + R + '" fill="none" stroke="var(--c)" stroke-width="2"/></g>' +
      '<text class="m-l1" x="22" y="27" text-anchor="middle" font-size="14" font-weight="600" fill="var(--c)">' + run.letter + '</text>' +
      '<text class="m-l2" x="22" y="27" text-anchor="middle" font-size="14" font-weight="600" fill="#fff" clip-path="url(#' + id + 't)">' + run.letter + '</text>' +
      '</svg></div>';
    layer.appendChild(slot);
    return {
      slot, bub: slot.firstChild, run,
      lv: slot.querySelector(".m-lv"), clip: slot.querySelector(".m-tclip"),
      t0: performance.now() - run.elapsed * 1000, shown: 0, done: false,
    };
  }

  function finish(b) {
    b.done = true;
    const failed = b.run.status === "FAILED";
    const svg = b.bub.querySelector("svg");
    const mark = failed ? "✕" : "✓";
    svg.querySelector(".m-l1").textContent = "";
    svg.querySelector(".m-l2").textContent = mark;
    svg.querySelector(".m-glow").classList.remove("m-live");
    svg.insertAdjacentHTML("beforeend",
      '<circle class="m-ripple" cx="22" cy="22" r="' + R + '" fill="none" stroke="' +
      (failed ? "__FAILED__" : "var(--c)") + '"/>');
    b.bub.classList.add("m-done");
    b.bub.title = b.run.job + (failed ? ": failed" : ": finished");
    setTimeout(() => {
      b.bub.classList.add("m-out");
      setTimeout(() => {
        b.slot.classList.add("m-gone");
        setTimeout(() => { b.slot.remove(); bubbles.delete(b.run.id); }, 520);
      }, 480);
    }, DONE_HOLD_MS);
    finished.add(b.run.id);
  }

  // Animation frames pause in a hidden tab; a slower timer then keeps the
  // state moving, so finished runs still leave instead of piling up.
  // The level eases toward its target at the same speed at any tick rate
  // (a time constant, not a fraction per frame).
  const EASE_MS = 270;
  let lastTick = performance.now();
  function tick() {
    const now = performance.now();
    const ease = 1 - Math.exp(-(now - lastTick) / EASE_MS);
    lastTick = now;
    for (const b of bubbles.values()) {
      if (b.done) continue;
      const sec = (now - b.t0) / 1000;
      const running = b.run.status === "RUNNING";
      const target = running ? Math.min(sec / b.run.expected, 0.93) : 1;
      b.shown = reduced() ? target : b.shown + (target - b.shown) * ease;
      const y = 40 - b.shown * 44;
      b.lv.setAttribute("transform", "translate(0," + y.toFixed(2) + ")");
      b.clip.setAttribute("y", (y + 6).toFixed(2));
      const left = Math.max(0, Math.round(b.run.expected - sec));
      b.bub.title = b.run.job + ": running " + Math.round(sec) + " s, " +
        (running && sec > b.run.expected ? "taking longer than usual"
                                         : "about " + left + " s left");
      if (!running && b.shown > 0.985) finish(b);
    }
  }
  function frame() { tick(); requestAnimationFrame(frame); }
  requestAnimationFrame(frame);
  setInterval(() => { if (performance.now() - lastTick > 400) tick(); }, 250);

  function update(runs) {
    const seen = new Set();
    for (const run of runs) {
      seen.add(run.id);
      const known = bubbles.get(run.id);
      if (known) { known.run = Object.assign(known.run, run); continue; }
      if (finished.has(run.id)) continue;
      bubbles.set(run.id, make(run));
    }
    // A run that left the list without a final status here finished.
    for (const b of bubbles.values())
      if (!seen.has(b.run.id) && !b.done && b.run.status === "RUNNING")
        b.run.status = "SUCCESS";
  }

  // Count-up: the dashboard's number tiles roll up to a new value.
  const values = new WeakMap();
  // The text node holding the number; React may split the value into
  // several text nodes (" ", "6.07M", " "). It is changed in place, never
  // replaced, because React keeps a reference to it for later updates.
  function numberNode(el) {
    const walker = document.createTreeWalker(el, NodeFilter.SHOW_TEXT);
    for (let node = walker.nextNode(); node; node = walker.nextNode())
      if (/[0-9]/.test(node.nodeValue)) return node;
    return null;
  }
  function animateNumber(el) {
    const node = numberNode(el);
    if (!node) return;
    const text = node.nodeValue;
    const state = values.get(el) || {};
    if (state.text === text || state.writing === text) return;
    const match = text.match(/^([^0-9-]*)(-?[0-9][0-9,]*(?:\.[0-9]+)?)(.*)$/s);
    if (!match || reduced()) { values.set(el, { text }); return; }
    const to = parseFloat(match[2].replace(/,/g, ""));
    const decimals = (match[2].split(".")[1] || "").length;
    const grouped = match[2].includes(",");
    const from = state.value === undefined ? 0 : state.value;
    const format = v => match[1] + v.toLocaleString("en-US", {
      minimumFractionDigits: decimals, maximumFractionDigits: decimals,
      useGrouping: grouped }) + match[3];
    const mine = { text, value: to, writing: null };
    values.set(el, mine);
    // Stops when React shows a newer value (that starts its own count-up).
    const write = out => {
      if (values.get(el) !== mine) return false;
      if (node.nodeValue !== text && node.nodeValue !== mine.writing)
        return false;
      mine.writing = out;
      node.nodeValue = out;
      return true;
    };
    const t0 = performance.now();
    (function step() {
      const t = Math.min(1, (performance.now() - t0) / 900);
      const out = t < 1
        ? format(from + (to - from) * (1 - Math.pow(1 - t, 3))) : text;
      if (write(out) && t < 1) requestAnimationFrame(step);
    })();
    // Animation frames pause in a hidden tab: never leave a half-counted
    // number on screen.
    setTimeout(() => write(text), 1000);
  }
  const scan = () => document
    .querySelectorAll('[data-testid="stMetricValue"]').forEach(animateNumber);
  new MutationObserver(scan).observe(document.body,
    { childList: true, subtree: true, characterData: true });
  scan();

  return { update };
}

export default function (component) {
  const motion = window.__steamMotion || (window.__steamMotion = createMotion());
  motion.update((component.data && component.data.runs) || []);
}
""".replace("__FAILED__", FAILED_COLOR)

_bubbles_component = st.components.v2.component(
    "job_bubbles",
    js=BUBBLES_JS,
)


@st.fragment(run_every=CHECK_SECONDS)
def job_bubbles():
    """Check for running jobs every few seconds and hand them to the
    browser, which animates the bubbles (top right of every page)."""
    try:
        runs = _active_runs()
        expected = expected_seconds(_run_history()) if not runs.empty else {}
    except DashboardBusy:
        return

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    _bubbles_component(
        data={"runs": bubble_runs(runs, expected, now)},
        key="job_bubbles",
    )
