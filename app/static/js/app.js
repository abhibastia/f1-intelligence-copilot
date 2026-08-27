// Applied as early as possible: setting the attribute after first paint makes
// the page flash the wrong theme on every load for anyone who has chosen the
// non-default one.
(function () {
  const saved = localStorage.getItem("f1-theme");
  if (saved === "light" || saved === "dark") {
    document.documentElement.setAttribute("data-theme", saved);
  }
})();

const SUN = '<circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/>';
const MOON = '<path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8z"/>';

function currentTheme() {
  return document.documentElement.getAttribute("data-theme")
      || (window.matchMedia("(prefers-color-scheme: light)").matches ? "light" : "dark");
}

function paintThemeButton() {
  const dark = currentTheme() === "dark";
  // Offer the theme you would switch TO, not the one you are in - a button
  // labelled with the current state reads as a status, not a control.
  document.getElementById("theme-icon").innerHTML = dark ? SUN : MOON;
  document.getElementById("theme-label").textContent = dark ? "Light" : "Dark";
}

function toggleTheme() {
  const next = currentTheme() === "dark" ? "light" : "dark";
  document.documentElement.setAttribute("data-theme", next);
  localStorage.setItem("f1-theme", next);
  paintThemeButton();
}

window.addEventListener("DOMContentLoaded", paintThemeButton);
window.addEventListener("DOMContentLoaded", restoreChat);
// Follow the OS while the reader has expressed no preference of their own.
window.matchMedia("(prefers-color-scheme: light)")
      .addEventListener("change", () => {
        if (!localStorage.getItem("f1-theme")) paintThemeButton();
      });

const esc = s => String(s ?? "").replace(/[&<>"']/g, c =>
  ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));

/* ---- the copilot rail ----
   Open by default on a wide screen and closed on a narrow one, where it would
   cover the content it is meant to sit beside. The choice is remembered, but
   only for desktop: a phone that remembered "open" would load with the rail
   over the page every time. */
const NARROW = window.matchMedia("(max-width: 68rem)");

function applyRail(open) {
  document.body.classList.toggle("rail-closed", !open);
}

function toggleRail() {
  const open = document.body.classList.contains("rail-closed");
  applyRail(open);
  if (!NARROW.matches) localStorage.setItem("f1-rail", open ? "open" : "closed");
  if (open) document.getElementById("q-chat").focus();
}

applyRail(NARROW.matches ? false : localStorage.getItem("f1-rail") !== "closed");
// Esc closes the overlay, which is the gesture people expect from something
// that covers the page.
document.addEventListener("keydown", e => {
  if (e.key === "Escape" && NARROW.matches &&
      !document.body.classList.contains("rail-closed")) toggleRail();
});

/* ---- section navigation ----
   Seven sections with no wayfinding meant a reader could not tell how much was
   below them, and the CDF section at the bottom went unseen. Scrollspy marks
   where they are; IntersectionObserver rather than a scroll handler so it costs
   nothing per frame. */
window.addEventListener("DOMContentLoaded", () => {
  const links = [...document.querySelectorAll(".secnav a")];
  if (!links.length) return;
  const byId = new Map(links.map(a => [a.getAttribute("href").slice(1), a]));
  const targets = [...byId.keys()]
    .map(id => document.getElementById(id)).filter(Boolean);

  const seen = new Set();
  const io = new IntersectionObserver(entries => {
    entries.forEach(e => e.isIntersecting ? seen.add(e.target.id) : seen.delete(e.target.id));
    // Mark the topmost visible section, so scrolling down does not leave the
    // marker on whichever section happened to fire its callback last.
    const current = targets.find(t => seen.has(t.id));
    links.forEach(a => a.removeAttribute("aria-current"));
    if (current && byId.get(current.id)) byId.get(current.id).setAttribute("aria-current", "true");
  }, {rootMargin: "-15% 0px -70% 0px"});
  targets.forEach(t => io.observe(t));
});

let history = [];

/* ---- conversation persistence ----
   The "saved — view it" link reloads the page, because the activity section is
   rendered server-side and only a reload shows the row the agent just wrote.
   When the chat was a section that scrolled away, losing it on reload cost
   nothing. Docked, it is the thing you are looking at, and having it wiped by
   following the app's own link is the sort of small betrayal that makes an
   assistant feel disposable. Session storage, so it lasts a tab and not longer. */
// Set by the inline config script in index.html - starters and the current
// season are the only two values this file needs from Jinja.
const START_QUESTIONS = window.APP_CONFIG.starters;
const CHAT_KEY = "f1-chat";
let rendered = [];
let lastFollowups = [];

function saveChat() {
  try {
    sessionStorage.setItem(CHAT_KEY,
      JSON.stringify({rendered, history, followups: lastFollowups}));
  } catch (e) { /* private mode, or full: not worth breaking the chat over */ }
}

function restoreChat() {
  let saved;
  try { saved = JSON.parse(sessionStorage.getItem(CHAT_KEY) || "null"); } catch (e) { return; }
  if (!saved || !saved.rendered || !saved.rendered.length) return;
  const log = document.getElementById("log");
  log.innerHTML = "";
  rendered = saved.rendered;
  history = saved.history || [];
  // innerHTML is safe here: every fragment was built by addMsg from esc()'d
  // values, and sessionStorage is same-origin and per-tab.
  rendered.forEach(m => {
    const el = document.createElement("div");
    el.className = "msg " + m.cls;
    el.innerHTML = `<div class="who">${esc(m.who)}</div><div class="bubble">${m.html}</div>`;
    log.appendChild(el);
  });
  log.scrollTop = log.scrollHeight;
  if (saved.followups) renderFollowups(saved.followups);
}

function clearChat() {
  rendered = []; history = [];
  try { sessionStorage.removeItem(CHAT_KEY); } catch (e) {}
  document.getElementById("log").innerHTML =
    `<div class="msg bot"><div class="who">Copilot</div><div class="bubble">` +
    `Ask me why a race turned out the way it did — or tell me to track a ` +
    `driver, save a note, or log a prediction.</div></div>`;
  renderFollowups(START_QUESTIONS);
  document.getElementById("q-chat").focus();
}

// Arriving from a write: draw the eye to the section that just changed,
// otherwise the page looks identical to the one the user just left.
if (location.hash === "#activity") {
  window.addEventListener("DOMContentLoaded", () => {
    const el = document.getElementById("activity");
    if (el) el.classList.add("flash");
  });
}

function askThis(text) {
  document.getElementById("q-chat").value = text;
  sendChat(new Event("submit"));
}

function addMsg(who, cls, html) {
  const log = document.getElementById("log");
  const el = document.createElement("div");
  el.className = "msg " + cls;
  el.innerHTML = `<div class="who">${esc(who)}</div><div class="bubble">${html}</div>`;
  log.appendChild(el);
  log.scrollTop = log.scrollHeight;
  return el;
}

// Called once a bubble has its final content, since the pending "thinking"
// state is replaced in place and there is no point persisting it.
function recordMsg(who, cls, el) {
  rendered.push({who, cls, html: el.querySelector(".bubble").innerHTML});
  rendered = rendered.slice(-24);
}

function renderTrace(trace) {
  if (!trace || !trace.length) return "";
  return `<div class="trace">` + trace.map(t => {
    const args = Object.entries(t.arguments || {})
      .map(([k, v]) => `${esc(k)}=${esc(JSON.stringify(v))}`).join(", ");
    // The returned value goes under the call. Without it the trace shows only
    // that a tool was named, which looks the same whether it answered or came
    // back empty - and the point of showing the trace is that it is evidence.
    const ev = t.evidence
      ? `<span class="ev">↳ ${esc(t.evidence)}</span>` : "";
    return `<div class="tcall ${t.is_write ? "write" : ""}">
      <span class="badge">${t.is_write ? "write" : "read"}</span>
      <span class="tname">${esc(t.tool)}</span>(${args})${ev}</div>`;
  }).join("") + `</div>`;
}

// Follow-ups come from the server, derived from the tool calls the answer
// actually made, so a suggestion can only ever be a question this data answers.
function renderFollowups(list) {
  const box = document.getElementById("starters");
  const label = document.getElementById("starters-label");
  if (!box) return;
  if (!list || !list.length) { box.innerHTML = ""; if (label) label.style.display = "none"; return; }
  if (label) { label.style.display = ""; label.textContent = "Ask next"; }
  box.innerHTML = list.map(q =>
    `<span class="chip" onclick="askThis(this.textContent.trim())">${esc(q)}</span>`).join("");
}

async function sendChat(ev) {
  ev.preventDefault();
  const input = document.getElementById("q-chat");
  const btn = document.getElementById("send");
  const question = input.value.trim();
  if (!question) return;

  const userEl = addMsg("You", "user", esc(question));
  recordMsg("You", "user", userEl);
  input.value = "";
  btn.disabled = true;
  const pending = addMsg("Copilot", "bot", '<span class="thinking">thinking</span>');

  try {
    const r = await fetch("/api/chat", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({question, history})
    });
    const d = await r.json();
    if (!r.ok) throw new Error(d.error || "The assistant is unavailable.");

    const answer = esc(d.answer || "").split("\n\n")
      .filter(Boolean).map(p => `<p>${p.replace(/\n/g, "<br>")}</p>`).join("");
    // The season query param carries the reader's current selection through
    // the reload; omitted rather than embedding a literal "null"/"None" when
    // no season is selected (a state the app never actually renders into,
    // but the link should still be a clean URL if it ever does).
    const seasonParam = window.APP_CONFIG.season != null ? `?season=${window.APP_CONFIG.season}` : "";
    pending.querySelector(".bubble").innerHTML = renderTrace(d.trace) + answer +
      (d.wrote ? `<p style="font-size:.85rem">
         <a class="saved" href="${seasonParam}#activity">
           Saved — view it under “What the assistant has done” &rarr;</a></p>` : "");

    renderFollowups(d.followups);

    history.push({role: "user", content: question});
    history.push({role: "assistant", content: d.answer || ""});
    history = history.slice(-6);
    recordMsg("Copilot", "bot", pending);
    lastFollowups = d.followups || [];
    saveChat();
  } catch (e) {
    pending.querySelector(".bubble").innerHTML =
      `<span style="color:var(--accent-ink)">${esc(e.message)}</span>` +
      ` <button class="chip" style="margin-left:.4rem"
                onclick="askThis(${JSON.stringify(question).replace(/"/g, "&quot;")})">Retry</button>`;
    recordMsg("Copilot", "bot", pending);
    saveChat();
  } finally {
    btn.disabled = false;
    input.focus();
  }
}

function quick(text) {
  document.getElementById("q").value = text;
  runSearch(new Event("submit"));
}

async function runSearch(ev) {
  ev.preventDefault();
  const q = document.getElementById("q").value.trim();
  const season = document.getElementById("season").value;
  const out = document.getElementById("results");
  if (!q) { out.innerHTML = '<p class="empty">Enter something to search for.</p>'; return; }
  out.innerHTML = '<p class="empty">Searching…</p>';

  const url = "/api/search?q=" + encodeURIComponent(q) + (season ? "&season=" + season : "");
  let data;
  try {
    const r = await fetch(url);
    data = await r.json();
    if (!r.ok) throw new Error(data.error || "Search failed");
  } catch (e) {
    out.innerHTML = '<p class="empty">' + esc(e.message) + '</p>';
    return;
  }
  if (!data.results.length) {
    out.innerHTML = '<p class="empty">No passages matched.</p>';
    return;
  }
  out.innerHTML = data.results.map(r => `
    <div class="hit">
      <div class="hit-head">
        <span class="r">${esc(r.race_name)}</span>
        <span class="s">${esc(r.season)} · round ${esc(r.round)} · ${esc(r.section)}</span>
        ${r.conditions ? `<span class="pill ${r.was_wet ? "wet" : "dry"}">${esc(r.conditions)}${
            r.precipitation_mm != null ? " · " + esc(r.precipitation_mm) + " mm" : ""}</span>` : ""}
        <span class="sim">${Number(r.similarity).toFixed(3)}</span>
      </div>
      <p>${esc(r.chunk_text).slice(0, 420)}…</p>
      ${r.url ? `<a class="src" href="${esc(r.url)}" target="_blank" rel="noopener">
          Read the full ${esc(r.section)} &rarr;</a>` : ""}
    </div>`).join("");
}
