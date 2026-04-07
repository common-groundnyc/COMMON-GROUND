# Strategic Product Review — Common Ground Platform Expansion

> **Date**: 2026-04-03
> **Reviewer**: Product Strategy Agent
> **Verdict**: The data layer is strong. The product layer has serious sequencing, distribution, and platform-choice problems that will waste months if not addressed now.

---

## 1. WHO IS THE USER? (The plan doesn't know.)

The roadmap says "zero accounts" but never names a single user persona. This is the most dangerous gap in the entire plan. "NYC residents" is not a user. Here's who actually uses civic data tools, ranked by likelihood of adoption:

| Persona | Size | Needs | Would they use this? |
|---------|------|-------|---------------------|
| **Tenant organizers** (CASA, Met Council, Crown Heights TU) | ~500 active in NYC | Building-level violation data, landlord portfolios, ammo for HP actions | **YES** — this is the core user. JustFix's Who Owns What gets 33k monthly visitors largely from this group. |
| **Legal aid attorneys** (Legal Aid, Brooklyn Legal Services) | ~2,000 in NYC | Quick building lookups during intake, portfolio analysis | **YES** — Brooklyn Legal Services says they use JustFix "every single day." |
| **Investigative journalists** (ProPublica, The City, Gothamist) | ~50-100 covering housing | Cross-referencing entities, following money, finding patterns | **YES** — but they need the MCP/CLI, not a bot. |
| **City council staffers** | ~300 across 51 offices | Constituent service, district-level stats | **MAYBE** — they already have internal tools and 311. |
| **Random NYC residents** | 8.3 million | "Is my building bad?" one-time lookup | **NO** for subscriptions. Maybe for a one-time dashboard visit. |

### The Telegram Problem

**Telegram is the wrong platform for NYC civic engagement.** The numbers are damning:

- Telegram has ~10 million US monthly users total (DemandSage, 2026). NYC is ~2.5% of the US population. That's ~250,000 Telegram users in NYC, optimistically.
- **WhatsApp has 98 million US users** and NYC's mayor launched an official WhatsApp channel specifically because "40% of NYC's population was born outside the US" and WhatsApp is their primary messaging app.
- **NotifyNYC has 1.2 million enrolled users** via SMS/email.
- **Citizen App has 3 million subscribers in NYC** via push notifications.
- SMS reaches 100% of phone owners.

The plan picks Telegram because of Forum Topics, Mini Apps, and bot API quality. These are developer-ergonomics reasons, not user-reach reasons. You are optimizing for the builder's convenience, not the user's reality.

**Recommendation**: Build the notification engine as channel-agnostic from day one. The first channel should be **SMS + email** (Twilio + Resend), not Telegram. Add Telegram as a second channel for the power-user/organizer niche. WhatsApp Business API as a third.

---

## 2. SEQUENCING IS BACKWARDS

### CLI First Is Wrong

The plan builds CLI in Week 1-2. Who is this for? Investigative journalists and power users — maybe 100 people. The CLI validates the MCP client pattern, but it validates nothing about product-market fit.

### The Dashboard Should Come Before the Bot

The plan puts the SQLRooms dashboard at Week 5-7. But the dashboard is the **discovery surface** — it's how people find out Common Ground exists and what it can do. Without it, there's no top-of-funnel. You can't acquire users for a notification bot if they've never seen the data.

### Proposed Re-Sequencing

| Phase | What | Why |
|-------|------|-----|
| **P0** | Subscription API + Parquet exports (same as current) | Foundation |
| **P1** | **Dashboard on website** (SQLRooms) | Discovery surface. Embeddable. Shareable URLs. SEO-indexable. This is how people find you. |
| **P2** | **Email/SMS notifications** (simplest possible alerts) | "Enter your email, pick your building, get weekly violation digest." This is the smallest loop that proves demand. No LLM parsing needed — form-based subscription. |
| **P3** | Telegram bot + NL parsing | Power-user channel for organizers who want richer interaction |
| **P4** | CLI | Developer/journalist tool |
| **P5** | Community features (Forum Topics) | Only after you have 500+ active subscribers to seed it |

### Why This Order Matters

The current plan front-loads the hardest, riskiest features (LLM parsing, Telegram bot framework, Forum Topics) before validating that anyone wants notifications at all. A simple email digest proves the same hypothesis in 1/10th the engineering time.

---

## 3. COMPARABLE PRODUCTS — WHAT WORKED AND WHAT DIED

### Winners

| Product | Users | Why It Worked |
|---------|-------|---------------|
| **Who Owns What** (JustFix) | 33k/month | Single clear use case (lookup a building), embedded in organizer workflows, no account needed |
| **Citizen App** | 3M in NYC | Push notifications about things happening RIGHT NOW near you. Addictive. Real-time. |
| **NotifyNYC** | 1.2M enrolled | Emergency alerts via SMS/email. Zero friction. Government-backed trust. |
| **NYC 311** | Millions of requests/year | Report a problem, get a tracking number. Clear feedback loop. |

### Failures and Semi-Failures

| Product | What Happened | Lesson |
|---------|--------------|--------|
| **Heat Seek NYC** | Built hardware sensors for heating violations. Technically impressive. Never scaled beyond pilot. | Hardware + civic tech = unsustainable without institutional backing. |
| **Civic tech graveyard** (civictech.guide/graveyard) | Hundreds of dead projects | Most died from: no users (built it, nobody came), no funding model, volunteer burnout |
| **SeeClickFix** | Acquired by CivicPlus, B2B pivot | Consumer civic engagement doesn't pay. Pivoted to selling to governments. |

### Where Common Ground Sits

Common Ground's **data layer** is genuinely differentiated — 294 tables, 60M+ rows, entity resolution, cross-domain linking. No other civic tool has this breadth. But the **product layer** is where civic tech projects die. The data is the moat; the product is the risk.

Common Ground complements JustFix (which is housing-only) and competes with nobody on breadth. But breadth is also a curse — "everything about NYC" is not a value proposition. "Your building's violations, your landlord's portfolio, delivered weekly" is.

---

## 4. GROWTH MODEL IS MISSING

The plan has cost projections for 100, 1K, and 10K users but zero strategy for getting there. This is the #1 killer of civic tech projects.

### 0 to 100 Users

**Only path that works**: Partner directly with tenant organizing groups. CASA, Crown Heights Tenant Union, Met Council on Housing, Right to Counsel Coalition. Go to their meetings. Show the tool. Get 5 organizers using it daily. They bring their buildings.

Do NOT launch on Product Hunt. Do NOT post on Reddit. Do NOT rely on word-of-mouth. Civic tools grow through **institutional adoption**, not viral loops.

### 100 to 1,000 Users

Legal aid organizations (Brooklyn Legal Services, Legal Aid Society, Mobilization for Justice). They serve thousands of tenants. If CG becomes part of their intake workflow (like JustFix did), each attorney brings dozens of buildings.

City council offices. If one staffer uses it for constituent services, 50 others hear about it.

### 1,000 to 10,000 Users

This requires press coverage (The City, Gothamist, ProPublica using CG data in investigations), plus the dashboard being publicly accessible and SEO-indexable. "Worst landlords in Brooklyn" as a public page that ranks on Google.

### Retention

The plan has no retention story. Why do people come back?

- **Organizers**: Come back because their buildings have ongoing cases. Natural retention.
- **Residents**: Come once to check their building, never return. Unless you give them a reason — notifications.
- **Journalists**: Come for investigations, sporadic.

Notifications ARE the retention mechanism. But only if the notifications are genuinely useful and not noisy. The 50/user/day rate cap in the plan is absurdly high — most users should get 1-5 notifications per WEEK or they'll mute everything.

---

## 5. WHAT TO CUT

### Cut Now (Scope Creep)

| Feature | Why Cut |
|---------|---------|
| **Forum Topics (Phase 4)** | Community moderation is a full-time job. Toxicity, misinformation, legal liability. You have zero community managers. A dead forum is worse than no forum. Revisit at 1,000+ users. |
| **Telegram Mini App (Phase 6)** | Building a second frontend inside Telegram before the first frontend (website dashboard) is proven? No. Cut entirely until Telegram bot has 500+ active users. |
| **`/ask` streaming (Phase 7)** | Cool demo, expensive at scale ($200/month at 10K users for Sonnet). Defer until you have revenue or grants. |
| **CLI homebrew tap** | Vanity. PyPI is sufficient. |
| **OPFS caching** | Premature optimization for a dashboard with zero users. |
| **PWA + Web Push** | Same. Build it when you have repeat visitors. |
| **RSS/Atom feeds** | Who subscribes to RSS in 2026? Maybe 12 people. |

### Keep / Prioritize

| Feature | Why Keep |
|---------|---------|
| **Dashboard (SQLRooms)** | Discovery surface. Top of funnel. Non-negotiable. |
| **Email digest** | Lowest-friction notification channel. Validates demand. |
| **SMS alerts** | Reaches everyone. NotifyNYC proved this works at 1.2M scale. |
| **Building-level subscriptions** | The core value prop. "Watch my building." |
| **Landlord portfolio view** | The "wow" moment that gets press coverage. |
| **Subscription API** | Foundation for all notification channels. |

### The 80/20

**20% of features that deliver 80% of value:**

1. Dashboard with building lookup + neighborhood overview (discovery)
2. Email/SMS subscription for building-level alerts (retention)
3. Landlord portfolio / network view (differentiation + press)
4. Entity resolution powering cross-dataset connections (technical moat)

Everything else is nice-to-have.

---

## 6. HARD QUESTIONS THE PLAN DOESN'T ANSWER

1. **Who maintains this?** The plan is 8 weeks of building by what appears to be a solo developer. Who handles uptime, bug reports, user support, moderation (if Forum Topics ship), and data freshness after launch?

2. **What's the funding model?** LLM costs scale linearly with users. At 10K users, you're spending $263/month. At 100K users, you're spending $2,600/month. Who pays? Grant funding? Donations? Pro tier?

3. **What happens when the data is wrong?** NYC open data has errors. When a notification says "new violation at your building" but it's a data entry mistake, who handles the angry user? What's the liability?

4. **Why would an organizer use this instead of JustFix?** JustFix is established, trusted, has legal aid partnerships. Common Ground has more data breadth but less depth on housing. The answer needs to be: "CG does everything JustFix does AND shows you campaign donations, corporate filings, court cases, and police complaints." If that's the pitch, lead with it.

5. **What about mobile?** The plan has no native mobile story. The dashboard is web-only. Telegram is the mobile play, but if Telegram is the wrong platform (see section 1), then mobile is unaddressed. A responsive web dashboard + SMS is probably sufficient.

---

## 7. REVISED RECOMMENDATION

### Phase 0 (Week 1): Foundation
- Subscription API + table in DuckLake
- Parquet exports for dashboard

### Phase 1 (Week 2-4): Dashboard
- SQLRooms on website `/explore`
- Building lookup, neighborhood overview, landlord portfolio
- Shareable URLs, SEO-friendly pages
- "Subscribe to this building" email capture form on every building page

### Phase 2 (Week 4-5): Email/SMS Notifications
- Change detection sensor in Dagster (same as current P3)
- Email digest (Resend) — weekly building summary
- SMS alerts (Twilio) — critical violations only
- Form-based subscription (no LLM needed)

### Phase 3 (Week 5-7): Validate and Iterate
- Ship to 5 tenant organizing groups
- Measure: signups, email open rates, SMS delivery, return visits
- If < 50 active subscribers after 4 weeks, stop and reassess
- If > 200, proceed to Phase 4

### Phase 4 (Week 7+): Expand Channels
- Telegram bot with NL parsing (for organizer power users)
- CLI (for journalists/developers)
- Only if Phase 3 validated demand

### Phase 5 (Month 3+): Community
- Forum Topics (only with a moderation plan and 500+ users)
- Mini App (only if Telegram bot has traction)

---

## Bottom Line

The data infrastructure is excellent — 294 tables, entity resolution, Lance embeddings, 15 MCP tools. This is genuinely best-in-class for a civic data platform.

The product plan has three critical flaws:
1. **Wrong platform first** — Telegram reaches ~250K New Yorkers; SMS/email reaches millions
2. **Wrong sequence** — building the bot before the dashboard means no discovery surface
3. **No growth plan** — cost projections without acquisition strategy is fantasy math

Fix the sequencing. Build the dashboard first. Launch with email/SMS. Go to tenant organizer meetings. Let Telegram be a power-user add-on, not the primary surface.

The difference between Common Ground becoming the next Who Owns What (33K monthly users, real impact) and becoming another entry in the civic tech graveyard is not the technology — it's whether the first 50 users are the RIGHT 50 users.
