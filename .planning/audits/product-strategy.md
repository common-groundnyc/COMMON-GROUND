# Product Strategy & UX Audit — Common Ground NYC

**Auditor:** product-strategist
**Date:** 2026-04-01
**Scope:** Website, MCP tools (API surface), brand, competitive landscape, growth strategy

---

## 1. Executive Summary

Common Ground has an exceptionally strong thesis ("cross-reference as the unit of value"), a working data lake (294 tables, 60M+ rows), and 14 MCP super tools that already deliver on the promise. The brand strategy documents are some of the most sophisticated I've seen in civic tech — the "Five Blindnesses" framework, the "parasite strategy" distribution model, and the "check the city" verb are all genuinely original.

**The gap is not vision. The gap is execution surface.** The product lives entirely inside MCP-connected AI clients (Claude, ChatGPT). There is no direct-to-consumer experience yet — no browser extension, no bot, no watch/alert system, no receipt cards. The website is a manifesto lander under construction. The thesis describes five surfaces; only one (the MCP conversation) exists.

**Bottom line:** CG is a world-class data engine with a B2AI distribution channel and zero B2C channels. The next 12 months should focus on building the surfaces that put cross-referenced truth in front of non-technical users at the moment of decision.

---

## 2. Current State Assessment

### 2.1 What Exists

| Asset | State | Quality |
|-------|-------|---------|
| Data lake (DuckDB/DuckLake) | Production | Excellent — 294 tables, 14 schemas, daily sync |
| MCP server (FastMCP) | Production | Excellent — 14 super tools, middleware pipeline, security hardened |
| Entity resolution (Splink + Lance) | Production | Strong — 2.96M names, vector routing, cross-reference |
| Website (Next.js) | In progress | Hero + below-fold built, manifesto sections partially complete |
| Browser extension | Not started | Described in thesis — highest-impact surface |
| Social bot (Reddit/X) | Not started | Described in thesis |
| Watch/alert system | Not started | Described in thesis |
| Receipt cards (shareable) | Not started | Described in thesis — key viral mechanic |
| Connect/MCP setup page | Partial | Route exists, not complete |

### 2.2 Website Architecture

The website correctly positions itself as a "public evidence room, not a product." Routes are clean and intentional:
- `/` — Manifesto (the lander)
- `/receipts/{slug}` — Shareable findings
- `/connect` — MCP setup + donations
- `/how` — Methodology
- `/supporters` — Sustainers list
- `/mcp` — Schema browser / tool documentation

Current components built: hero, scroll indicator, receipt-vs-google comparison, below-fold sections, light mode variant, search, LLM demo, receipts, confrontation. The design system is well-defined (3 accent colors, Matter/JetBrains/Space Grotesk typography, "civic punk" aesthetic).

### 2.3 MCP Tool Surface (= the actual product API)

The 14 tools are organized as "super tools" — each absorbs multiple granular operations:

| Tool | Purpose | User Question Pattern |
|------|---------|----------------------|
| `building()` | Full building profile from address/BBL | "What's going on at this address?" |
| `entity()` | Cross-reference a person across all datasets | "Who is this person?" |
| `network()` | Ownership, corporate, political graphs | "Who owns what? Follow the money." |
| `neighborhood()` | ZIP-level portrait, comparison, gentrification | "What's this neighborhood like?" |
| `school()` | School profiles and comparisons | "How's this school?" |
| `safety()` | Crime, crashes, shootings | "Is this area safe?" |
| `health()` | Health outcomes, COVID, hospitals | "What are the health risks?" |
| `legal()` | Court cases, settlements | "Any legal issues?" |
| `civic()` | Contracts, permits, budget | "What's the city spending?" |
| `transit()` | Parking, MTA, traffic | "How's the commute?" |
| `services()` | Childcare, shelters, food | "What services are nearby?" |
| `semantic_search()` | Concept-based search across all data | "Find complaints about X" |
| `suggest()` | Discovery and onboarding | "What can I explore?" |
| `query()` | Raw SQL and exports | Power user escape hatch |

This is a remarkably complete investigative toolkit. The routing instructions in the MCP server description are well-designed for LLM comprehension. The workflow chains (e.g., `building() -> entity() -> network(type="ownership")`) demonstrate genuine investigative thinking.

---

## 3. Competitive Landscape

### 3.1 Direct Competitors (None Truly Comparable)

| Platform | What They Do | CG Advantage |
|----------|-------------|--------------|
| **NYC Open Data Portal** | Publishes raw datasets | CG crosses them; portal just lists |
| **LittleSis** | Relationship maps of powerful people (1.6M relationships, 400K entities) | CG has local depth; LittleSis has national breadth. No NYC-specific cross-reference |
| **OpenCorporates** | Company data (192M companies, 140 jurisdictions) | CG links corporate entities to buildings, violations, political donations |
| **OpenSecrets/FollowTheMoney** | Campaign finance tracking | CG connects donations to real-world outcomes (contracts, enforcement patterns) |
| **MuckRock/DocumentCloud** | FOIA filing + document analysis | Complementary, not competitive. CG could generate FOIA-worthy leads |
| **BetaNYC** | Community organizing around open data | Community, not product. CG is the tool BetaNYC members need |
| **PropertyData (UK)** | Browser extension overlaying property market data | Similar surface concept, different domain and jurisdiction |

**Key finding:** No existing platform crosses NYC's public data into a conversational, AI-native interface. CG's moat is the integrated lake + LLM delivery. LittleSis is the closest conceptual peer (relationship mapping), but it's manually curated and national, not computationally cross-referenced and NYC-deep.

### 3.2 Emerging Competitive Threats

1. **AI copilots with web access** — Claude/ChatGPT can already search the web. If they get good enough at finding and synthesizing open data directly, the MCP advantage shrinks. CG's defense: pre-computed crosses are faster, more accurate, and more complete than real-time web scraping.

2. **StreetEasy/Zillow adding data overlays** — Real estate platforms could add violation/safety data natively. CG's defense: they'll never add data that makes listings look bad (misaligned incentives).

3. **City government improving its own portals** — NYC could build better tools. CG's defense: governments optimize for compliance, not user comprehension. This misalignment is structural.

---

## 4. Gap Analysis: Thesis vs. Reality

The thesis describes five surfaces. Here's the gap for each:

### 4.1 Surface 1: The Extension (NOT STARTED — HIGHEST IMPACT)

**Thesis:** Browser extension on StreetEasy, Zillow, Google Maps. Auto-annotates listings with violations, noise, flood risk, school quality, transit reliability. "80% of initial users come from here."

**Current state:** Nothing built. No Chrome extension project exists.

**Why this matters most:** Apartment hunting is the single highest-intent, highest-anxiety moment in NYC life. People are already on StreetEasy. They're already making decisions. CG data appearing at the point of decision — without the user having to know CG exists — is the "parasite strategy" in its purest form.

**What's needed:**
- Chrome extension that detects address/listing pages on StreetEasy, Zillow, apartments.com, Google Maps
- Calls CG MCP server (or a lightweight REST API layer) with the detected address
- Renders a compact context card overlay: violations, landlord score, neighborhood trajectory, noise profile
- "Powered by Common Ground" attribution with link to full profile
- Estimated effort: 4-6 weeks for MVP

### 4.2 Surface 2: The Bot (NOT STARTED — HIGH VIRAL POTENTIAL)

**Thesis:** Summonable on Reddit, X, Discord, Nextdoor. Tag it to fact-check claims with receipt cards.

**Current state:** Nothing built.

**Why this matters:** Every time someone fact-checks a claim with a CG receipt card, it's a micro-advertisement. The bot borrows the audience of every platform it operates on. This is the viral growth engine.

**What's needed:**
- Reddit bot watching for mentions in NYC subreddits
- X/Twitter bot responding to @mentions
- Discord bot for NYC community servers
- Each bot: parse the query, call MCP tools, format a receipt card, reply
- Estimated effort: 3-4 weeks per platform

### 4.3 Surface 3: The Conversation (EXISTS — NEEDS POLISH)

**Thesis:** The moat. Conversational, contextual, follows up, helps you think.

**Current state:** Works via MCP in Claude/ChatGPT. The `suggest()` tool provides onboarding. The tool routing instructions are well-designed.

**Gaps:**
- No "compared to what?" follow-up prompting built into responses
- No session memory (each query is independent)
- No guided investigation workflows ("You asked about this building. Did you know the owner controls 47 others?")
- The conversation quality depends entirely on the host LLM's behavior, not CG's system

### 4.4 Surface 4: The Watch (NOT STARTED — RETENTION ENGINE)

**Thesis:** Follow places, get notified when reality shifts. Material changes only.

**Current state:** Nothing built. No user accounts, no saved places, no notification infrastructure.

**Why this matters:** This converts one-time users into permanent users. Without it, CG is a tool people use once and forget about.

**What's needed:**
- User identity (email-based, lightweight)
- "Follow this address/ZIP" from any surface
- Change detection pipeline: compare daily syncs, flag material differences
- Email/push notification for material changes (new violations, permit filings, rezoning proposals, ownership transfers)
- Estimated effort: 8-12 weeks

### 4.5 Surface 5: The Receipts (PARTIALLY STARTED)

**Thesis:** Every check produces a portable, shareable context card with question, answer, sources, caveats.

**Current state:** `/receipts/{slug}` route exists in website architecture. Not yet functional.

**Why this matters:** Receipts are the currency of trust. They're what people screenshot, paste in group chats, bring to community board meetings. They're CG's unit of viral distribution.

**What's needed:**
- Canonical receipt card component (render from MCP tool output)
- Permalink generation for every query result
- OG meta tags for social sharing (question + key finding + source count)
- Print-friendly / screenshot-optimized layout
- Estimated effort: 3-4 weeks

---

## 5. Strategic Recommendations

### 5.1 Priority Order (Next 12 Months)

| # | Surface | Why First | Timeline |
|---|---------|-----------|----------|
| 1 | **Receipt cards** | Foundation for everything else. Bot needs cards. Extension needs cards. Social sharing needs cards. | Weeks 1-4 |
| 2 | **Browser extension** | Highest-intent user moment. Proves the "parasite strategy." Zero-effort discovery. | Weeks 4-10 |
| 3 | **Reddit/X bot** | Viral growth engine. Every fact-check is an ad. Borrowing platform audiences. | Weeks 8-12 |
| 4 | **Complete manifesto lander** | Conversion surface for all other channels. Currently half-built. | Weeks 2-8 (parallel) |
| 5 | **Watch/alert system** | Retention. Needs user identity. Build after initial traction. | Weeks 12-20 |

### 5.2 The "Aha Moment" Problem

**CG's biggest UX challenge:** Users don't know what questions to ask.

The `suggest()` tool helps, but only for MCP users who are already connected. For everyone else, there's no discovery mechanism. The thesis acknowledges this ("Come for the gossip") but the current product doesn't deliver gossip to anyone.

**Recommendation: Lead with pre-computed findings, not empty search boxes.**

- Homepage should show 5-10 real findings that make people say "wait, you can see THAT?"
- Browser extension should show data before the user asks for it
- Bot should proactively surface interesting findings in relevant threads
- Receipt cards should be shareable standalone (no CG account needed)

### 5.3 Sustainability Model Gaps

The thesis describes a Wikipedia-style model: free tier (rate-limited), donate >= $25/year for high-capacity access. This is clean and well-designed. Current gaps:

- No payment infrastructure (Stripe integration not present)
- No rate limiting by user identity (MCP server has global rate limits only)
- No `/supporters` page content
- No donation flow on `/connect`

### 5.4 Language and Positioning

The thesis is emphatic: never use "data," "civic," "dashboard," "portal," "tables," "municipal," "transparency." User-facing language should be: "Check the city," "What's really going on here," "Your city, with receipts."

**Current violations:** The `/mcp` page is a schema browser showing table names and tool documentation. This is developer-facing, not user-facing. Need a clear separation between the developer/MCP setup experience and the public-facing product experience.

---

## 6. Innovation Opportunities (From 2026 Landscape Research)

### 6.1 AI-Powered Investigation Workflows

The 2026 AI Journalism Lab (CUNY/Microsoft) is training 24 journalists on AI-powered investigative tools. CG could be the data backbone for these workflows. **Opportunity:** Partner with AI J-Lab to make CG a standard tool in investigative journalism curricula.

### 6.2 FOIA Lead Generation

MuckRock automates FOIA filing. CG could identify when data gaps exist and suggest specific FOIA requests. **Example:** "This landlord's LLC was formed in Delaware. File this FOIA to get the beneficial ownership records." This would make CG indispensable for investigative reporters.

### 6.3 Community Verification Layer

CivicWatch and similar platforms enable crowdsourced verification. CG could add a "confirm/dispute" layer where residents verify or challenge data findings with ground-truth observations. **Example:** "CG says this building has 0 open violations. Does that match what you see?" This builds trust and catches data lag.

### 6.4 Proactive Neighborhood Briefings

No competitor produces automated, personalized neighborhood briefings. CG could generate weekly/monthly "state of your neighborhood" reports for followed ZIPs. **Content:** New violations filed, businesses opened/closed, crime trajectory, permit activity, upcoming community board items.

### 6.5 Institutional Sales (B2B2C)

Tenant organizing groups, legal aid societies, community boards, and local newsrooms are all potential institutional users. They need:
- Bulk query access (beyond individual rate limits)
- Embeddable receipt cards for their own publications
- Custom investigation templates
- API access for their own tools

This is a revenue path that doesn't conflict with the free-for-individuals model.

### 6.6 Data Storytelling Templates

Data Through Design (NYC's annual data art exhibition) shows appetite for narrative data presentation. CG could offer "investigation templates" — guided multi-step workflows that tell a story:
- "The Slumlord Exposer" — start with an address, uncover the full network
- "The Equity Auditor" — compare two ZIPs on 311 response times
- "The Gentrification Tracker" — watch displacement signals over time

---

## 7. UX Risks

### 7.1 MCP-Only Distribution Is Fragile

If Anthropic or OpenAI change MCP support, pricing, or tool discovery, CG loses its entire distribution channel. **Mitigation:** Build at least one owned channel (browser extension, standalone web app) that doesn't depend on third-party AI platforms.

### 7.2 Trust at Scale

If CG receipt cards go viral and one contains an error, the reputational damage could be severe. **Mitigation:**
- Every card must show data freshness date
- Every card must show source datasets and row counts
- Build a correction/update mechanism for cards that get shared
- Consider a "confidence level" indicator for cross-references vs. single-source findings

### 7.3 The "Data Nerd" Perception

Despite the thesis's emphasis on plain language, the current MCP tools return dense tabular data. Non-technical users in a ChatGPT conversation may still feel overwhelmed. **Mitigation:** Add narrative summaries to tool responses (the middleware pipeline could add this), not just tables.

---

## 8. Key Metrics to Track

| Metric | What It Measures | Target (6 months) |
|--------|-----------------|-------------------|
| MCP tool calls / day | Core usage | 500+ |
| Unique addresses queried / week | Breadth of engagement | 1,000+ |
| Receipt cards generated | Shareable content production | 200+ / week |
| Receipt card shares (OG link clicks) | Viral distribution | 50+ / week |
| Extension installs | Browser channel adoption | 5,000+ |
| Manifesto page scroll depth | Lander effectiveness | 60%+ reach bottom |
| `/connect` conversion rate | MCP setup completion | 15%+ |
| Donation conversion | Sustainability | 2%+ of active users |
| Repeat users (7-day return) | Retention / habit formation | 25%+ |

---

## 9. Summary: What Would Make CG Win

Three things separate Common Ground from every other civic data project:

1. **The cross-reference moat** — 294 tables in one lake, with entity resolution linking them. Nobody else has this for NYC. The lake is the moat; the crosses are the product.

2. **The "parasite strategy"** — CG doesn't ask people to come to a portal. It shows up where they already are: StreetEasy listings, Reddit arguments, group chat addresses. This is the right distribution model, but it's 0% built.

3. **The conversation layer** — The LLM-native interface that contextualizes, follows up, and helps people think. This is uncopyable by dashboard products. It already works via MCP.

**The single most important thing to build next:** Receipt cards (the shareable output format) followed by the browser extension (the highest-intent distribution channel). Everything else flows from those two.

**The competitive position is strong. The execution gap is the constraint. Ship the surfaces.**
