# Civic Infrastructure Impact Review — Common Ground NYC Platform Expansion

> **Date**: 2026-04-03
> **Reviewer**: Civic Technology Advisor
> **Documents reviewed**: cg-platform-roadmap.md, cg-telegram-bot-architecture.md, cg-notification-engine-design.md

---

## 1. CIVIC POWER — WHAT THIS ENABLES

### 1.1 The Asymmetry This Addresses

New York City publishes an extraordinary amount of open data — 294 tables, 60M+ rows covering housing violations, building permits, campaign finance, court records, police misconduct, and more. But this data is scattered across dozens of portals, requires technical skill to query, and is practically invisible to the people it most affects.

The result is a structural information asymmetry: **landlords, developers, and their attorneys have staff to monitor this data. Tenants, community boards, and journalists do not.** Common Ground's notification engine directly attacks this asymmetry by turning passive data archives into active surveillance of power.

### 1.2 What Changes When Buildings Get Watched

The `address_watch` subscription type is the most civically powerful feature in this platform. When tenants can say "tell me when a new violation is filed at my building" and receive a Telegram alert within hours, several things shift:

- **Evidence accumulation becomes automatic.** Tenants in housing court currently scramble to find violation histories. With CG, they have a timestamped record of every violation filed during their tenancy, delivered to their phone as it happens.
- **Organizing across portfolios becomes possible.** The `entity_watch` + `network(type="ownership")` combination lets tenants discover that their building's problems are part of a pattern across a landlord's entire portfolio. This is exactly what JustFix.nyc's Who Owns What enabled — Brooklyn Legal Services reports using it "every single day" during tenant intakes. CG goes further by making this *proactive* rather than reactive.
- **HP actions get stronger.** Housing Part cases in NYC rely on documenting a pattern of neglect. Automated violation tracking builds that pattern without requiring tenants to manually check HPD's website.

**Precedent**: The Displacement Alert Project (DAP) Map showed that when you connect eviction filings, code violations, vacate orders, and construction permits into a single view, displacement patterns become visible that no single dataset reveals. CG's change detection engine, with its ability to watch across 287 delta-merge tables simultaneously, is the real-time version of this.

### 1.3 What Changes When Campaign Money Gets Watched

The `network(type="political")` tool combined with `civic(view="contracts")` creates a donation-to-contract pipeline tracker. This is the kind of tool investigative journalists at THE CITY, City Limits, and ProPublica spend weeks building manually for individual stories.

With CG's subscription engine, a journalist (or any citizen) could set up:
- `entity_watch` on a developer's name
- `table_watch` on campaign contribution tables
- `zip_category` on DOB permits in a specific neighborhood

And receive alerts whenever those entities appear in new filings. This turns investigative journalism from a one-off project into a continuous monitoring operation.

### 1.4 What Changes When Neighborhoods Get Forums

Per-ZIP Telegram Forum Topics create something that doesn't really exist in NYC civic life: **a data-grounded neighborhood conversation space.** Community Board meetings happen monthly and are attended by a self-selecting group. 311 is one-directional. Nextdoor is anecdote-driven.

A ZIP-code forum where the bot posts contextualized data changes ("HPD violations in 11201 are up 23% this quarter compared to last year") creates a shared factual foundation for neighborhood discussion. This is closer to what CompStat did for police accountability — not the data itself, but the *regular public review* of the data that changed behavior.

### 1.5 The "Bloomberg Terminal for Civic Engagement" Thesis

This comparison has real substance. Bloomberg Terminal's power isn't just data — it's that professionals *live in it* throughout their day, receiving alerts, querying on demand, and discussing with peers. CG's four-surface approach (Telegram bot, CLI, website dashboard, MCP for AI agents) mirrors this:

- Telegram = mobile alerts + community (like Bloomberg's chat)
- CLI = power-user query interface (like Bloomberg's terminal commands)
- SQLRooms dashboard = visual exploration (like Bloomberg's charting)
- MCP = programmatic access for AI-powered investigation

If executed well, this could become the default way informed New Yorkers interact with their city's data — not as a tool you visit, but as an ambient layer of civic awareness.

---

## 2. ETHICAL RISKS

### 2.1 Surveillance Reversal: Landlords Watching Tenants

**Risk level: HIGH**

The same `entity_watch` that lets tenants track a landlord's violations lets a landlord track a tenant organizer's name across public records. A landlord could:
- Watch for tenant association incorporation filings
- Monitor 311 complaint patterns to identify which tenants are filing
- Track Housing Court case filings against them to identify litigious tenants

This is not hypothetical. Research from the Anti-Eviction Mapping Project documents how NYC landlords already deploy surveillance technologies — facial recognition, CCTV, biometric access — specifically to catch tenants in lease violations and accelerate displacement. CG would add *data surveillance* to this toolkit.

**Mitigation required**: Rate-limit `entity_watch` subscriptions per user. Flag patterns that suggest employer/landlord monitoring (watching many individuals at the same address). Consider whether individual names should be watchable at all, or only entities with a public portfolio (LLCs, corporations, registered agents).

### 2.2 Harassment Vector: Stalking via Public Records

**Risk level: MEDIUM-HIGH**

A determined harasser could use CG to aggregate a person's public footprint: property records, court cases, campaign donations, business filings, marriage records, death index entries. Each dataset is individually public; the aggregation is what creates danger.

The `entity_xray` pattern (which already exists in the MCP server) combined with real-time alerts means someone could be notified the moment their target appears in any new public filing.

**Mitigation required**:
- Implement a "watched entities" audit log that is reviewable
- Rate-limit the number of distinct individual names a single user can watch (e.g., max 10 entity_watch subscriptions)
- Consider an opt-out registry where individuals can request their name be excluded from entity_watch alerts (not from the underlying data, which is public record)
- Display a clear terms-of-use statement that monitoring individuals for harassment purposes is prohibited

### 2.3 Gentrification Accelerant

**Risk level: MEDIUM**

Real estate investors already use proprietary data tools to identify "opportunity" neighborhoods — areas with rising 311 complaints (indicating changing demographics), new DOB permits (development activity), and falling crime (gentrification signal). CG's `zip_category` subscriptions + dashboard aggregate Parquets essentially democratize this same analysis.

The counterargument: this data is already available to anyone with technical skill. CG makes it equally accessible to *both* community defenders and speculative investors. The question is whether the community benefit outweighs the speculative risk.

**Mitigation**: Frame data in terms of community impact, not investment opportunity. The `agg_zip_overview.parquet` should include displacement indicators (rent-stabilized unit loss, eviction filings) alongside development metrics. Context matters — "47 new DOB permits" reads differently next to "312 eviction filings in the same ZIP."

### 2.4 Digital Divide: Who Gets Excluded

**Risk level: HIGH**

Telegram has ~27 million US users (8% of population), skewing male, under 34, and tech-savvy. It is popular with activists, journalists, and crypto communities — not with elderly tenants in NYCHA housing, non-English speakers in immigrant communities, or people whose primary communication channel is SMS or WhatsApp.

The populations most affected by housing violations, police misconduct, and environmental hazards are the least likely to use Telegram. Specifically:
- **Older adults**: Only ~15% of Telegram users are over 45
- **Low-income households**: May have limited smartphones or data plans
- **Non-English speakers**: Telegram's interface and CG's alerts would be in English
- **Communities of color**: The NYC Council's 2024 Oversight Hearing on 311 found that communities of color report issues at lower rates — a pattern CG could amplify rather than correct

**The SMS question**: The roadmap mentions email digests and RSS feeds as alternative channels (Phase 7), but not SMS. In NYC's most vulnerable communities, SMS reach far exceeds any app-based platform. WhatsApp is dominant in many immigrant communities (Dominican, Chinese, South Asian). Telegram is a niche choice.

**Mitigation required**:
- Add SMS as a delivery channel (Twilio, ~$0.0075/message — trivial cost at projected scale)
- Add WhatsApp Business API as a channel (critical for immigrant communities)
- Support Spanish, Chinese (Simplified), and Haitian Creole at minimum — NYC's three most common non-English languages
- Partner with community organizations that already have trust and communication channels (see Section 5)

### 2.5 Misinformation Through Decontextualization

**Risk level: MEDIUM**

Raw data without context misleads. Examples:
- "Building has 100 HPD violations" sounds catastrophic — but many buildings accumulate violations over decades, and most get resolved. The crucial number is *open* violations.
- "Owner has 500 violations across portfolio" — but what if they own 200 buildings? That's 2.5 per building, which is below average.
- A spike in 311 complaints might reflect a community organizing campaign (good!) rather than deteriorating conditions (bad!).

The notification payload design (Section 4.2 of the notification engine) includes `batch_summary` ("3 new violations since last check") but no contextual framing. The Phase 4 roadmap mentions "Contextual framing: '23% this quarter' not just raw counts" — this must be a hard requirement, not a nice-to-have.

**Mitigation required**:
- Every violation notification must include resolution status
- Every count must include a comparison baseline (portfolio average, ZIP average, city average)
- The dashboard's `agg_building_summary.parquet` already includes `open_violations` vs `violation_count` — ensure alerts make this distinction too
- Add a "What does this mean?" expandable section to notification cards

### 2.6 Community Forum Toxicity

**Risk level: MEDIUM**

Per-ZIP Forum Topics could become:
- NIMBY organizing spaces ("no more shelters in our ZIP!")
- Racist complaint amplifiers (311 "noise" complaints weaponized against communities of color)
- Landlord-bashing without factual basis
- Political campaign spaces during election season

Telegram's moderation tools are limited compared to Discord or purpose-built forum software. The bot is the only moderation mechanism, and the roadmap doesn't address content policy.

**Mitigation required**:
- Establish clear community guidelines before launching any forum topic
- Bot should only respond to @mentions with data, not engage in opinion
- Consider having bot-posted data updates be the *only* posts in certain topics, with discussion in linked threads
- Appoint community moderators per-ZIP (partner with existing community board members)
- Auto-flag messages that name specific individuals in a negative context

---

## 3. GOVERNANCE QUESTIONS

### 3.1 Who Decides What Gets an Alert?

The notification engine design currently treats all data changes equally — a new HPD violation and a new restaurant inspection result trigger the same pipeline. But *framing* is power. Decisions that need explicit governance:

- Which tables are available for `table_watch`? (Should police misconduct data generate the same alerts as restaurant grades?)
- What constitutes "contextual framing" and who writes the templates?
- Which subscription types are available by default vs. opt-in?
- What happens when the city asks CG to stop alerting on a specific dataset?

**Recommendation**: Create a public "Data Surfacing Policy" document that explains what data is watchable, how alerts are framed, and what the editorial principles are. This is CG's equivalent of a newspaper's editorial standards.

### 3.2 Data Correction and Right of Reply

Public records contain errors. Names are misspelled. Violations are attributed to wrong buildings. Court records reflect allegations, not convictions.

If someone's name appears in an `entity_watch` alert and the underlying data is wrong, CG has:
- No correction mechanism (data comes from city sources)
- No "right of reply" (the person named has no way to add context)
- No takedown process (what if someone demands removal?)

**Recommendation**:
- Add a "Report data issue" link to every notification that directs to the originating agency's correction process
- Include a disclaimer: "This data comes from [Agency]. Errors should be reported to [Agency contact]."
- Establish a clear policy for legal takedown requests: CG surfaces public data, doesn't create it. Link to the source agency for corrections.

### 3.3 Community Discussion Ownership

Who owns the conversations in per-ZIP Forum Topics? What's the content policy? What happens if:
- A community organizer shares sensitive information about a planned action?
- A landlord's attorney uses forum posts as evidence in an eviction proceeding?
- Police use forum discussions to identify protest organizers?

**Recommendation**: CG must have a written privacy policy and terms of service *before* launching community forums. Consider:
- Making forums read-only for non-members (prevent scraping)
- Allowing pseudonymous participation
- Clear statement that CG will not voluntarily share user data or forum content with law enforcement without a court order

---

## 4. WHAT CIVIC TECH GETS WRONG (AND HOW CG CAN AVOID IT)

### 4.1 The Graveyard Problem

Most civic tech projects die. The pattern is well-documented:
1. Philanthropic donors give small, time-bound grants for prototypes
2. Prototypes launch to media coverage
3. Grant ends, maintenance stops
4. Tool quietly disappears

CG's cost projections are remarkably lean ($263/month at 10K users) because infrastructure is already running on Hetzner. But the real cost isn't servers — it's:
- Ongoing data pipeline maintenance (APIs change, schemas break, datasets get deprecated)
- Community management (forum moderation, user support)
- Feature development (responding to user needs)
- Legal defense (inevitable FOIA disputes, takedown requests)

**CG's advantage**: It's built on an existing data pipeline that's maintained for other purposes (the MCP server, AI investigation tools). The marginal cost of the notification layer is genuinely low. But if the maintainer (single developer?) stops working on it, everything stops.

**Recommendation**: Document the bus factor. If this is a one-person project, what's the succession plan? Consider:
- Open-sourcing the pipeline and notification engine
- Partnering with BetaNYC (NYC's civic tech community, 9,991 participants in 2025) for community maintenance
- Applying for Knight Foundation or NSF CIVIC funding — not for development, but for sustainability

### 4.2 The "Already-Empowered" Trap

JustFix.nyc's research shows that their tools are used "every single day" by legal aid attorneys — educated professionals who already advocate for tenants. That's valuable, but it's not the same as tenants themselves using the tools.

CG risks the same pattern: journalists, lawyers, and tech-savvy organizers adopt it quickly. The tenants who actually live with violations, breathe the bad air, and face eviction never hear about it.

**Recommendation**: The first 100 users should NOT be found on Telegram. They should be found through:
- Legal Aid Society intake offices (tenant already in crisis, hand them the tool)
- Met Council on Housing hotline (tenant calling about a problem, offer ongoing monitoring)
- NYCHA tenant association meetings (organized tenants, ready for data tools)
- Community board meetings (already civically engaged, need better data)
- Houses of worship in high-violation ZIPs (trusted institutions)

Then work backwards from what those users actually need. They may not want Telegram at all.

### 4.3 "Build It and They Will Come" Doesn't Work

Civic tech adoption requires active distribution, not just availability. The most successful civic tech tools in NYC have a distribution partner:

| Tool | Distribution Partner | How Users Find It |
|------|---------------------|-------------------|
| JustFix Who Owns What | Legal aid attorneys, housing organizers | During intake, during organizing meetings |
| Heat Seek | Housing court judges, legal aid | Judge orders temperature monitoring |
| NYC 311 | City government marketing, 311 app | Universal awareness campaign |
| Citizen (safety) | Viral word-of-mouth, push notifications | Fear of crime + social proof |

CG needs distribution partners, not just a good product. The Telegram bot won't go viral organically.

**Recommendation**: Before building the bot, identify 3-5 organizational partners who would distribute CG to their existing constituencies. Each partner should commit to:
- Introducing CG during their existing touchpoints with tenants/citizens
- Providing feedback on what alerts and features their users actually need
- Co-designing the onboarding flow for their specific audience

### 4.4 Equity Amplification Risk

The NYC Council's 2024 311 Oversight Hearing revealed that communities of color file 311 complaints at lower rates than white communities — not because they have fewer problems, but because of distrust, language barriers, and learned helplessness about government responsiveness.

If CG's notification engine treats 311 complaint volume as a signal (via `zip_category` subscriptions), it will amplify this existing inequity: over-alerted neighborhoods with vocal complainants, under-alerted neighborhoods with systemic problems. The same applies to HPD violations (which require inspections that tenants must request) and DOB complaints (which require knowledge of the complaint process).

**Recommendation**:
- Use violation and complaint data as *part* of a picture, never as the whole picture
- The dashboard should include demographic overlays (census data is already in the lake) so users can see where complaint rates diverge from conditions
- Consider proactive outreach to low-complaint ZIPs: "Your neighborhood has few 311 complaints but high environmental risk scores — here's how to file"

---

## 5. WHAT THIS COULD BECOME

### 5.1 Five-Year Vision (If It Works)

**Year 1**: NYC housing focus. 1,000 active users, primarily legal aid attorneys, housing organizers, and investigative journalists. Proves the model: real-time data alerts lead to faster HP actions, better-sourced journalism, and more informed community boards.

**Year 2**: Expand to all NYC domains. Campaign finance tracking for election cycles. School performance monitoring for parents. Environmental hazard alerts for affected communities. 10,000 users. First organizational partnerships (Legal Aid Society, Met Council on Housing).

**Year 3**: Replicate to other cities. Chicago, Philadelphia, and LA all have Socrata open data portals with similar datasets. The pipeline architecture (Dagster + dlt + DuckLake) is city-agnostic — only the source configuration changes. ProPublica or a journalism school becomes the multi-city operator.

**Year 4**: Government adoption. City agencies use CG internally for cross-agency awareness. Community boards get official CG dashboards. 311 modernization incorporates CG's notification engine. The city pays for it.

**Year 5**: The model becomes standard civic infrastructure, like 311 is today. Every city with open data has a CG-like layer that turns passive archives into active community intelligence.

### 5.2 Critical Partnerships

| Partner | What They Bring | What CG Gives Them |
|---------|-----------------|-------------------|
| **Legal Aid Society** | 50,000+ tenant interactions/year, legal expertise | Automated evidence gathering, portfolio analysis |
| **Met Council on Housing** | Hotline, tenant organizing network | Real-time violation tracking for their members |
| **ProPublica / THE CITY / City Limits** | Journalism distribution, credibility | Automated tipsheet, continuous monitoring |
| **BetaNYC** | 9,991 civic tech participants, technical volunteers | A production platform to contribute to |
| **NYC Comptroller's Office** | Official data, policy influence | Better landlord accountability tools |
| **CUNY / Columbia Data Science** | Student researchers, academic credibility | Real-world civic tech research platform |

### 5.3 What If Government Wants In?

If city government wanted to use CG, it would likely be for:
- **Community board data access**: Every CB should have a CG dashboard for their district
- **Cross-agency awareness**: HPD knowing about DOB permits in the same building they're investigating
- **311 modernization**: CG's notification engine is what 311 should have been — proactive, not reactive
- **Participatory budgeting**: Data-informed community spending decisions

The risk: government adoption could compromise CG's independence. If the city funds it, can CG still surface data that embarrasses the city?

**Recommendation**: Maintain editorial independence through organizational structure (nonprofit, not government contractor). Accept government data partnerships but not editorial control.

---

## 6. RECOMMENDATIONS

### 6.1 Features to Prioritize for Civic Impact

1. **`address_watch` + Housing Court integration** — This is the killer feature for tenants. Make it work perfectly before anything else.
2. **SMS delivery channel** — Reach the people who need this most, not just the people easiest to reach.
3. **Contextual framing in every alert** — "3 new Class C violations (vermin), 47 total open violations, worst 2% of buildings in ZIP 11221." Never raw numbers alone.
4. **Spanish language support** — 1.9 million Spanish speakers in NYC. Non-negotiable for equity.
5. **"Portfolio view"** — When alerting on a building, always show the owner's full portfolio health. Single-building alerts miss the pattern.

### 6.2 Safeguards to Build Before Launch

1. **Anti-stalking protections**: Cap entity_watch at 10 individuals per user. Flag suspicious patterns. Audit log for all subscriptions.
2. **Data correction links**: Every alert must link to the source agency's correction process.
3. **Privacy policy and ToS**: Written, reviewed by a housing attorney, published before forums launch.
4. **Community guidelines**: Clear rules for forum conduct, moderation process, and appeal mechanism.
5. **Contextual framing engine**: No alert goes out without comparison baselines and resolution status.
6. **Terms prohibiting commercial real estate prospecting** (won't stop determined actors, but establishes norms and enables account termination).

### 6.3 User Research Before Building

1. **Interview 20 tenants** who have been through HP actions — what information did they wish they had, and when?
2. **Interview 10 legal aid attorneys** — what do they look up manually that CG could automate?
3. **Interview 5 investigative journalists** — what monitoring do they do by hand?
4. **Interview 5 community board members** — what data do they need for monthly meetings?
5. **Test the Telegram assumption** — ask these 40 people what messaging apps they actually use daily.

### 6.4 First 100 Users — How to Reach Them

| Cohort | Size | Channel | Hook |
|--------|------|---------|------|
| Legal aid attorneys (BLS, Legal Aid Society, Mobilization for Justice) | 20 | Email + in-person demo at their offices | "Never manually check HPD again" |
| Housing organizers (Met Council, CASA, local tenant unions) | 20 | Organizer network, BetaNYC events | "Know about violations before your landlord does" |
| Investigative journalists (THE CITY, City Limits, Gothamist) | 10 | Direct outreach, journalism conferences | "Automated tipsheet for your beat" |
| Community board members (start with 3 high-violation CBs) | 15 | CB meeting presentations | "Data dashboard for your next meeting" |
| Tenants in active HP cases (via legal aid partners) | 25 | Legal aid attorney introduces during intake | "Track your building's violations on your phone" |
| Civic tech community (BetaNYC) | 10 | BetaNYC Slack, meetup presentations | "Contribute to the platform" |

---

## 7. SUMMARY ASSESSMENT

### The Promise

Common Ground has the potential to be the most significant civic technology platform in NYC since 311. It addresses a real information asymmetry, uses a technically sound architecture, and operates at a cost that doesn't depend on grants to survive. The notification engine — the ability to turn 294 static datasets into a living alert system — is genuinely novel. No existing tool does this.

### The Danger

The same infrastructure that empowers tenants can empower landlords, harassers, and speculators. The Telegram-first strategy risks serving the already-connected rather than the most vulnerable. Launching without contextual framing, anti-stalking protections, and community guidelines would be irresponsible. And like most civic tech, the biggest risk isn't technical failure — it's building something nobody actually uses because the distribution problem was never solved.

### The Path

Build `address_watch` first. Partner with Legal Aid before writing a line of bot code. Add SMS before adding Forum Topics. Frame every number in context. And remember that the goal is not a cool data platform — it's whether a tenant in East New York with black mold in their apartment gets better information, faster, than they would have without this tool. If that tenant can't or won't use Telegram, the platform has failed its mission regardless of how elegant the architecture is.

---

## Sources

Research informing this review:

- [BetaNYC 2025 End of Year Report](https://www.beta.nyc/2026/02/13/2025-eoy-report/) — 9,991 participants in 74 civic tech events
- [Housing Data Coalition](https://www.housingdatanyc.org/) — NYC housing data advocacy
- [JustFix.nyc / Who Owns What](https://whoownswhat.justfix.org/en/) — property ownership transparency tool
- [Shelterforce: How Hidden Property Owners Are Revealed](https://shelterforce.org/2022/03/24/how-hidden-property-owners-and-bad-landlord-patterns-are-revealed-in-nyc/) — JustFix impact
- [Shelterforce: Tech Tools Help Tenants Push Back](https://shelterforce.org/2025/06/17/tech-tools-help-tenants-push-back-against-problematic-landlords/) — tenant tech tools
- [Anti-Eviction Lab: Landlord Technologies of Gentrification](https://www.antievictionlab.org/ny-report) — landlord surveillance in NYC
- [McElroy & Vergerio: Automating Gentrification (2022)](https://journals.sagepub.com/doi/abs/10.1177/02637758221088868) — academic research on landlord tech
- [ACM Interactions: What Does Failure Mean in Civic Tech?](https://interactions.acm.org/archive/view/march-april-2024/what-does-failure-mean-in-civic-tech-we-need-continued-conversations-about-discontinuation) — civic tech discontinuation
- [Knight Foundation: Sustainable Future for Civic Tech](https://knightfoundation.org/articles/building-a-sustainable-future-for-civic-tech/) — sustainability models
- [Giving Compass: Sustaining Civic Tech Beyond Funding](https://givingcompass.org/article/sustaining-civic-technology-beyond-the-funding-cycle) — grant dependency pattern
- [Telegram Statistics 2026](https://www.demandsage.com/telegram-statistics/) — 27M US users, skewing young/male/tech-savvy
- [NYC Comptroller: Accountability & Transparency](https://comptroller.nyc.gov/reports/a-new-charter-to-confront-new-challenges/accountability-transparency/) — government transparency tools
- [Georgetown Law Tech Review: JustFix and 21st Century Tenant Movement](https://georgetownlawtechreview.org/wp-content/uploads/2022/06/4.1-Clement_Housing_Formatted.pdf) — legal analysis
- [NYCEDC: Civic Tech Competition for Tenant Protection](https://edc.nyc/press-release/new-york-city-announces-winners-civic-tech-competition-strengthen-tenant-protection) — NYC civic tech investment
