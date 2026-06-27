# Udemy Module Production Package

Generate a complete Udemy lecture production package for RxJS Deep Dive module **$ARGUMENTS**.

## Step 1 — Read the module content

Before generating anything, read the module source file:

| Module | File |
|--------|------|
| 0  | docs/modules/00-setup.md |
| 1  | docs/modules/01-history.md |
| 2  | docs/modules/02-observable-contract.md |
| 3  | docs/modules/03-creation.md |
| 4  | docs/modules/04-operators.md |
| 5  | docs/modules/05-flattening.md |
| 6  | docs/modules/06-time.md |
| 7  | docs/modules/07-combining.md |
| 8  | docs/modules/08-error.md |
| 9  | docs/modules/09-hot-cold.md |
| 10 | docs/modules/10-state.md |
| 11 | docs/modules/11-typescript.md |
| 12 | docs/modules/12-custom-operators.md |
| 13 | docs/modules/13-testing.md |
| 14 | docs/modules/14-framework.md |
| 15 | docs/modules/15-capstone.md |

Read the file fully. Every script, slide, and quiz question must reflect the actual content — do not invent or paraphrase beyond what the module teaches.

---

## Step 2 — Produce the full production package

Generate all six sections below in order, separated by `---`.

---

### SECTION 1 — Udemy Section Metadata

```
Section title:       [Module N — Title]
Section description: [2–3 sentences. What the student will be able to do after this section.]
Prerequisites:       [What module(s) must be completed first]
Estimated duration:  [Sum of all lecture estimates below]
Skill level:         Intermediate
```

---

### SECTION 2 — Lecture Breakdown

Split the module into individual lectures. Each lecture must be **2–7 minutes**. Complex code walkthroughs may go up to 10 minutes. Every concept section in the module file becomes at least one lecture. Aim for 5–8 lectures per module (20–40 min total).

Format each lecture as:

```
Lecture N.1 — [Title]           [X min]
Lecture N.2 — [Title]           [X min]
...
```

---

### SECTION 3 — Video Scripts

Write a full narration script for **every lecture** in the breakdown.

**Script rules (Udemy quality standards):**
- Open with a hook: "By the end of this lecture you'll be able to..."
- Second person throughout: "You'll see...", "Let's look at...", "Notice how..."
- Short sentences. Scripts are spoken, not read.
- Insert timing markers: `[0:00]`, `[0:30]`, `[1:00]` etc.
- Insert action cues: `[SLIDE 2]`, `[DEMO]`, `[TYPE]`, `[PAUSE]`, `[HIGHLIGHT line 3]`
- Code walkthroughs: narrate what you type as you type it — "I'm importing switchMap from rxjs..."
- Close each lecture: "In the next lecture we'll look at..."
- Keep each script to approximately the target lecture duration (150 words ≈ 1 minute of speech)

---

### SECTION 4 — Slide Outlines

For **every lecture**, produce a slide-by-slide outline.

**Slide rules:**
- Slide 1: Lecture title + "Module N — Course Name" subtitle
- One concept per slide — never put two ideas on one slide
- Max 6 words per bullet point
- Code slides: show a single focused snippet (max 15 visible lines), syntax-highlighted label
- Diagram slides: describe the marble diagram or ASCII diagram to draw
- Final slide: one-sentence key takeaway

Format:
```
[Lecture N.1 Slides]
Slide 1: Title — "Lecture title" / "Module N — RxJS Deep Dive"
Slide 2: [Concept] — bullet 1 / bullet 2 / bullet 3
Slide 3: [Code] — snippet label / what to highlight
...
Slide N: Takeaway — "One sentence."
```

---

### SECTION 5 — Section Quiz

Write **5 multiple-choice questions** for the module as a whole.

**Quiz rules (Udemy format):**
- One clearly correct answer per question
- Three plausible wrong answers — not obviously silly, not trick questions
- Test understanding and application, not memorization of terms
- Include a 1–2 sentence explanation shown after the learner submits

Format:
```
Q1. [Question text]
  A) [Option]
  B) [Option]
  C) [Option — correct]
  D) [Option]
Correct: C
Explanation: [Why C is correct and why the others are wrong.]

Q2. ...
```

---

### SECTION 6 — Resources & Downloads

List everything to attach to the Udemy section:

```
Code file:     src/module-N/[filename].ts   — runnable TypeScript for all examples in this module
Companion docs: https://[domain]/modules/NN-slug  — VitePress reference page
RxJS docs:     https://rxjs.dev/api          — official API reference
```

Also list any operator-specific doc links for operators introduced in this module (e.g., `https://rxjs.dev/api/operators/switchMap`).

---

## Udemy content quality checklist

Before finishing, verify:
- [ ] Every lecture is 2–10 minutes (check word count: 150 words ≈ 1 min)
- [ ] Scripts open with a learning outcome hook
- [ ] Scripts include `[DEMO]` / `[TYPE]` / `[SLIDE N]` cues
- [ ] No slide has more than 6 words per bullet
- [ ] Quiz has exactly 5 questions with explanations
- [ ] Resources section includes the companion docs URL
- [ ] All content matches the module source file — nothing invented

---

## Course context

**Course:** RxJS Deep Dive  
**Thesis:** "The domain can change. The RxJS machine stays the same."  
**Audience:** JavaScript/TypeScript developers who know async/await and want to master reactive programming  
**Companion docs:** VitePress site in `docs/` — each module has a page at `docs/modules/`  
**Stack covered:** RxJS 7, TypeScript (strict, ES2023), Vitest, Angular, React, Vue
