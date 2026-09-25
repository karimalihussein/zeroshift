---
name: ZeroShift
description: "Database migration, made visible." A distributed-systems control plane for a local educational lab.
colors:
  control-blue: "#356ae6"
  control-blue-deep: "#2555c2"
  control-blue-soft: "#eaf0ff"
  done-green: "#16835b"
  done-green-soft: "#eaf7f1"
  retry-amber: "#b66a12"
  retry-amber-soft: "#fff5e6"
  compensated-violet: "#6f4bc2"
  compensated-violet-soft: "#f1ecfb"
  failed-red: "#c3404d"
  failed-red-soft: "#fff0f2"
  idle-slate: "#aab3c2"
  ink: "#172033"
  button-ink: "#26334a"
  muted: "#647188"
  quiet: "#8994a7"
  chip-ink: "#3c4960"
  line: "#dde3ec"
  line-strong: "#c9d2df"
  canvas: "#f6f8fb"
  paper: "#ffffff"
  panel-tint: "#fbfcfe"
  code-night: "#0f1a2c"
typography:
  display:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "clamp(28px, 3vw, 42px)"
    fontWeight: 650
    lineHeight: 1
    letterSpacing: "-0.045em"
  headline:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "20px"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "-0.02em"
  title:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "13px"
    fontWeight: 700
    lineHeight: 1.25
    letterSpacing: "-0.01em"
  stat:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "22px"
    fontWeight: 650
    lineHeight: 1.1
    letterSpacing: "-0.02em"
  section:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "18px"
    fontWeight: 700
    lineHeight: 1.2
    letterSpacing: "-0.02em"
  body:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "13px"
    fontWeight: 400
    lineHeight: 1.5
  small:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "12px"
    fontWeight: 400
    lineHeight: 1.45
  label:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "11px"
    fontWeight: 650
    lineHeight: 1.35
  caption:
    fontFamily: "Aptos, Segoe UI Variable, SF Pro Display, Inter, sans-serif"
    fontSize: "10px"
    fontWeight: 650
    lineHeight: 1.3
  mono:
    fontFamily: "SFMono-Regular, Cascadia Code, Roboto Mono, monospace"
    fontSize: "11px"
    fontWeight: 400
    lineHeight: 1.55
rounded:
  tag: "4px"
  chip: "6px"
  control: "7px"
  tile: "10px"
  card: "12px"
  panel: "16px"
  pill: "999px"
spacing:
  xs: "6px"
  sm: "10px"
  md: "16px"
  lg: "20px"
  xl: "24px"
components:
  button:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.button-ink}"
    rounded: "{rounded.control}"
    padding: "8px 12px"
    height: "38px"
  button-primary:
    backgroundColor: "{colors.control-blue}"
    textColor: "{colors.paper}"
    rounded: "{rounded.control}"
    padding: "8px 18px"
    height: "40px"
  button-primary-hover:
    backgroundColor: "{colors.control-blue-deep}"
  button-armed:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
    rounded: "{rounded.control}"
  button-danger:
    backgroundColor: "{colors.failed-red-soft}"
    textColor: "{colors.failed-red}"
    rounded: "{rounded.control}"
  input:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "{rounded.control}"
    padding: "0 11px"
    height: "38px"
  chip:
    backgroundColor: "#eef1f6"
    textColor: "#3c4960"
    typography: "{typography.label}"
    rounded: "{rounded.chip}"
    padding: "2px 8px"
  chip-done:
    backgroundColor: "{colors.done-green-soft}"
    textColor: "{colors.done-green}"
  chip-waiting:
    backgroundColor: "{colors.control-blue-soft}"
    textColor: "{colors.control-blue-deep}"
  chip-retrying:
    backgroundColor: "{colors.retry-amber-soft}"
    textColor: "{colors.retry-amber}"
  chip-compensated:
    backgroundColor: "{colors.compensated-violet-soft}"
    textColor: "{colors.compensated-violet}"
  chip-failed:
    backgroundColor: "{colors.failed-red-soft}"
    textColor: "{colors.failed-red}"
  status-pill:
    backgroundColor: "#f1f4f8"
    textColor: "{colors.muted}"
    rounded: "{rounded.pill}"
    padding: "6px 11px"
  lab-nav-active:
    backgroundColor: "{colors.ink}"
    textColor: "{colors.paper}"
    rounded: "{rounded.control}"
    padding: "7px 12px"
  panel:
    backgroundColor: "{colors.paper}"
    rounded: "{rounded.panel}"
    padding: "24px"
  card:
    backgroundColor: "{colors.panel-tint}"
    rounded: "{rounded.card}"
    padding: "14px"
  graph-node:
    backgroundColor: "{colors.paper}"
    textColor: "{colors.ink}"
    rounded: "11px"
    padding: "10px 11px 10px 12px"
  code-block:
    backgroundColor: "{colors.code-night}"
    textColor: "#d8e1f0"
    typography: "{typography.mono}"
    rounded: "{rounded.tile}"
    padding: "12px 14px"
---

# Design System: ZeroShift

## Overview

**Creative North Star: "The Control Plane"**

ZeroShift is an operator's instrument panel for systems you can watch think. A cool grey canvas holds white panels drawn with hairline borders; everything inside them is either a real observation from the running system or a control that changes it. The two labs (Migration at `/`, Event-driven at `/events`) share one top bar, one lab switcher, one button and input vocabulary, and one set of state colours. Density is high but calm: small, confident type, tabular numerals, and colour spent only where state lives.

Colour is semantic, not decorative. Blue means selected, active, or waiting on something; green means done; amber means retrying or reversing; violet means compensated; red means failed; a cool slate means not yet reached. The same five-plus-one vocabulary paints rail stops, graph-node dots, step pips, timeline dots, chips and legend swatches, so a viewer learns it once and reads it everywhere, including on a shared screen at a distance.

The form language is process made spatial: a stage track or rail of circular stops joined by 2px connectors, service lanes with nodes linked by causal curves, a sticky inspector that explains the selected thing. The system refuses the stacked dashboard of equal, interchangeable panels; each panel has one job in the story.

**Key Characteristics:**
- Cool grey canvas, white hairline-bordered panels, ink text; one ambient shadow for the primary panel only.
- A single state vocabulary (done, waiting, retrying, compensated, failed, idle) reused across every component.
- System sans for everything; monospace only for identifiers, topics, offsets, payloads and log lines.
- Circles for state, rounded rectangles for containers, 999px pills for status.
- Tabular numerals wherever a number can change.
- Motion only reports state that the backend has actually reached.

## Colors

A restrained cool-neutral field with one blue control accent and a strict semantic state set; hue on screen is always information.

### Primary
- **Control Blue** (`control-blue`): primary action fill, selected graph node border, active tab underline, waiting state (rail stop ring, node dot, dashed node border), migration track fill and data packets, focus ring at 25% alpha.
- **Control Blue Deep** (`control-blue-deep`): hover for primary buttons, link and link-button text, waiting-chip and running-pill text, log operation labels.
- **Control Blue Wash** (`control-blue-soft`): waiting stop fill, selected table row, running pill and waiting chip background.

### Secondary (state set)
- **Done Green** / **Done Green Wash**: completed stops, node dots, pips and rail connectors; success pills and chips; completion panel; live indicator dots.
- **Retry Amber** / **Retry Amber Wash**: retrying stops and nodes, lagging consumer rows, paused log stream, rollback (reverse sync) panel and packets, changed-row highlight.
- **Compensated Violet** / **Compensated Violet Wash**: saga compensation only: stops, node dots, rail connectors, chips. Compensation edges use a lighter violet dashed stroke.
- **Failed Red** / **Failed Red Wash**: failed stops and nodes (node border at 45% alpha, faint `#fffafa` fill), danger buttons, error messages, dead-letter topics, down cards.
- **Idle Slate** (`idle-slate`): not-yet-reached stops, dots and legend swatch.

### Neutral
- **Ink** (`ink`): body text, headings, active lab-nav pill, armed send button.
- **Button Ink** (`button-ink`): default button text, one step softer than ink.
- **Muted** (`muted`): secondary text, field labels, lane heads, table headers.
- **Quiet** (`quiet`): tertiary text, timestamps, disabled-looking meta, icon tint.
- **Line** / **Line Strong**: panel and divider hairlines / input and button strokes.
- **Canvas** (`canvas`): page background. **Paper** (`paper`): panels and nodes. **Panel Tint** (`panel-tint`): inset cards, JSON editor, log panel.
- **Code Night** (`code-night`): the one dark surface, for raw payload blocks in the inspector.

### Named Rules
**The Colour Is State Rule.** Green, amber, violet and red appear only to report a state of that name. Never use them as decoration or to differentiate categories.

**The One Vocabulary Rule.** A new component that shows state takes the same six states and the same colours: solid fill for the stop or dot, the wash plus the full colour for chips and pills. Do not add a seventh state colour without a seventh backend state.

**The Blue Waits Rule.** Blue marks what is selected, active, or waiting. Waiting is drawn as outline plus wash (dashed border on nodes, a halo on rail stops), never as a solid fill, so it cannot be mistaken for done.

## Typography

**Display / Body Font:** Aptos (with Segoe UI Variable, SF Pro Display, Inter, sans-serif)
**Mono Font:** SFMono-Regular (with Cascadia Code, Roboto Mono, monospace)

**Character:** A plain, well-hinted system sans set small and tight, with semi-bold 650 as the working weight. Mono is a signal: it marks a value that came from the machine.

### Hierarchy
- **Display** (650, clamp(28px, 3vw, 42px), 1, -0.045em): the single page h1. The event lab sets it at 28px (-0.035em), 22px on mobile.
- **Headline** (650-700, 17-20px, 1.2, -0.02em): panel titles (composer, inspector head at 18px, migration sections at 20px).
- **Title** (700, 13px, 1.25, -0.01em): graph node type, inspector section heads, card titles.
- **Body** (400, 12-13px, 1.45-1.5): notes, legends, table cells; explanatory prose capped at 62-92ch.
- **Label** (650, 11-12px): field labels, lane heads, chips, pills, button text (12-13px).
- **Mono** (400-600, 11-12px, 1.55): ids, topics, partition@offset, timestamps, JSON, log rows.
- **Big numbers** (600-700, 22-36px, -0.03 to -0.045em, tabular): stats, percentages, key metrics.

### Named Rules
**The Machine Voice Rule.** Mono is only for text a machine emitted: ids, topics, offsets, headers, payloads, log lines, timestamps. Labels and prose stay in sans.

**The Tabular Rule.** Any number that updates live uses `font-variant-numeric: tabular-nums`.

## Layout

A centred shell, `min(1440px, 100% - 48px)`, under an 82px top bar (brand left, lab nav centre, connection and environment pills right). Sections stack with 16-20px gaps. The event lab reads top to bottom as the story: a 10-stop flow rail, a composer strip, then a workbench of causation graph (fluid) beside a 400px sticky inspector, then tabbed system views. The graph lays out five service lanes (min 150px each, 820px minimum, horizontal scroll) on faint `#f8fafc` lane backgrounds with sticky lane heads.

Spacing rhythm runs 6 / 10 / 16 / 20 / 24px; panel padding is 24px (18-20px in the event lab's denser panels), cards 14px, nodes 10-12px.

Responsive: at 1240px the inspector narrows to 340px; at 1080px the workbench stacks and the rail scrolls horizontally at 108px per stop; at 760px the rail wraps to two rows of five without connectors, lanes collapse into one column with each node labelled by its lane and its cause, and the tab strip fades at its right edge. The migration lab steps down at 1120 / 880 / 620px. Reduced motion disables all animation and transition.

## Elevation & Depth

Mostly flat, with tonal layering (canvas, paper, panel tint, lane background) and hairline borders doing the separating. One ambient shadow lifts the page's lead panel; interactive nodes gain a small lift only on hover or selection.

### Shadow Vocabulary
- **Ambient panel** (`box-shadow: 0 16px 45px rgba(28,45,74,.07), 0 2px 8px rgba(28,45,74,.04)`): the lead panel of a page only (migration hero, flow rail).
- **Node rest** (`box-shadow: 0 1px 2px rgba(28,45,74,.05)`): graph nodes at rest.
- **Node lift** (`box-shadow: 0 8px 20px rgba(28,45,74,.09)` with `translateY(-1px)`): graph node and rail stop hover.
- **Selection ring** (`box-shadow: 0 0 0 3px rgba(53,106,230,.16), 0 8px 20px rgba(28,45,74,.08)`): selected node; rail stops use a 3px blue outline at 28%.
- **Status glow** (`box-shadow: 0 0 0 3px rgba(22,131,91,.12)`): live/connection dots, tinted to their state.

### Named Rules
**The One Lifted Panel Rule.** Only the page's lead panel carries the ambient shadow; every other panel is border-only on paper.

## Shapes

Circles mean state (rail stops 32px, node dots 20px, timeline dots 12px, legend and pip dots 8-9px). Rounded rectangles mean containers, stepping up with scale: 4px tags, 6px chips, 7px controls, 10px tiles and code, 11-12px nodes and cards, 15-18px panels. Pills (999px) are reserved for status and connection indicators. Connectors are 2px rounded lines; progress is a 4-7px rounded bar. Causal edges are 1.6px curves in a cool grey, 2.2px blue when hot, dashed violet for compensation, dotted pale grey toward the read model.

## Components

### Buttons
Quiet, compact, bordered.
- **Shape:** gently rounded (7px), 38px min height (40px for composer actions, 30px small).
- **Default:** white fill, `line-strong` border, button ink text, 12px/650, 7px icon gap. Hover shifts to `#f3f6fa` with `#aeb9c9` border; active nudges down 1px; disabled drops to 38% opacity.
- **Primary:** Control Blue fill and border, white text; hover to Control Blue Deep. One per surface (Start migration, Send order).
- **Armed:** ink fill when the primary action will trigger a failure scenario.
- **Danger:** red text on red wash with a pale red border; **danger-soft** is red text only.
- **Link / Quiet / Copy:** borderless, transparent; link text in blue deep, underlined on hover.
- **Focus:** 3px outline at `rgba(53,106,230,.25)`, 2px offset, on all controls.

### Chips and Pills
- **Chip:** 6px radius, 11px/650, neutral `#eef1f6` with `#3c4960` text by default; state variants use wash plus full colour. 10px inside nodes.
- **Status pill:** 999px, 30px min height, same wash-plus-colour mapping (running blue, success green, waiting amber, failed red). Saga pill uses the event state mapping.

### Cards / Containers
- **Panel:** paper, 1px `line` border, 16px radius (15px in the migration lab), 18-24px padding.
- **Card:** panel tint, 1px border, 12px radius, 14px padding; a down card turns red wash with a 30% red border.
- **Callout:** 10px radius, `#f3f6fb` fill, 12px text; bad and warn variants use the state washes.

### Inputs / Fields
- **Style:** white, 1px `line-strong` stroke, 7px radius, 36-38px tall, 11px side padding; hover darkens the stroke to `#adb9c9`.
- **Focus:** the shared 3px translucent blue outline.
- **Error:** red border (JSON editor), red helper text below.
- **Stepper:** a 36px segmented quantity control with flush tinted buttons.

### Navigation
- **Top bar:** ink brand mark (three white bars), product name at 18px, lab nav as a segmented control in a translucent white 10px-radius tray; the current lab is an ink pill with white text. Connection status is a pill with a state dot.
- **Tabs:** borderless 42px tabs, muted 13px/650 text, current tab ink with a 2px blue underline; counts are small pills that turn amber or red when something needs attention.

### Flow Rail (signature)
An ordered row of ten circular stops on a lifted white panel, joined by 2px connectors that turn green (done) or violet (compensated) behind the stop. Each stop shows its state fill, a 12px/700 name, a two-line muted detail, and sub-state dots. Waiting stops pulse a blue halo. The selected stop gets the blue outline. A legend of the six states follows directly beneath.

### Causation Graph Node (signature)
A white 11px-radius card in its service lane: a state dot with icon, the message type (13px/700), an uppercase 10px kind tag, the mono topic and partition@offset, and a three-segment step bar (outbox, kafka, consumer) that colours each step by its own state. Waiting nodes use a dashed blue border, failed nodes a red border and faint red fill, retrying an amber border. New nodes arrive with a 450ms fade-down. Projection nodes are a compact variant in the read-model lane.

### Inspector (signature)
A sticky paper panel beside the graph: header with title, meta and chips; sections of key-value grids, a vertical timeline whose dots take the state colours, identifier rows with copy buttons, and raw payloads in the Code Night block.

### Migration Track
The migration lab's five-stage track: 29px numbered circles on a 2px line that fills blue left to right; the active stage scales to 1.12 with a blue wash ring. Data packets travel the line while moving (amber and reversed during rollback).

## Do's and Don'ts

### Do:
- **Do** map every state to the six-state vocabulary and its exact colours; draw it as a filled circle for stops and dots, wash plus colour for chips and pills.
- **Do** keep panels paper with a 1px `line` border; lift only the page's lead panel with the ambient shadow.
- **Do** set ids, topics, offsets, headers, payloads and timestamps in mono at 11-12px, and everything else in the sans.
- **Do** use tabular numerals for every live number.
- **Do** give each surface one Control Blue primary action; switch it to ink when the action is armed to fail.
- **Do** use the shared 3px translucent blue focus outline on every interactive element.
- **Do** honour reduced motion by removing animation and transition entirely.

### Don't:
- **Don't** use green, amber, violet or red for anything but the state of that name.
- **Don't** draw waiting as a solid blue fill; it is outline, dash or halo on a wash.
- **Don't** build a page as a stack of equal, interchangeable panels; each panel carries one step of the story.
- **Don't** animate progress, arrivals or packets ahead of what the backend has reported.
- **Don't** set labels, headings or prose in mono.
- **Don't** add a second dark surface; Code Night is only for raw payloads.
