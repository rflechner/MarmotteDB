# Fragment-based sorted index infographic

[Version française](sorted-index-table.prompt.fr.md)

Generated with the built-in `image_gen` tool from the following prompt, then
corrected to improve contrast.

```text
Use case: infographic-diagram
Asset type: a polished English technical infographic for the MarmotteDB repository documentation, to be saved as a PNG.
Primary request: explain the author's intended sorted_index_table.rs architecture, faithfully distinguishing code mechanisms from planned ones.
Style: exceptionally clear editorial technical infographic, flat vector-like rendering, restrained colors with strong semantic contrast, generous whitespace, crisp thin lines, beautiful large English typography. Light background. Professional software architecture documentation, not a slide screenshot, not a photorealistic image. No characters or invented mascot, no decorative unrelated objects.
Format: tall portrait 2:3, high resolution, with readable text at normal viewing size. All text must be in English; preserve exact code identifiers. Four carefully organized sections with the insertion/split example as the visual centerpiece. Avoid tiny text and crowding.

Top title, large: "MarmotteDB"
Main subtitle, large: "The fragment-based sorted index"
Small subheading: "Architecture intent · sorted_index_table.rs"
Intro sentence: "Distribute the index across multiple files to limit insertion costs."

SECTION 1, label: "01  A value points to a target"
Draw a horizontal entry with four clearly separated named cells:
"active" / "target" / "size" / "value"
Below: "Example: value 25 → target 4096"
Small caption: "An entry associates an indexed value with a reference to the data."
Do not label target as a definitely implemented byte offset: use only the generic reference language.
Show active means "Valid or inactive entry", not thread or process activity.

SECTION 2, label: "02  One file per fragment"
Draw one file schematic with filename "00000000.ix".
Top band "Header" containing "Count · Capacity · Min · Max".
Body has a clean ordered row of numeric values "10" "20" "30" "40".
Caption: "Entries sorted by value, then by target."
Beside it, show two smaller file outlines labeled "00000001.ix" and "00000002.ix" to communicate multiple fragments. Do not depict a guaranteed global sorted ordering or disjoint ranges across these files.

SECTION 3, label: "03  Three insertion paths"
Three clearly distinct compact paths with arrows and condition labels.
Path A: "Space available" → "Add, then sort the fragment"
Show a tiny simple sequence: "[10, 30] + 20 → [10, 20, 30]".
Path B, larger and visually dominant: "Full fragment, value between min and max" → "Split around the new value".
Show EXACTLY this split example, with careful arrow routing:
One source full fragment row "[10, 20, 30, 40]".
New item "25" approaching the split operation with a plus/arrow.
Two resulting file boxes side by side, one labeled "Existing fragment" with "[10, 20, 25]", the second "New fragment" with "[30, 40]".
Use a distinct highlight color for the inserted 25; a second consistent color for moved 30 and 40 in both the source row and new fragment. Each old value is conserved exactly once and the new 25 appears once in the final state. Caption: "Move greater values into a new file."
Path C: "No fragment selected" → "Create a fragment and write the entry".

SECTION 4, label: "04  The desired trade-off"
Clearly separate two dashed-outline boxes under an explicit badge "PLANNED".
Box 1 exact text:
"Limit shifts"
"shift_threshold"
"Create a fragment if insertion shifts too many entries."
Box 2 exact text:
"Merge fragments"
"max_incomplete_fragments_count"
"Compact incomplete fragments when too many have accumulated."
These dashed mechanisms are design intentions in comments, not implemented functionality.

Bottom discreet but readable footer with exact text:
"Prototype in progress"
"The shift threshold and compaction remain to be implemented. Binary placement of variable-sized values remains to be stabilized."
A second short footer line:
"The current choice favors the first fragment with available space: ranges may overlap."

Constraints: educational and faithful, not a production-ready claim. Distinguish per-fragment sorting from global sorting. Do not invent binary-search support, transactions, caching, tree structures, background workers, LSM or B-tree branding. Do not add unrequested slogans or fabricated metrics. Clear diagrams must match the exact split example. Use only the supplied copy or minimal labels explicitly needed by its diagrams. No watermark.
```

## Contrast correction

```text
Use case: precise-object-edit.
Edit target: the provided MarmotteDB infographic.
Make one focused readability correction: replace ALL dark/vignetted/gradient background areas, especially the top title zone and bottom footer zone, with a uniform solid opaque white or extremely pale gray background. No dark glow, no black corners, no haze, no translucent fade. Make ALL top and bottom text high contrast dark navy on white and fully readable.
Preserve the four sections, their exact English wording, the layout of the entry fields, file diagrams, numeric values, arrows, three insertion paths, colors inside the panels, and the PLANNED badge.
Ensure the top title "MarmotteDB" is completely inside the canvas with at least 40 px top margin, not clipped. The subtitle "The fragment-based sorted index" must remain clearly visible.
Provide enough bottom white space for these footer lines in readable dark text, no fading:
"Prototype in progress"
"The shift threshold and compaction remain to be implemented."
"Binary placement of variable-sized values remains to be stabilized."
"Fragment ranges may overlap."
If space is necessary, extend the canvas top and bottom and keep all the main panels intact. The output should be a professional clean English technical infographic with an entirely light, opaque background. Do not change any data in the split example: [10, 20, 30, 40] + 25 becomes [10, 20, 25] and [30, 40].
```
