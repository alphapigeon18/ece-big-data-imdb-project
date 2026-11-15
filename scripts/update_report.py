"""Copy the generated result tables into REPORT.md.

REPORT.md contains placeholders of the form

    <!-- RESULT:exploration/q1_genre_runtime_stats -->
    ...anything...
    <!-- /RESULT -->

Everything between the two markers is replaced by the content of
``outputs/exploration/q1_genre_runtime_stats.md``. Text outside the markers
(the explanations written by the team) is never modified.

Usage:  python scripts/update_report.py [--report REPORT.md]
"""

import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BLOCK = re.compile(
    r"(<!-- RESULT:(?P<name>[\w/.-]+) -->\n)(?P<body>.*?)(<!-- /RESULT -->)",
    re.DOTALL,
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", default=str(ROOT / "REPORT.md"))
    parser.add_argument("--outputs", default=str(ROOT / "outputs"))
    args = parser.parse_args()
    report = Path(args.report)
    outputs = Path(args.outputs)
    filled, missing = [], []

    def replace(match):
        source = outputs / f"{match.group('name')}.md"
        if source.exists():
            filled.append(match.group("name"))
            body = source.read_text(encoding="utf-8").strip() + "\n"
        else:
            missing.append(match.group("name"))
            body = "_Result not generated yet._\n"
        return match.group(1) + "\n" + body + "\n" + match.group(4)

    text = BLOCK.sub(replace, report.read_text(encoding="utf-8"))
    report.write_text(text, encoding="utf-8")
    print(f"{len(filled)} tables updated, {len(missing)} missing")
    for name in missing:
        print(f"  missing: outputs/{name}.md")


if __name__ == "__main__":
    main()
