from pathlib import Path

import fitz


PDF_PATH = Path("midterm data pipeline project.pdf")
OUTPUT_DIR = Path(".agents/outputs/pdf_pages")


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    document = fitz.open(PDF_PATH)
    print(f"pages={document.page_count}")
    print(f"metadata={document.metadata}")

    for page_number, page in enumerate(document, start=1):
        pixmap = page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
        output_path = OUTPUT_DIR / f"page-{page_number:02d}.png"
        pixmap.save(output_path)
        print(f"rendered={output_path} size={pixmap.width}x{pixmap.height}")


if __name__ == "__main__":
    main()