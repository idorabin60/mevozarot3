import pypdf
import sys

try:
    reader = pypdf.PdfReader("assignment2 (6).pdf")
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    print(text)
except Exception as e:
    print(f"Error: {e}")
