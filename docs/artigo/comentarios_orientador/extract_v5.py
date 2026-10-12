import json
import fitz

pdf_path = r"d:\tiokk-n8n\docs\artigo\comentarios_orientador\rafael-rezende-tcc-v5.pdf"
doc = fitz.open(pdf_path)
print(f"Total pages: {len(doc)}")

annotations = []
for page_num in range(len(doc)):
    page = doc[page_num]
    annots = page.annots()
    if not annots:
        continue
    for annot in annots:
        info = annot.info
        rect = list(annot.rect)
        # Try to extract marked/highlighted text if applicable
        highlighted_text = ""
        try:
            # For highlights, get text under rect
            highlighted_text = page.get_textbox(annot.rect)
        except Exception:
            pass
            
        annotations.append({
            "page": page_num + 1,
            "type": annot.type[1] if annot.type else "Unknown",
            "author": info.get("title", ""),
            "content": info.get("content", ""),
            "subject": info.get("subject", ""),
            "rect": rect,
            "highlighted_text": highlighted_text.strip()
        })

print(f"Total annotations found: {len(annotations)}")

output_json = r"d:\tiokk-n8n\docs\artigo\comentarios_orientador\v5_annotations.json"
with open(output_json, "w", encoding="utf-8") as f:
    json.dump(annotations, f, indent=2, ensure_ascii=False)

for idx, a in enumerate(annotations, 1):
    print(f"--- Nota {idx} (Página {a['page']}) ---")
    print(f"Tipo: {a['type']} | Autor: {a['author']}")
    print(f"Conteúdo: {a['content']}")
    if a['highlighted_text']:
        print(f"Texto selecionado: {a['highlighted_text']}")
    print()
