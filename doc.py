#section removers
def get_paragraph_heading_level(paragraph) -> int | None:
    style_name = getattr(paragraph.style, "name", "") or ""
    match = re.fullmatch(r"Heading\s+([1-9])", style_name.strip(), re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None


def section_remover(doc: Document, headers_to_remove: list[str] | None) -> list[str]:
    """
    Remove each selected Heading 1/Heading 2 section from the document body.

    A Heading 1 section is removed until the next Heading 1. A Heading 2 section
    is removed until the next Heading 1 or Heading 2.
    """
    if not headers_to_remove:
        return []

    normalized_targets = {
        str(header).strip()
        for header in headers_to_remove
        if str(header).strip()
    }
    if not normalized_targets:
        return []

    removed_headers: list[str] = []
    body = doc.element.body
    children = list(body.iterchildren())
    remove_indexes: set[int] = set()

    final_sectpr = next(
        (child for child in reversed(children) if child.tag.endswith("}sectPr")),
        None,
    )

    def can_remove_body_child(element) -> bool:
        return not (final_sectpr is not None and element is final_sectpr)

    idx = 0
    while idx < len(children):
        child = children[idx]
        if not child.tag.endswith("}p"):
            idx += 1
            continue

        paragraph = Paragraph(child, doc)
        heading_level = get_paragraph_heading_level(paragraph)
        heading_text = paragraph.text.strip()
        if heading_level not in {1, 2} or heading_text not in normalized_targets:
            idx += 1
            continue

        removed_headers.append(heading_text)
        remove_indexes.add(idx)
        scan_idx = idx + 1
        while scan_idx < len(children):
            scan_child = children[scan_idx]
            if scan_child.tag.endswith("}p"):
                scan_paragraph = Paragraph(scan_child, doc)
                scan_level = get_paragraph_heading_level(scan_paragraph)
                if scan_level is not None and scan_level <= heading_level:
                    break
            if can_remove_body_child(scan_child):
                remove_indexes.add(scan_idx)
            scan_idx += 1
        idx = scan_idx

    for remove_idx in sorted(remove_indexes, reverse=True):
        child = children[remove_idx]
        parent = child.getparent()
        if parent is not None:
            parent.remove(child)

    return removed_headers

#caller
def get_template_section_headers(template_path: Path) -> list[str]:
    doc = Document(str(template_path))
    headers: list[str] = []
    seen: set[str] = set()

    for paragraph in doc.paragraphs:
        style_name = getattr(paragraph.style, "name", "")
        if style_name not in {"Heading 1", "Heading 2"}:
            continue

        header_text = paragraph.text.strip()
        if header_text and header_text not in seen:
            headers.append(header_text)
            seen.add(header_text)

    return headers

#tab3;
section_template_path = (
    save_uploaded_template(uploaded_template)
    if uploaded_template is not None
    else BASE_TEMPLATE_PATH.expanduser().resolve()
)
section_headers = []
try:
    section_headers = get_template_section_headers(section_template_path)
except Exception as exc:
    st.warning(f"Could not read section headers from template: {exc}")

if section_headers:
    if st.session_state.get("_merged_docx_section_headers") != section_headers:
        st.session_state._merged_docx_section_headers = section_headers
        st.session_state.merged_docx_selected_sections = section_headers

    selected_sections = st.multiselect(
        "Sections to include",
        options=section_headers,
        default=section_headers,
        help="Deselect Heading 1 or Heading 2 sections to remove them from the merged DOCX.",
        key="merged_docx_selected_sections",
    )
    sections_to_remove = [
        header for header in section_headers if header not in selected_sections
    ]
    st.session_state.merged_docx_sections_to_remove = sections_to_remove
else:
    sections_to_remove = []
    st.session_state.merged_docx_sections_to_remove = []
merged_docx_sections_to_remove = sections_to_remove


#in merge
    map: bool = False,
    table_job: dict | None = None,
    sections_to_remove: list[str] | None = None,
):
    doc = Document(str(template_path))
    removed_sections = section_remover(doc, sections_to_remove)
    tag_map = (
        load_tag_map_from_dict(content_dir)
        if isinstance(content_dir, dict)

    doc.save(str(output_path))
    print(f"Saved: {output_path}")
    if removed_sections:
        print(f"Removed sections: {', '.join(removed_sections)}")
