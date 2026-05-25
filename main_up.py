from __future__ import annotations
import shutil
import time
from pathlib import Path
from typing import Iterable, List, Optional
import streamlit as st
import json
from pathlib import Path
from typing import Any, Iterable
import pandas as pd
from config import settings
from ingest import build_vector_stores
from query import generate_market_overview, resolve_base_faiss_dir
import executive_summary as exec_summary_module
import market_approach as market_approach_module
from merge import process_document
#from theme import apply_theme
from concurrent.futures import ThreadPoolExecutor, as_completed
#from help import render_help
# from word_compareables import txt_to_word
# from word_commentary import txt_to_word_commentary
#from extract_all_docint_tables_with_headings import extract_all_tables_with_headings
import traceback


# Prefer this function name from the summary script created earlier.
# If your local file exposes another alias, adjust only this import line.
#from create_forecast_summary import create_forecast_summary_json

APP_TITLE = "Realestate Automation GenAI"
DEFAULT_MERGED_OUTPUT_FILE = "output.docx"
DEFAULT_MERGED_OUTPUT_FILE_MARKET = "Market_Overview.docx"
DEFAULT_COMMENTARY_OUTPUT_FILE = "Commentary.docx"
DEFAULT_SALES_OUTPUT_FILE = "Sales.docx"
UPLOAD_DIR = Path("data") / "uploaded_inputs"
BASE_TEMPLATE_PATH = Path("BaseTemplate.docx")
BASE_MARKET_OVERVIEW_TEMPLATE_PATH = Path("BaseTemplate_MarketOverview.docx").expanduser().resolve()
BASE_COMMENTARY_TEMPLATE_PATH = Path("BaseTemplate_Commentary.docx").expanduser().resolve()
BASE_SALES_TEMPLATE_PATH = Path("BaseTemplate_Sales.docx").expanduser().resolve()


import uuid
import tempfile

def get_session_dir() -> Path:
    if "session_id" not in st.session_state:
        st.session_state.session_id = str(uuid.uuid4())

    d = Path(tempfile.gettempdir()) / "realestate_app" / st.session_state.session_id
    d.mkdir(parents=True, exist_ok=True)
    return d

# def generate_context_forecast_jsons(
#     selected_contexts: Iterable[str],
#     *,
#     pdf_dir: str | Path = "./data/pdfs",
#     faiss_manifest_path: str | Path = "./faiss/manifest.json",
#     docint_root: str | Path = "./docint",
#     output_dir: str | Path = ".",
#     model_id: str = "prebuilt-layout",
# ) -> list[dict[str, Any]]:
#     """
#     Generate forecast summary JSONs for selected contexts.

#     For each selected context:
#       1. Resolve context name to its source PDF.
#          Preferred mapping:
#            ./faiss/manifest.json -> indexes[context]["source_files"][0]
#          Fallback:
#            Match context name against files in ./data/pdfs.

#       2. Check if DocInt table extraction already exists:
#            ./docint/<pdf_stem>/table.xlsx
#            ./docint/<pdf_stem>/table.json

#       3. If not present, call:
#            extract_all_tables_with_headings(pdf_path, output_root="./docint")

#       4. Create forecast summary JSON from table.xlsx using:
#            create_forecast_summary_json(...)

#       5. Save final summary JSONs as:
#            Context1.json
#            Context2.json
#            Context3.json
#            ...

#     Parameters
#     ----------
#     selected_contexts:
#         List/iterable of selected context names, e.g. ["canada_q4", "montreal_q2"].

#     pdf_dir:
#         Folder containing source PDFs.

#     faiss_manifest_path:
#         Manifest created during ingestion.

#     docint_root:
#         Root folder where DocInt outputs are stored.

#     output_dir:
#         Folder where final Context1.json, Context2.json, etc. will be saved.

#     model_id:
#         Azure Document Intelligence model ID.

#     Returns
#     -------
#     list[dict[str, Any]]
#         One result dict per context.
#     """

#     selected_contexts = list(selected_contexts)
#     pdf_dir = Path(pdf_dir).expanduser().resolve()
#     manifest_path = Path(faiss_manifest_path).expanduser().resolve()
#     docint_root = Path(docint_root).expanduser().resolve()
#     output_dir = Path(output_dir).expanduser().resolve()

#     output_dir.mkdir(parents=True, exist_ok=True)
#     docint_root.mkdir(parents=True, exist_ok=True)

#     manifest: dict[str, Any] = {}
#     if manifest_path.exists():
#         manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

#     def _norm(value: str) -> str:
#         return (
#             value.lower()
#             .replace(".pdf", "")
#             .replace("-", "_")
#             .replace(" ", "_")
#             .strip()
#         )

#     def _resolve_pdf_for_context(context_name: str) -> Path:
#         """
#         Resolve selected context/index name to PDF path.
#         Preferred: manifest indexes[context_name]["source_files"].
#         Fallback: fuzzy match against files in pdf_dir.
#         """
#         indexes = manifest.get("indexes", {}) if manifest else {}

#         # 1. Exact context key match in manifest.
#         if context_name in indexes:
#             source_files = indexes[context_name].get("source_files", [])
#             if source_files:
#                 candidate = pdf_dir / source_files[0]
#                 if candidate.exists():
#                     return candidate.resolve()

#         # 2. Normalized context key match in manifest.
#         normalized_context = _norm(context_name)
#         for index_key, index_info in indexes.items():
#             if _norm(index_key) == normalized_context:
#                 source_files = index_info.get("source_files", [])
#                 if source_files:
#                     candidate = pdf_dir / source_files[0]
#                     if candidate.exists():
#                         return candidate.resolve()

#         # 3. Fallback: match context name against PDF stems.
#         pdf_candidates = sorted(pdf_dir.glob("*.pdf"))
#         for pdf_path in pdf_candidates:
#             if _norm(pdf_path.stem) == normalized_context:
#                 return pdf_path.resolve()

#         # 4. Fallback: partial match either way.
#         for pdf_path in pdf_candidates:
#             pdf_key = _norm(pdf_path.stem)
#             if normalized_context in pdf_key or pdf_key in normalized_context:
#                 return pdf_path.resolve()

#         raise FileNotFoundError(
#             f"Could not map selected context '{context_name}' to a PDF. "
#             f"Checked manifest: {manifest_path} and PDF folder: {pdf_dir}"
#         )

#     results: list[dict[str, Any]] = []

#     for idx, context_name in enumerate(selected_contexts, start=1):
#         pdf_path = _resolve_pdf_for_context(context_name)

#         table_output_dir = docint_root / pdf_path.stem
#         table_xlsx = table_output_dir / "table.xlsx"
#         table_json = table_output_dir / "table.json"

#         # Reuse existing DocInt output if both files exist.
#         if table_xlsx.exists() and table_json.exists():
#             docint_result = {
#                 "source_pdf": str(pdf_path),
#                 "output_dir": str(table_output_dir),
#                 "excel_path": str(table_xlsx),
#                 "json_path": str(table_json),
#                 "reused_existing_docint": True,
#             }
#         else:
#             docint_result = extract_all_tables_with_headings(
#                 pdf_path,
#                 output_root=docint_root,
#                 model_id=model_id,
#             )
#             docint_result["reused_existing_docint"] = False

#             table_xlsx = Path(docint_result["excel_path"]).expanduser().resolve()
#             table_json = Path(docint_result["json_path"]).expanduser().resolve()

#         final_json_path = output_dir / f"Context{idx}.json"

#         summary_result = create_forecast_summary_json(
#             table_xlsx,
#             output_json_path=final_json_path,
#         )

#         results.append(
#             {
#                 "context_number": idx,
#                 "context_name": context_name,
#                 "pdf_path": str(pdf_path),
#                 "docint_output_dir": str(table_output_dir),
#                 "docint_excel_path": str(table_xlsx),
#                 "docint_json_path": str(table_json),
#                 "reused_existing_docint": docint_result.get(
#                     "reused_existing_docint", False
#                 ),
#                 "summary_json_path": str(final_json_path),
#                 "summary_result": {
#                     k: v
#                     for k, v in summary_result.items()
#                     if k != "payload"
#                 },
#             }
#         )

#     return results

def cleanup_previous_run_outputs_once(remove: bool = False, base_dir: str | Path = ".") -> None:
    """
    Remove only specific output files from previous runs, plus everything
    inside ./data/merge_content, only once per Streamlit session if remove=True.

    Deletes ONLY:
      - response_RAG_gpt5mini.txt
      - response_RAG_gpt5mini_2.txt
      - commentary_output.txt
      - generated_market_approach_output.txt
      - all contents inside ./data/merge_content

    Does NOT remove anything else.
    """
    if not remove:
        return

    # Run only once per Streamlit session
    session_key = "_previous_run_outputs_cleaned"
    if st.session_state.get(session_key, False):
        return

    base_dir = Path(base_dir).expanduser().resolve()

    files_to_remove = [
        "response_RAG_gpt5mini.txt",
        "response_RAG_gpt5mini_2.txt",
        "commentary_output.txt",
        "generated_market_approach_output.txt",
        "map_1.png",
        "map_2.png",
        "address_dca.txt",
        "address_summary.txt",
    ]

    # Remove only the exact files listed above
    for filename in files_to_remove:
        file_path = base_dir / filename
        if file_path.exists() and (file_path.is_file() or file_path.is_symlink()):
            file_path.unlink()

    # Remove everything inside ./data/merge_content, but not the folder itself
    merge_dir = base_dir / "data" / "merge_content"
    upload_dir = base_dir / "data" / "uploaded_inputs"

    dir_list = [merge_dir,upload_dir]
    for folder in dir_list:
        if folder.exists() and folder.is_dir():
            for item in folder.iterdir():
                if item.is_file() or item.is_symlink():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)

    # Mark cleanup as done for this session
    st.session_state[session_key] = True

def _ensure_market_overview_session_state():
    defaults = {
        "market_overview_text": "",
        "market_overview_output_path": "",          # backward-compatible: final downloadable path
        "market_overview_text_by_tag": {},          # e.g. {"T2": "...", "T2_2": "..."}
        "market_overview_docx_paths": [],           # docx paths produced per DB
        "market_overview_download_path": "",        # final docx to download
        "market_overview_ran_dbs": [],              # captures all DBs run this session
        "market_overview_used_multiple_dbs": False, # explicit flag for 2-DB run
        "models_ran_this_session": False,           # preserve existing meaning if other code depends on it
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


# ---------- Existing app helpers ----------

def _run_market_overview_task(args): #helper for parallel compute
    return run_market_overview(args)

def get_pdf_dir() -> Path:
    configured = getattr(settings, "pdf_dir", None)
    return Path(configured) if configured else Path("data") / "pdfs"


def list_available_dbs() -> List[str]:
    try:
        base_dir = resolve_base_faiss_dir()
    except FileNotFoundError:
        return []

    dbs: List[str] = []
    for path in sorted(base_dir.iterdir()):
        if path.is_dir() and (path / "index.faiss").exists():
            dbs.append(path.name)
    return dbs


def save_uploaded_pdfs(uploaded_files) -> List[Path]:
    target_dir = get_pdf_dir()
    target_dir.mkdir(parents=True, exist_ok=True)

    saved_paths: List[Path] = []
    for uploaded_file in uploaded_files:
        target_path = target_dir / uploaded_file.name
        target_path.write_bytes(uploaded_file.getbuffer())
        saved_paths.append(target_path)
    return saved_paths

def save_uploaded_template(uploaded_file) -> Path:
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    target_path = UPLOAD_DIR / uploaded_file.name
    target_path.write_bytes(uploaded_file.getbuffer())
    return target_path.resolve()


def render_sidebar() -> None:
    st.sidebar.header("Folders")
    st.sidebar.code(f"PDF input: {get_pdf_dir()}")
    try:
        st.sidebar.code(f"FAISS DBs: {resolve_base_faiss_dir()}")
    except FileNotFoundError:
        st.sidebar.warning("FAISS by_report folder is not available yet.")


def ensure_excel_source(uploaded_excel) -> Path | None:
    return None if uploaded_excel is None else save_uploaded_template(uploaded_excel)


def run_market_overview(selected_db: str) -> str:
    return generate_market_overview(selected_db)


def run_executive_summary(excel_path: Path, sheet_name :str) -> str:
    executive_summary_sheet = sheet_name 
    #print(executive_summary_sheet)
    if executive_summary_sheet is None:
        return "Appropriate Sheet not selected for generation"

    try:
        table_text = exec_summary_module.extract_exec_summary_table(
            excel_path,
            executive_summary_sheet,
        )
    except ValueError as e:
        return "Appropriate Sheet not found for generation" + f"Could not extract executive summary context: {e}"

    commentary = exec_summary_module.generate_commentary(table_text)
    return commentary


def run_market_approach(excel_path: Path, sheet_name :str) -> str:
    try:
        comp_sheet = sheet_name 
        #print(comp_sheet)
        sheet_to_use = (
            comp_sheet
            if comp_sheet is not None
            else market_approach_module.SHEET_NAME
        )
        context = market_approach_module.extract_market_approach_context(
            str(excel_path),
            sheet_to_use,
        )
    except ValueError as e:
        return "Appropiate Sheet not found for generation" + f"Could not extract market approach context: {e}"

    messages = market_approach_module.build_messages(context)
    response_text = market_approach_module.call_azure_openai(messages)
    return response_text


# ---------- New DOCX merge helpers ----------


def get_existing_merged_docx_path() -> Optional[Path]:
    session_path = st.session_state.get("merged_docx_output_path")
    if session_path:
        candidate = Path(session_path).expanduser().resolve()
        if candidate.exists():
            return candidate

    root_output = Path(DEFAULT_MERGED_OUTPUT_FILE).expanduser().resolve()
    if root_output.exists():
        return root_output

    return None


def outputs_ready_for_merge() -> bool:
    return bool(
        build_merge_content_map(
            executive_summary_text=st.session_state.get("executive_summary_text"),
            market_overview_texts=[
                text
                for _, text in sorted(
                    st.session_state.get("market_overview_text_by_tag", {}).items()
                )
            ],
            market_approach_text=st.session_state.get("market_approach_text"),
        )
    )


def build_market_overview_content_map(
    overview_texts: Iterable[str],
) -> dict[str, str]:
    tags = ["T2", "T2_2"]
    content_map: dict[str, str] = {}

    for tag, text in zip(tags, overview_texts):
        if not text:
            continue
        content_map[tag] = text

    return content_map


def build_merge_content_map(
    *,
    executive_summary_text: str | None = None,
    market_overview_texts: Iterable[str] | None = None,
    market_approach_text: str | None = None,
) -> dict[str, str]:
    content_map: dict[str, str] = {}

    if executive_summary_text:
        content_map["T1"] = executive_summary_text

    if market_overview_texts:
        content_map.update(
            build_market_overview_content_map(market_overview_texts)
        )

    if market_approach_text:
        content_map["T3"] = market_approach_text

    return content_map


def generate_merged_docx_bytes(
    merge_path: Path | None = None,
    uploaded_template=None,
    excel_path: Path | None = None,
    path_only: bool = False,
    map: bool = False,
    content_map: dict[str, str] | None = None,
):
    template_path = (
            Path(uploaded_template).expanduser().resolve()
            if isinstance(uploaded_template, (str, Path))
            else save_uploaded_template(uploaded_template)
            if uploaded_template is not None
            else BASE_TEMPLATE_PATH.expanduser().resolve()
        )

    if not template_path.exists():
        raise FileNotFoundError(
            f"Base DOCX template was not found in the root folder: {template_path}"
        )

    merged_path = (
        Path(merge_path).expanduser().resolve()
        if merge_path is not None
        else Path(DEFAULT_MERGED_OUTPUT_FILE).expanduser().resolve()
    )

    merged_path.parent.mkdir(parents=True, exist_ok=True)
    if content_map is None:
        raise ValueError("content_map is required for Streamlit merges.")

    process_document(
        template_path,
        content_map,
        merged_path,
        excel_path,
        map=map,
    )

    # Optional: persist for later UI/status checks
    st.session_state.merged_docx_output_path = str(merged_path)

    if path_only:
        return str(merged_path)

    return merged_path.read_bytes()


st.set_page_config(page_title=APP_TITLE, layout="wide")
#apply_theme()
render_sidebar()
cleanup_previous_run_outputs_once(remove=True) #Uncomment when deploying

if "available_dbs" not in st.session_state:
    st.session_state.available_dbs = list_available_dbs()

for key in (
    "market_overview_text",
    "market_overview_output_path",
    "executive_summary_text",
    "market_approach_text",
    "merged_docx_output_path",
):
    if key not in st.session_state:
        st.session_state[key] = ""

if "models_ran_this_session" not in st.session_state:
    st.session_state.models_ran_this_session = False

homepage_tab, help_tab = st.tabs(["Homepage 🏠", "Help ❓"])

with homepage_tab:
    CARD_HEIGHT = 400
    c1, c2 = st.columns([1, 1])

    with c1:
        with st.container(border=True, height=CARD_HEIGHT):
            st.subheader("Upload Market Overview PDFs")

            uploaded_files = st.file_uploader(
                "Select one or more PDF files",
                type=["pdf"],
                accept_multiple_files=True,
                help="Files will be used for generation of content.",
            )

            if st.button("Process PDFs", use_container_width=True):
                if not uploaded_files:
                    st.warning("Please choose at least one PDF file to upload.")
                else:
                    try:
                        saved_files = save_uploaded_pdfs(uploaded_files)
                        st.success(f"Saved {len(saved_files)} file(s) to {get_pdf_dir().resolve()}")

                        for file_path in saved_files:
                            st.write(f"- {file_path.name}")

                    except Exception as exc:
                        st.error(f"Failed to save PDFs: {exc}")

                    try:
                        with st.spinner("Building FAISS indexes from PDFs..."):
                            manifest = build_vector_stores()

                        st.session_state.available_dbs = list_available_dbs()

                        st.success(
                            f"Rebuilt FAISS DBs successfully. "
                            f"Created {len(manifest.get('indexes', {}))} index(es)."
                        )

                    except Exception as exc:
                        st.error(f"Failed to rebuild FAISS DBs: {exc}")

            st.subheader("Select Research Context In Order")
            available_dbs = list_available_dbs()

            if available_dbs:
                selected_dbs = st.multiselect(
                    "Available Context",
                    options=available_dbs,
                    max_selections=2,
                )

                selected_db = selected_dbs[0] if selected_dbs else None

            else:
                selected_dbs = []
                selected_db = None
                st.info("No FAISS DBs found yet. Upload PDFs and click 'Process PDFs'.")

    with c2:
        with st.container(border=True, height=CARD_HEIGHT):
            st.subheader("Excel upload")

            uploaded_excel = st.file_uploader(
                "Upload one Excel workbook for Executive Summary and Market Approach",
                type=["xlsx", "xlsm"],
                accept_multiple_files=False,
                help="Please upload an Excel workbook. This file will be used for both flows.",
            )
            executive_summary_sheet = None
            comparables_sheet = None
            
            if uploaded_excel is not None:
                try:
                    excel_file = pd.ExcelFile(uploaded_excel)

                    sheet_names = [None] + excel_file.sheet_names

                    executive_summary_sheet = st.selectbox(
                        "Select sheet for Executive Summary",
                        sheet_names,
                        index=0,
                        key="executive_summary_sheet_selector",
                        format_func=lambda x: "Not Available" if x is None else x,
                    )

                    comparables_sheet = st.selectbox(
                        "Select sheet for Comparables / Market Approach",
                        sheet_names,
                        index=0,
                        key="comparables_sheet_selector",
                        format_func=lambda x: "Not Available" if x is None else x,
                    )
                    # Reset uploaded file pointer so it can be reused later
                    uploaded_excel.seek(0)

                except Exception as exc:
                    st.error(f"Could not read sheets from uploaded workbook: {exc}")


                st.caption(
                    f"Uploaded Excel will be used for both flows: {uploaded_excel.name}"
                )
            else:
                st.warning(
                    "Please upload an Excel workbook to continue. No default workbook is available."
                )

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        generate_overview_clicked = st.button(
            "Generate market overview",
            use_container_width=True,
            disabled=not bool(selected_db),
        )
        generate_exec_clicked = st.button(
            "Generate executive summary",
            use_container_width=True,
            disabled = executive_summary_sheet is None,
        )
    with action_col2:
        generate_market_clicked = st.button(
            "Generate market approach",
            use_container_width=True,
            disabled=comparables_sheet is None,
        )
        run_all_clicked = st.button(
            "Run all",
            use_container_width=True,
            disabled=not bool(selected_db),
        )

    _ensure_market_overview_session_state()
    if generate_overview_clicked:
        try:
            selected_dbs_to_run = selected_dbs if selected_dbs else ([selected_db] if selected_db else [])

            if not selected_dbs_to_run:
                st.warning("Please select at least one DB.")
                st.stop()

            # results_json = generate_context_forecast_jsons(
            #     selected_dbs_to_run,
            #     output_dir="./data/merge_content"
            # )

            with st.spinner("Generating market overview in parallel..."):
                with ThreadPoolExecutor(max_workers=min(2, len(selected_dbs_to_run))) as executor:
                    generated_texts = list(executor.map(_run_market_overview_task, selected_dbs_to_run))

            overview_content_map = build_merge_content_map(
                market_overview_texts=generated_texts,
            )

            final_download_path =  generate_merged_docx_bytes(uploaded_template=BASE_MARKET_OVERVIEW_TEMPLATE_PATH , merge_path=DEFAULT_MERGED_OUTPUT_FILE_MARKET , path_only=True, content_map=overview_content_map)

            # Session state updates
            st.session_state.market_overview_text = "\n\n".join(generated_texts)
            st.session_state.market_overview_text_by_tag = overview_content_map
            st.session_state.market_overview_output_path = final_download_path
            st.session_state.market_overview_download_path = final_download_path
            st.session_state.market_overview_ran_dbs = selected_dbs_to_run
            st.session_state.market_overview_used_multiple_dbs = len(selected_dbs_to_run) > 1
            st.session_state.models_ran_this_session = True

            st.success("Market overview generated successfully.")

        except Exception as exc:
            st.error(f"Failed to generate market overview: {exc}")

    if generate_exec_clicked:
        try:
            excel_path = ensure_excel_source(uploaded_excel)
            with st.spinner(f"Generating executive summary from: {excel_path.name}..."):
                st.session_state.executive_summary_text = run_executive_summary(excel_path,st.session_state.get("executive_summary_sheet_selector"))
            st.session_state.models_ran_this_session = True
            st.success("Executive summary generated successfully.")
        except Exception as exc:
            st.error(f"Failed to generate executive summary: {exc}")

    if generate_market_clicked:
        try:
            excel_path = ensure_excel_source(uploaded_excel)
            with st.spinner(f"Generating market approach from: {excel_path.name}..."):
                st.session_state.market_approach_text = run_market_approach(excel_path,st.session_state.get("comparables_sheet_selector"))
            st.session_state.models_ran_this_session = True
            st.success("Market approach generated successfully.")
        except Exception as exc:
            st.error(f"Failed to generate market approach: {exc}")

    if run_all_clicked:
        try:
            start = time.time()
            excel_path = ensure_excel_source(uploaded_excel)
            selected_dbs_to_run = selected_dbs if selected_dbs else ([selected_db] if selected_db else [])

            if not selected_dbs_to_run:
                st.warning("Please select at least one DB.")
                st.stop()

            with st.spinner("All LLM workflows running..."):
                # results_json = generate_context_forecast_jsons(
                # selected_dbs_to_run,
                # output_dir="./data/merge_content"
                # )
                with ThreadPoolExecutor(max_workers=min(2, len(selected_dbs_to_run))) as executor:
                    generated_texts = list(executor.map(_run_market_overview_task, selected_dbs_to_run))

                overview_content_map = build_market_overview_content_map(
                    generated_texts,
                )
                
                # with ThreadPoolExecutor(max_workers=2) as executor:
                #     exec_future = executor.submit(run_executive_summary, excel_path)
                #     market_future = executor.submit(run_market_approach, excel_path)

                #     exec_text = exec_future.result()
                #     market_text = market_future.result()

                exec_text = run_executive_summary(excel_path,st.session_state.get("executive_summary_sheet_selector"))
                market_text = run_market_approach(excel_path,st.session_state.get("comparables_sheet_selector"))
                merge_content_map = build_merge_content_map(
                    executive_summary_text=exec_text,
                    market_overview_texts=generated_texts,
                    market_approach_text=market_text,
                )
                final_download_path =  generate_merged_docx_bytes(uploaded_template=BASE_MARKET_OVERVIEW_TEMPLATE_PATH , merge_path=DEFAULT_MERGED_OUTPUT_FILE_MARKET , path_only=True, content_map=merge_content_map)
            
            # Session state updates
            st.session_state.executive_summary_text = exec_text
            st.session_state.market_approach_text = market_text
            st.session_state.market_overview_text = "\n\n".join(generated_texts)
            st.session_state.market_overview_text_by_tag = overview_content_map
            st.session_state.market_overview_output_path = final_download_path
            st.session_state.market_overview_download_path = final_download_path
            st.session_state.market_overview_ran_dbs = selected_dbs_to_run
            st.session_state.market_overview_used_multiple_dbs = len(selected_dbs_to_run) > 1
            st.session_state.models_ran_this_session = True
            st.success("All three LLM workflows completed successfully.")
            end = time.time()
            print(f"time took {end - start:.3f} seconds")

        
        except Exception as e:
            st.error(f"Failed to run all workflows: {repr(e)}")
            st.code(traceback.format_exc())


    st.subheader("Generated outputs")
    output_tabs = st.tabs(["Market Overview", "Executive Summary", "Market Approach", "Merged DOCX"])

    with output_tabs[0]:
        if st.session_state.market_overview_text:
            st.text(st.session_state.market_overview_text)

            if st.session_state.market_overview_ran_dbs:
                st.caption(
                    "DB(s) used: " + ", ".join(st.session_state.market_overview_ran_dbs)
                )

            if st.session_state.market_overview_download_path:
                download_path = Path(st.session_state.market_overview_download_path)

                with download_path.open("rb") as f:
                    st.download_button(
                        label="Download market overview DOCX",
                        data=f.read(),
                        file_name=download_path.name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    )
        else:
            st.caption("The market overview output will appear here once generated.")

    with output_tabs[1]:
        executive_summary_text = st.session_state.get("executive_summary_text", "")
        if executive_summary_text:
            st.text(executive_summary_text)
            st.download_button(
                label="Download executive summary DOCX",
                data=lambda: generate_merged_docx_bytes(
                    uploaded_template=BASE_COMMENTARY_TEMPLATE_PATH,
                    merge_path=DEFAULT_COMMENTARY_OUTPUT_FILE,
                    map=False,
                    content_map={"T1": executive_summary_text},
                ),
                file_name=DEFAULT_COMMENTARY_OUTPUT_FILE,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                type="primary",
                width="stretch",
                key="download_commentary_docx",
                on_click="ignore",
            )
        else:
            st.caption("The output will appear here once generated.")

    with output_tabs[2]:
        market_approach_text = st.session_state.get("market_approach_text", "")
        if market_approach_text:
            st.text(market_approach_text)
            st.download_button(
                label="Download sales DOCX",
                data=lambda: generate_merged_docx_bytes(
                    uploaded_template=BASE_SALES_TEMPLATE_PATH,
                    merge_path=DEFAULT_SALES_OUTPUT_FILE,
                    map=False,
                    content_map={"T3": market_approach_text},
                ),
                file_name=DEFAULT_SALES_OUTPUT_FILE,
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                type="primary",
                width="stretch",
                key="download_sales_docx",
                on_click="ignore",
            )
        else:
            st.caption("The output will appear here once generated.")

    with output_tabs[3]:
        st.subheader("Merged DOCX")
        uploaded_template = st.file_uploader(
            "Optional: Upload DOCX template",
            type=["docx"],
            accept_multiple_files=False,
            help="If uploaded, it will be used instead of the default BaseTemplate.docx.",
            key="uploaded_base_template",
        )
        overview_text_by_tag = st.session_state.get("market_overview_text_by_tag", {})
        merge_content_map: dict[str, str] = {}
        if st.session_state.get("executive_summary_text"):
            merge_content_map["T1"] = st.session_state.executive_summary_text
        merge_content_map.update(
            {
                tag: overview_text_by_tag[tag]
                for tag in ("T2", "T2_2")
                if overview_text_by_tag.get(tag)
            }
        )
        if st.session_state.get("market_approach_text"):
            merge_content_map["T3"] = st.session_state.market_approach_text
        ready = bool(merge_content_map)

        if not ready:
            st.warning(
                "Merge content is not ready yet. Please generate commentary, "
                "market overview, and market approach first."
            )
        #sht_name = st.session_state.get("comparables_sheet_selector")
        st.download_button(
            label="Download merged DOCX - May take a few seconds",
            data=lambda: generate_merged_docx_bytes(
                uploaded_template=uploaded_template,
                map=False,
                content_map=merge_content_map,
            ),  # callable runs on click
            file_name=DEFAULT_MERGED_OUTPUT_FILE,
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            type="primary",
            width="stretch",
            key="download_merged_docx",
            disabled=not ready,
            on_click="ignore",
        )

        merged_docx_path = get_existing_merged_docx_path()
        if merged_docx_path is not None and merged_docx_path.exists():
            st.info(f"Please run generation to get latest output.")

with help_tab:
    st.markdown("### help")
    #render_help()
