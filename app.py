"""
3GPP Contribution Analyzer v2 — Lightweight Edition
====================================================
Output 1: Conclusions 취합 .docx (원본과 동일)
Output 2: TF-IDF Proposal Summary .docx (원본과 동일)
Output 3: Gemini 의미 분석 (선택, 서버 키 고정)

Cloud Function으로 다운로드/파싱 위임 가능 (미설정 시 직접 처리)
"""

import streamlit as st
import os
import tempfile
import zipfile
import requests
import numpy as np
import re
import io
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from openpyxl import load_workbook
from docx import Document
from docx.table import Table
from docx.text.paragraph import Paragraph
from sklearn.cluster import AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import google.generativeai as genai

# ==========================================
# 1. Page Config & Session State
# ==========================================
st.set_page_config(page_title="3GPP Analyzer v2", page_icon="📡", layout="wide")

DEFAULTS = {
    "authenticated": False,
    "process_done": False,
    "extracted_data": [],
    "out1_bytes": None,
    "out2_bytes": None,
    "notebooklm_txt": None,
    "log_text": "",
    "ai_summary_generated": False,
    "ai_summary_bytes": None,
    "ai_summary_text": "",
    "ai_model_name": "",
    # Meeting/agenda selection
    "meeting_list": [],
    "agenda_dict": {},
    "all_entries": [],
}
for k, v in DEFAULTS.items():
    if k not in st.session_state:
        st.session_state[k] = v


def append_log(text):
    st.session_state.log_text += f"{text}\n"


# ==========================================
# 2. Config
# ==========================================
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "") or st.secrets.get("GEMINI_API_KEY", "")
CLOUD_FUNCTION_URL = os.environ.get("CLOUD_FUNCTION_URL", "") or st.secrets.get("CLOUD_FUNCTION_URL", "")


# ==========================================
# 3. 원본 그대로 — 유틸리티 함수들
# ==========================================
def read_excel_from_bytes(uploaded_file):
    wb = load_workbook(uploaded_file, read_only=False, data_only=True)
    ws = wb.active
    entries = []
    for row in ws.iter_rows(min_row=2):
        cell = row[0]
        comp = row[2] if len(row) > 2 else None
        docid = str(cell.value).strip() if cell.value else ""
        company = str(comp.value).strip() if comp and comp.value else ""
        if not docid:
            continue
        if getattr(cell, "hyperlink", None) and cell.hyperlink.target:
            link = cell.hyperlink.target
        else:
            link = f"https://www.3gpp.org/ftp/tsg_ran/WG1_RL1/TSGR1_122/Docs/{docid}.zip"
        entries.append({"doc": docid, "company": company, "link": link})
    return entries


# ==========================================
# 3b. 회의 번호 → FTP에서 TDoc 리스트 xlsx 자동 조회
# ==========================================
# WG별 FTP 경로 매핑
WG_FTP_MAP = {
    "RAN1": "tsg_ran/WG1_RL1",
    "RAN2": "tsg_ran/WG2_RL2",
    "RAN3": "tsg_ran/WG3_IU",
    "RAN4": "tsg_ran/WG4_Radio",
    "SA1": "tsg_sa/WG1_Serv",
    "SA2": "tsg_sa/WG2_Arch",
    "SA3": "tsg_sa/WG3_Security",
    "SA4": "tsg_sa/WG4_CODEC",
    "SA5": "tsg_sa/WG5_TM",
    "SA6": "tsg_sa/WG6_MissionCritical",
    "CT1": "tsg_ct/WG1_mm-cc-sm_ex-CN1",
    "CT3": "tsg_ct/WG3_interworking_ex-CN3",
    "CT4": "tsg_ct/WG4_protocollars_ex-CN4",
    "CT6": "tsg_ct/WG6_Smart_Card_App_ex-T3",
}

# WG별 회의 폴더 prefix 후보들 (FTP 디렉토리에서 회의 폴더를 식별하는 패턴)
# 일부 WG는 여러 명명 규칙을 사용 (예: SA2는 TSGS2_, S2_ 등)
WG_MEETING_PREFIXES = {
    "RAN1": ["TSGR1_", "TSGR1#"],
    "RAN2": ["TSGR2_", "TSGR2#"],
    "RAN3": ["TSGR3_", "TSGR3#"],
    "RAN4": ["TSGR4_", "TSGR4#"],
    "SA1":  ["TSGS1_", "S1_", "S1-"],
    "SA2":  ["TSGS2_", "S2_", "S2-"],
    "SA3":  ["TSGS3_", "S3_", "S3-"],
    "SA4":  ["TSGS4_", "S4_", "S4-"],
    "SA5":  ["TSGS5_", "S5_", "S5-"],
    "SA6":  ["TSGS6_", "S6_", "S6-"],
    "CT1":  ["C1_", "C1-", "TSGC1_"],
    "CT3":  ["C3_", "C3-", "TSGC3_"],
    "CT4":  ["C4_", "C4-", "TSGC4_"],
    "CT6":  ["C6_", "C6-", "TSGC6_"],
}

# WG별 TDoc 리스트 xlsx 파일명 패턴
WG_TDOC_PREFIX = {
    "RAN1": "TDoc_List_Meeting_RAN1#",
    "RAN2": "TDoc_List_Meeting_RAN2#",
    "RAN3": "TDoc_List_Meeting_RAN3#",
    "RAN4": "TDoc_List_Meeting_RAN4#",
    "SA1": "TDoc_List_Meeting_SA1#",
    "SA2": "TDoc_List_Meeting_SA2#",
    "SA3": "TDoc_List_Meeting_SA3#",
    "SA4": "TDoc_List_Meeting_SA4#",
    "SA5": "TDoc_List_Meeting_SA5#",
    "SA6": "TDoc_List_Meeting_SA6#",
    "CT1": "TDoc_List_Meeting_CT1#",
    "CT3": "TDoc_List_Meeting_CT3#",
    "CT4": "TDoc_List_Meeting_CT4#",
    "CT6": "TDoc_List_Meeting_CT6#",
}


def list_meetings_from_ftp(wg):
    """FTP 디렉토리 목록에서 해당 WG의 회의 폴더 목록을 가져온다."""
    ftp_path = WG_FTP_MAP.get(wg)
    if not ftp_path:
        return []
    url = f"https://www.3gpp.org/ftp/{ftp_path}/"
    try:
        r = requests.get(url, timeout=15, verify=False)
        r.raise_for_status()
        # HTML 디렉토리 리스팅에서 폴더명 추출
        # href="TSGR2_133bis/" 또는 href="/ftp/.../TSGR2_133bis/" 패턴
        all_links = re.findall(r'href="([^"]*)"', r.text)
        prefixes = WG_MEETING_PREFIXES.get(wg, [])
        meetings = []
        seen = set()
        for link in all_links:
            name = link.rstrip("/").split("/")[-1]
            if not name or name in seen:
                continue
            # 여러 prefix 후보 중 하나라도 매칭되면 회의 폴더
            for pfx in prefixes:
                if name.upper().startswith(pfx.upper()):
                    meetings.append(name)
                    seen.add(name)
                    break
        # 숫자 기준 최신순 정렬
        def sort_key(m):
            nums = re.findall(r'\d+', m)
            return int(nums[0]) if nums else 0
        meetings.sort(key=sort_key, reverse=True)
        return meetings[:30]  # 최근 30개
    except Exception as e:
        append_log(f"FTP 회의 목록 조회 오류: {e}")
        return []


def fetch_tdoc_list_xlsx(wg, meeting_folder):
    """
    회의 폴더의 Docs/ 안에서 TDoc_List xlsx를 다운받아 파싱.
    반환: (agenda_dict, entries)
      agenda_dict: {"7.1 - AI/ML": [entry1, entry2, ...], ...}
      entries: 전체 entry 리스트
    """
    import urllib.parse

    ftp_path = WG_FTP_MAP.get(wg, "")
    tdoc_prefix = WG_TDOC_PREFIX.get(wg, "TDoc_List_Meeting_")
    
    # 회의 번호 추출: 여러 prefix 후보에서 매칭되는 것 제거
    meeting_num = meeting_folder
    for pfx in WG_MEETING_PREFIXES.get(wg, []):
        if meeting_folder.upper().startswith(pfx.upper()):
            meeting_num = meeting_folder[len(pfx):]
            break

    # SA2의 경우: TSGS2_168_City → 168, TSGS2_168bis_City → 168bis
    # 도시명 등 부가 문자 제거: 숫자(+bis/e 등)만 추출
    match = re.match(r'^(\d+(?:bis|e|b)?)', meeting_num, re.I)
    if match:
        meeting_num = match.group(1)

    docs_url = f"https://www.3gpp.org/ftp/{ftp_path}/{meeting_folder}/Docs/"

    # TDoc 리스트 xlsx 파일명 구성 (# 포함)
    xlsx_filename = f"{tdoc_prefix}{meeting_num}.xlsx"

    # 시도 1: # 인코딩하여 요청 (%23)
    xlsx_url_encoded = f"{docs_url}{urllib.parse.quote(xlsx_filename)}"
    r = None
    try:
        r = requests.get(xlsx_url_encoded, timeout=30, verify=False)
        r.raise_for_status()
    except Exception:
        pass

    # 시도 2: # 그대로 (일부 서버에서 동작)
    if r is None or r.status_code != 200:
        xlsx_url_raw = f"{docs_url}{xlsx_filename}"
        try:
            r = requests.get(xlsx_url_raw, timeout=30, verify=False)
            r.raise_for_status()
        except Exception:
            pass

    # 시도 3: Docs 폴더 HTML을 파싱해서 실제 xlsx 파일명 찾기
    if r is None or r.status_code != 200:
        try:
            dir_resp = requests.get(docs_url, timeout=15, verify=False)
            dir_resp.raise_for_status()
            # TDoc_List로 시작하는 xlsx 파일 찾기
            xlsx_links = re.findall(r'href="([^"]*TDoc_List[^"]*\.xlsx)"', dir_resp.text, re.I)
            if xlsx_links:
                actual_filename = xlsx_links[0].split("/")[-1]
                actual_url = f"{docs_url}{urllib.parse.quote(actual_filename)}"
                r = requests.get(actual_url, timeout=30, verify=False)
                r.raise_for_status()
        except Exception as e:
            append_log(f"TDoc 리스트 다운로드 실패 (모든 시도): {e}")
            return {}, []

    if r is None or r.status_code != 200:
        append_log(f"TDoc 리스트 다운로드 실패: {xlsx_filename}")
        return {}, []

    # Parse xlsx
    wb = load_workbook(io.BytesIO(r.content), read_only=True, data_only=True)
    ws = wb.active

    # 헤더 행 찾기 — 상위 10행까지 스캔
    header_row = None
    col_map = {}
    for row_idx, row in enumerate(ws.iter_rows(min_row=1, max_row=10), start=1):
        for col_idx, cell in enumerate(row):
            val = str(cell.value or "").strip().lower()
            if any(kw in val for kw in ["tdoc", "td#", "td number", "document"]) and "tdoc" not in col_map:
                col_map["tdoc"] = col_idx
            if any(kw in val for kw in ["source", "company", "submitting"]) and "company" not in col_map:
                col_map["company"] = col_idx
            if "agenda" in val and "company" not in val:
                col_map["agenda"] = col_idx
        if "tdoc" in col_map and "agenda" in col_map:
            header_row = row_idx
            break

    # Fallback: 3GPP 표준 레이아웃 (A=TDoc, C=Source/Company, L=Agenda)
    if not header_row:
        header_row = 1
        col_map = {"tdoc": 0, "company": 2, "agenda": 11}

    entries = []
    agenda_dict = {}

    for row in ws.iter_rows(min_row=header_row + 1):
        tdoc_idx = col_map.get("tdoc", 0)
        company_idx = col_map.get("company", 2)
        agenda_idx = col_map.get("agenda", 11)

        if len(row) <= tdoc_idx:
            continue

        tdoc_cell = row[tdoc_idx]
        company_cell = row[company_idx] if len(row) > company_idx else None
        agenda_cell = row[agenda_idx] if len(row) > agenda_idx else None

        tdoc_id = str(tdoc_cell.value or "").strip()
        if not tdoc_id:
            continue

        company = str(company_cell.value or "").strip() if company_cell else ""
        agenda = str(agenda_cell.value or "").strip() if agenda_cell else ""

        # 하이퍼링크에서 다운로드 URL 추출
        if getattr(tdoc_cell, "hyperlink", None) and tdoc_cell.hyperlink.target:
            link = tdoc_cell.hyperlink.target
        else:
            link = f"{docs_url}{tdoc_id}.zip"

        entry = {"doc": tdoc_id, "company": company, "link": link, "agenda": agenda}
        entries.append(entry)

        if agenda:
            agenda_dict.setdefault(agenda, [])
            agenda_dict[agenda].append(entry)

    wb.close()
    return agenda_dict, entries


def clone_paragraph(src, dest):
    np_para = dest.add_paragraph("", style=src.style)
    for r in src.runs:
        nr = np_para.add_run(r.text)
        nr.bold = r.bold
        nr.italic = r.italic
        nr.underline = r.underline
        if hasattr(r.font, "name") and r.font.name:
            nr.font.name = r.font.name
        if hasattr(r.font, "size") and r.font.size:
            nr.font.size = r.font.size
        if hasattr(r.font, "color") and getattr(r.font.color, "rgb", None):
            nr.font.color.rgb = r.font.color.rgb
    return np_para


def repackage_docm_to_docx(path, td):
    ud = os.path.join(td, "docm_unzip")
    os.makedirs(ud, exist_ok=True)
    with zipfile.ZipFile(path, 'r') as zf:
        zf.extractall(ud)
    tf = os.path.join(ud, "[Content_Types].xml")
    t = open(tf, 'r', encoding='utf-8').read()
    t = t.replace(
        'application/vnd.ms-word.document.macroEnabled.main+xml',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml'
    )
    open(tf, 'w', encoding='utf-8').write(t)
    rp = os.path.join(td, "repack.zip")
    with zipfile.ZipFile(rp, 'w', zipfile.ZIP_DEFLATED) as zf:
        for r, _, fs in os.walk(ud):
            for f in fs:
                full = os.path.join(r, f)
                arc = os.path.relpath(full, ud)
                zf.write(full, arc)
    out = os.path.join(td, "repack.docx")
    os.rename(rp, out)
    return out


def _download_doc(entry, td_name, headers):
    try:
        kwargs = {"headers": headers, "timeout": 60, "verify": False}
        r = requests.get(entry["link"], **kwargs)
        r.raise_for_status()
        fp = os.path.join(td_name, f"{entry['doc']}.zip")
        with open(fp, "wb") as f:
            f.write(r.content)
        return entry, fp, None
    except Exception as ex:
        return entry, None, str(ex)


# ==========================================
# 4. Output 1 — extract_all_conclusions (원본 동일)
# ==========================================
def extract_all_conclusions(entries, status_elem, progress_elem, log_func):
    if CLOUD_FUNCTION_URL:
        return _extract_via_cloud(entries, status_elem, progress_elem, log_func)
    return _extract_local(entries, status_elem, progress_elem, log_func)


def _extract_via_cloud(entries, status_elem, progress_elem, log_func):
    """Cloud Function으로 다운로드/파싱 위임, 결과로 원본과 동일한 docx 생성."""
    od = Document()
    od.add_heading("3GPP Conclusions", level=0)
    extracted_list = []
    total = len(entries)
    batch_size = 10
    all_results = []

    for i in range(0, total, batch_size):
        batch = entries[i:i + batch_size]
        status_elem.text(f"☁️ 클라우드 처리 [{min(i+batch_size, total)}/{total}]")
        progress_elem.progress(min(i+batch_size, total) / total)
        try:
            resp = requests.post(CLOUD_FUNCTION_URL, json={"entries": batch}, timeout=300)
            resp.raise_for_status()
            all_results.extend(resp.json().get("results", []))
        except Exception as e:
            log_func(f"Cloud Function 오류, 로컬 전환: {e}")
            return _extract_local(entries, status_elem, progress_elem, log_func)

    for idx, item in enumerate(all_results, 1):
        tbl = od.add_table(rows=4, cols=2, style="Table Grid")
        tbl.cell(0,0).text, tbl.cell(0,1).text = "Document", item.get("doc","")
        tbl.cell(1,0).text, tbl.cell(1,1).text = "Link", item.get("link","")
        tbl.cell(2,0).text, tbl.cell(2,1).text = "Company", item.get("company","")
        tbl.cell(3,0).text, tbl.cell(3,1).text = "Title", item.get("title","")

        content = item.get("content","")
        if content and content not in ("결론 섹션 없음","DOC 파일 없음"):
            for line in content.split("\n"):
                if line.strip():
                    od.add_paragraph(line)
        else:
            od.add_paragraph(content or "결론 섹션 없음")

        extracted_list.append({
            "doc": item.get("doc",""), "company": item.get("company",""),
            "link": item.get("link",""), "title": item.get("title",""),
            "content": content,
            "full_content": content,
        })
        log_func(f"{item.get('doc','')} 추출 완료")
        if idx < len(all_results):
            od.add_page_break()

    st.session_state.extracted_data = extracted_list
    _build_notebooklm_txt(extracted_list)
    bio = io.BytesIO()
    od.save(bio)
    bio.seek(0)
    return bio


def _extract_local(entries, status_elem, progress_elem, log_func):
    """원본 extract_all_conclusions과 동일."""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_func(f"임시 디렉터리 생성: {temp_dir}")
        od = Document()
        od.add_heading("3GPP Conclusions", level=0)

        cps = [
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(conclusions?)\s*$", re.I),
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(summary)\s*$", re.I),
        ]
        eps = [
            re.compile(r"^(?:#\s*)?(?:\d+\.?\s*)?(references?|appendix|acknowledgment)\s*$", re.I),
        ]
        headers = {"User-Agent": "Mozilla/5.0"}
        download_results = []
        extracted_list = []
        total = len(entries)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(_download_doc, e, temp_dir, headers): e for e in entries}
            for i, fut in enumerate(as_completed(futures), start=1):
                e, fp, err = fut.result()
                download_results.append((e, fp, err))
                progress_elem.progress(i / total)
                status_elem.text(f"Downloaded [{i}/{total}]: {e['doc']}")
                log_func(f"[{i}/{total}] Downloaded: {e['doc']}")

        for idx, (e, fp, err) in enumerate(download_results, start=1):
            status_elem.text(f"Extracting [{idx}/{total}]: {e['doc']}")
            doc_text_buffer = []
            full_text_buffer = []

            tbl = od.add_table(rows=4, cols=2, style="Table Grid")
            tbl.cell(0,0).text, tbl.cell(0,1).text = "Document", e["doc"]
            tbl.cell(1,0).text, tbl.cell(1,1).text = "Link", e["link"]
            tbl.cell(2,0).text, tbl.cell(2,1).text = "Company", e["company"]
            tbl.cell(3,0).text = "Title"

            try:
                if err or not fp:
                    raise Exception(err or "Download failed")
                ed = os.path.join(temp_dir, e["doc"])
                os.makedirs(ed, exist_ok=True)
                with zipfile.ZipFile(fp) as zf:
                    zf.extractall(ed)

                src_path = None
                for ext in ("*.docx", "*.docm", "*.doc"):
                    src_path = next(Path(ed).rglob(ext), None)
                    if src_path: break

                if not src_path:
                    od.add_paragraph("DOC 파일을 찾을 수 없습니다.")
                    log_func(f"{e['doc']} 없음")
                    continue

                file_path_str = str(src_path)
                if src_path.suffix.lower() == ".docm":
                    try:
                        file_path_str = repackage_docm_to_docx(file_path_str, temp_dir)
                    except Exception as ex:
                        log_func(f"{e['doc']} docm 변환 오류: {ex}")

                try:
                    sd = Document(file_path_str)
                except Exception as ex:
                    od.add_paragraph(f"문서를 열 수 없습니다 (구형 .doc 파일이거나 손상됨): {ex}")
                    log_func(f"{e['doc']} 문서 파싱 에러: {ex}")
                    continue

                title = ""
                paras = sd.paragraphs
                for p in paras:
                    t = p.text.strip()
                    if t:
                        full_text_buffer.append(t)
                    if not title and t.lower().startswith("title:"):
                        title = t.split(":", 1)[1].strip()
                if not title:
                    title = sd.core_properties.title or ""
                tbl.cell(3, 1).text = title

                start = None
                for pat in cps:
                    for j, p in enumerate(paras):
                        if pat.match(p.text.strip()):
                            start = j; break
                    if start is not None: break

                if start is None:
                    od.add_paragraph("결론 섹션 없음")
                    log_func(f"{e['doc']} 결론없음")
                else:
                    end = len(paras)
                    for ep in eps:
                        for j, p in enumerate(paras[start+1:], start+1):
                            if ep.match(p.text.strip()):
                                end = j; break
                        if end < len(paras): break
                    for j in range(start+1, end):
                        clone_paragraph(paras[j], od)
                        doc_text_buffer.append(paras[j].text)
                    log_func(f"{e['doc']} 추출 완료")

                extracted_list.append({
                    "doc": e["doc"], "company": e["company"], "link": e["link"],
                    "title": title,
                    "content": "\n".join(doc_text_buffer) if doc_text_buffer else "Conclusion 섹션을 찾지 못했습니다.",
                    "full_content": "\n".join(full_text_buffer) if full_text_buffer else "원문 텍스트를 추출하지 못했습니다."
                })
            except Exception as ex:
                od.add_paragraph(f"오류 - {e['doc']}: {ex}")
                log_func(str(ex))

            if idx < len(download_results):
                od.add_page_break()

        st.session_state.extracted_data = extracted_list
        _build_notebooklm_txt(extracted_list)
        bio = io.BytesIO()
        od.save(bio)
        bio.seek(0)
    return bio


def _build_notebooklm_txt(extracted_list):
    txt = ["=== 3GPP Contributions Conclusions ==="]
    for item in extracted_list:
        txt.append(f"\n\n--- Document: {item['doc']} ---")
        txt.append(f"Company: {item['company']}")
        txt.append(f"Title: {item['title']}")
        txt.append("Content:")
        txt.append(item['content'])
    st.session_state.notebooklm_txt = "\n".join(txt)


# ==========================================
# 5. Output 2 — TF-IDF parse_and_summarize (원본 동일)
# ==========================================
class TFIDFEmbedder:
    def __init__(self, max_features=3000, ngram_range=(1, 2)):
        self.v = TfidfVectorizer(
            max_features=max_features, ngram_range=ngram_range,
            lowercase=True, stop_words="english", strip_accents="unicode",
            token_pattern=r"\b[a-zA-Z]{2,}\b",
        )
        self.fitted = False

    def encode(self, texts):
        if isinstance(texts, str): texts = [texts]
        proc = [re.sub(r"\s+", " ", re.sub(r"[^\w\s\-]", " ", t.lower())).strip() for t in texts]
        if not self.fitted:
            self.v.fit(proc)
            self.fitted = True
        return self.v.transform(proc).toarray()


def parse_and_summarize(in_bio, status_elem, log_func):
    """원본 parse_and_summarize와 동일."""
    d = Document(in_bio)
    props, pcs, cur = [], {}, None

    for el in d.element.body:
        if el.tag.endswith("tbl"):
            tbl = Table(el, d)
            for r in tbl.rows:
                if r.cells[0].text.strip() == "Company":
                    cur = r.cells[1].text.strip()
        elif el.tag.endswith("p"):
            p = Paragraph(el, d)
            txt = p.text.strip()
            if txt.lower().startswith("proposal"):
                buf, cm = [txt], {cur} if cur else set()
                idx2 = d.element.body.index(el) + 1
                while idx2 < len(d.element.body):
                    sib = d.element.body[idx2]
                    if not sib.tag.endswith("p"): break
                    sp = Paragraph(sib, d)
                    st_text = sp.text.rstrip()
                    if not st_text.strip() or st_text.lower().startswith("proposal"): break
                    buf.append(st_text)
                    if cur: cm.add(cur)
                    idx2 += 1
                bl = "\n".join(buf)
                props.append(bl)
                pcs[bl] = cm.copy()

    r = Document()
    r.add_heading("Proposal Summary", 0)

    if not props:
        r.add_paragraph("No proposals found.")
        bio = io.BytesIO()
        r.save(bio)
        bio.seek(0)
        return bio

    status_elem.text("Generating embeddings & Clustering...")
    em = TFIDFEmbedder()
    emb = em.encode(props)

    N = len(props)
    mn, mx = max(2, N // 5), max(3, N // 2)
    best_diff = float("inf")
    best_lbl = None
    for thr in np.linspace(0.2, 0.8, 13):
        try:
            hac = AgglomerativeClustering(
                n_clusters=None, metric="cosine", linkage="average",
                distance_threshold=thr, compute_full_tree=True,
            )
            lbls = hac.fit_predict(emb)
            cnt = len(set(lbls))
            diff = abs(cnt - (mn + mx) / 2)
            if diff < best_diff:
                best_diff = diff
                best_lbl = lbls
        except: pass
    lbls = best_lbl if best_lbl is not None else np.zeros(N, dtype=int)

    clusters = {}
    for i, l in enumerate(lbls):
        clusters.setdefault(l, {"idxs": [], "cm": set()})
        clusters[l]["idxs"].append(i)
        clusters[l]["cm"].update(pcs[props[i]])

    items = []
    for info in clusters.values():
        idxs = info["idxs"]
        subset = emb[idxs]
        cent = np.mean(subset, axis=0, keepdims=True)
        sims = cosine_similarity(cent, subset)[0]
        rep = props[idxs[int(np.argmax(sims))]]
        cm = sorted(info["cm"])
        items.append({"proposal": rep, "companies": cm, "count": len(cm)})

    items.sort(key=lambda x: x["count"], reverse=True)

    status_elem.text("Creating summary...")
    for it in items:
        r.add_paragraph(it["proposal"])
        r.add_paragraph(f"Supporting companies ({it['count']}): " + (", ".join(it["companies"]) if it["companies"] else "(none)"))
        r.add_paragraph("")

    bio = io.BytesIO()
    r.save(bio)
    bio.seek(0)
    log_func("Summary 생성 완료")
    return bio


# ==========================================
# 6. Output 3 — Gemini AI 분석 (선택, 서버 키)
#    ★ 할루시네이션 제로 + 정밀 그룹핑 프롬프트 ★
# ==========================================
def _build_doc_inventory(extracted_data):
    """다운로드된 문서 목록을 명시적으로 나열. Gemini가 이 목록 밖의 문서를 인용하면 할루시네이션."""
    lines = []
    for item in extracted_data:
        lines.append(f"  - {item['doc']} (회사: {item['company']})")
    return "\n".join(lines)


def run_gemini_analysis(extracted_data, status_elem, api_key):
    genai.configure(api_key=api_key)

    # 문서 인벤토리 (허용된 문서 번호 목록)
    doc_inventory = _build_doc_inventory(extracted_data)
    valid_doc_ids = {item['doc'] for item in extracted_data}
    valid_companies = {item['company'] for item in extracted_data if item['company']}

    # 기고문 본문 구성
    text_buffer = []
    for item in extracted_data:
        text_buffer.append(
            f"========== 문서 시작: {item['doc']} ==========\n"
            f"회사: {item['company']}\n"
            f"제목: {item.get('title', '')}\n"
            f"내용:\n{item['content']}\n"
            f"========== 문서 끝: {item['doc']} =========="
        )
    full_text = "\n\n".join(text_buffer)

    # ★ 핵심 프롬프트 — 할루시네이션 방지 + 정밀 그룹핑 ★
    MAIN_PROMPT = f"""당신은 3GPP 표준화 회의 기고문을 분석하는 전문가입니다.

아래에 다운로드된 기고문 원문이 제공됩니다. 이 원문만을 근거로 분석하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[절대 규칙 — 할루시네이션 금지]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 아래 [허용된 문서 목록]에 있는 문서 번호만 인용할 수 있습니다.
   이 목록에 없는 문서 번호를 절대 만들어내거나 인용하지 마세요.
2. 아래 [허용된 회사 목록]에 있는 회사명만 사용할 수 있습니다.
   이 목록에 없는 회사를 절대 만들어내지 마세요.
3. 원문에 명시적으로 적혀 있는 내용만 분석하세요.
   원문에 없는 내용을 추론하거나 지어내지 마세요.
4. 어떤 회사가 어떤 제안을 지지하는지는, 해당 회사의 기고문에
   해당 제안이 실제로 기술되어 있을 때만 인정됩니다.
5. 확실하지 않으면 포함하지 마세요. 누락이 환각보다 낫습니다.

[허용된 문서 목록]
{doc_inventory}

[허용된 회사 목록]
{', '.join(sorted(valid_companies))}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[그룹핑 규칙 — 정밀 그룹핑]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. 두 제안을 같은 그룹으로 묶으려면, 다음 조건을 모두 만족해야 합니다:
   a) 같은 기술적 메커니즘을 다루고 있어야 합니다.
      (예: 둘 다 "C-DRX timer 제어"에 대해 이야기하는 경우)
   b) 제안하는 구체적 동작/방향이 동일하거나 매우 유사해야 합니다.
      (예: 둘 다 "network이 DRX timer를 제어해야 한다"고 제안하는 경우)
   c) 같은 주제를 다루더라도 제안 방향이 다르면 별도 그룹입니다.
      (예: "WUS 필요하다" vs "WUS 불필요하다"는 별도 그룹)

2. 뭉뚱그리지 마세요:
   - "에너지 효율 관련 제안"처럼 광범위한 그룹은 금지합니다.
   - "Cell DTX/DRX 패턴을 UE에 알려야 한다"처럼 구체적 동작 수준으로 묶으세요.
   - 하나의 기고문에 여러 개의 서로 다른 제안이 있으면,
     각 제안을 별도로 분류하세요 (같은 문서가 여러 그룹에 속할 수 있음).

3. 1개 회사만 단독 주장한 제안은 결과에서 제외하세요.
   반드시 2개 이상의 서로 다른 회사가 동일/유사 제안을 한 경우만 포함하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[출력 양식] — 반드시 아래 형식을 정확히 따르세요
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

### [순위]. [제안의 구체적 동작을 요약한 제목]
* **지지 회사 (총 N개사):** 회사명1, 회사명2, ... (쉼표 구분, 중복 제거)
* **상세 내용:** 이 제안이 구체적으로 무엇을 요구하는지 2-3문장으로 기술. 원문의 표현을 최대한 유지.
* **근거 문서:**
  - [문서번호] (회사명): 해당 문서에서 이 제안이 나오는 부분의 핵심 문구 인용
  - [문서번호] (회사명): 해당 문서에서 이 제안이 나오는 부분의 핵심 문구 인용
  (각 지지 회사의 근거 문서를 반드시 하나 이상 나열하세요)

순위는 지지 회사 수가 많은 순서대로 내림차순으로 부여하세요.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[기고문 원문 데이터]
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{full_text}
"""

    MAP_PROMPT_TEMPLATE = """당신은 3GPP 기고문 분석 전문가입니다.

[절대 규칙]
1. 아래 제공된 문서에 있는 내용만 추출하세요. 없는 내용을 지어내지 마세요.
2. 허용된 문서 번호: {doc_list}
3. 각 제안을 추출할 때, 반드시 해당 제안이 적힌 문서 번호와 회사명을 함께 기록하세요.
4. 하나의 기고문에 여러 제안이 있으면 각각 별도로 추출하세요.
5. 구체적 동작 수준으로 추출하세요 (예: "Proposal 3: C-DRX with DL WUS" → 그대로 기록).

[출력 양식]
- 제안: [원문에 가깝게 제안 내용 기술]
- 문서: [문서번호]
- 회사: [회사명]
- 원문 근거: [해당 제안이 나오는 원문 문구를 최대한 그대로 인용]

빠지는 제안이 없도록 모든 Proposal, Observation, Recommendation을 추출하세요.

[기고문 원문]
{batch_text}"""

    REDUCE_PROMPT_TEMPLATE = """당신은 3GPP 기고문 분석 전문가입니다.

아래에 1차 추출된 제안 목록이 있습니다. 이를 최종 보고서로 병합하세요.

[절대 규칙 — 할루시네이션 금지]
1. 허용된 문서 번호: {doc_list}. 이 목록 밖의 문서를 인용하면 안 됩니다.
2. 허용된 회사: {company_list}. 이 목록 밖의 회사를 인용하면 안 됩니다.
3. 1차 추출 결과에 실제로 있는 내용만 사용하세요.

[그룹핑 규칙 — 정밀 그룹핑]
1. 구체적 동작이 동일한 제안만 같은 그룹으로 묶으세요.
2. 같은 주제라도 제안 방향이 다르면 별도 그룹입니다.
3. "에너지 효율" 같은 광범위한 그룹은 금지. "Cell DTX/DRX 패턴 정보를 UE에 전달" 수준으로 구체화.
4. 2개 이상의 회사가 지지하는 제안만 포함하세요.

[출력 양식]
### [순위]. [제안의 구체적 동작 요약 제목]
* **지지 회사 (총 N개사):** 회사1, 회사2, ...
* **상세 내용:** 구체적 제안 내용 2-3문장
* **근거 문서:**
  - [문서번호] (회사명): 원문 핵심 문구 인용
  - [문서번호] (회사명): 원문 핵심 문구 인용

지지 회사 수 내림차순으로 정렬.

[1차 추출 결과]
{intermediate_text}"""

    status_elem.text("🧠 Gemini AI 분석 중...")
    try:
        valid_models = [m.name for m in genai.list_models() if 'generateContent' in m.supported_generation_methods]
        target = next((m for m in valid_models if 'flash' in m.lower() and 'vision' not in m.lower()),
                       next((m for m in valid_models if 'pro' in m.lower() and 'vision' not in m.lower()), valid_models[-1]))
        model_display = target.split('/')[-1]
        model = genai.GenerativeModel(target)
        strict_config = {"temperature": 0.0}  # 0.0으로 더 엄격하게

        total_docs = len(extracted_data)
        response = None
        doc_list_str = ", ".join(sorted(valid_doc_ids))
        company_list_str = ", ".join(sorted(valid_companies))

        if total_docs > 20:
            # Map-Reduce
            batch_size = 20
            total_batches = (total_docs + batch_size - 1) // batch_size
            intermediate = []
            for i in range(total_batches):
                status_elem.text(f"🚀 1차 추출 [{i+1}/{total_batches}]")
                batch = extracted_data[i*batch_size:(i+1)*batch_size]
                bt = "\n\n".join([
                    f"========== {it['doc']} ({it['company']}) ==========\n{it['content']}"
                    for it in batch
                ])
                batch_docs = ", ".join([it['doc'] for it in batch])
                mp = MAP_PROMPT_TEMPLATE.format(doc_list=batch_docs, batch_text=bt)

                for attempt in range(3):
                    try:
                        res = model.generate_content(mp, generation_config=strict_config)
                        if res and res.text: intermediate.append(res.text)
                        break
                    except Exception as e:
                        if "429" in str(e) or "503" in str(e):
                            for cd in range(60,0,-1):
                                status_elem.text(f"⚠️ API 대기 {cd}초 ({attempt+1}/3)")
                                time.sleep(1)
                        else: raise
                if i < total_batches-1:
                    for cd in range(5,0,-1):
                        status_elem.text(f"⏳ 대기 {cd}초 ({i+1}/{total_batches} 완료)")
                        time.sleep(1)

            status_elem.text("🧠 최종 병합 분석 중...")
            fi = "\n\n=== 배치 구분 ===\n\n".join(intermediate)
            rp = REDUCE_PROMPT_TEMPLATE.format(
                doc_list=doc_list_str,
                company_list=company_list_str,
                intermediate_text=fi,
            )
            for attempt in range(3):
                try:
                    response = model.generate_content(rp, generation_config=strict_config); break
                except Exception as e:
                    if "429" in str(e) or "503" in str(e):
                        for cd in range(60,0,-1):
                            status_elem.text(f"⚠️ 대기 {cd}초 ({attempt+1}/3)"); time.sleep(1)
                    else: raise
        else:
            # Direct analysis
            for attempt in range(3):
                try:
                    response = model.generate_content(MAIN_PROMPT, generation_config=strict_config); break
                except Exception as e:
                    if "429" in str(e) or "503" in str(e):
                        for cd in range(30,0,-1):
                            status_elem.text(f"⚠️ 대기 {cd}초 ({attempt+1}/3)"); time.sleep(1)
                    else: raise

        status_elem.text("✅ AI 분석 완료!")
        if response and response.text:
            result_text = response.text

            # ★ 할루시네이션 후처리 검증 ★
            # 결과에서 인용된 문서 번호가 실제 다운로드 목록에 있는지 체크
            cited_docs = set(re.findall(r'[A-Z]\d?-\d{7}', result_text))
            hallucinated = cited_docs - valid_doc_ids
            if hallucinated:
                result_text += f"\n\n---\n⚠️ **검증 경고:** 다음 문서 번호는 다운로드된 파일 목록에 없습니다 (할루시네이션 가능성): {', '.join(sorted(hallucinated))}"

            # Output 3 docx 생성
            doc = Document()
            doc.add_heading(f"AI 정밀 분석 요약 ({model_display})", 0)
            doc.add_paragraph(f"분석 대상: {total_docs}개 문서")
            doc.add_paragraph(f"분석 모델: {model_display} (temperature=0.0)")
            doc.add_paragraph("")
            for line in result_text.split('\n'):
                if re.match(r'^(#+)?\s*\d+\.|^###', line.strip()):
                    p = doc.add_paragraph()
                    p.add_run(line.replace('#','').strip()).bold = True
                elif line.strip().startswith('* **'):
                    doc.add_paragraph(line.strip())
                elif line.strip().startswith('- [') or line.strip().startswith('- '):
                    doc.add_paragraph(line.strip())
                else:
                    doc.add_paragraph(line)
            bio = io.BytesIO()
            doc.save(bio)
            st.session_state.ai_summary_bytes = bio.getvalue()
            st.session_state.ai_summary_text = result_text
            st.session_state.ai_model_name = model_display
            st.session_state.ai_summary_generated = True
            return True
    except Exception as e:
        err = str(e)
        if "429" in err or "Quota" in err:
            st.error("❌ API 용량 초과. 잠시 후 다시 시도하세요.")
        else:
            st.error(f"❌ API 오류: {e}")
    return False


# ==========================================
# 7. Streamlit UI
# ==========================================
st.sidebar.title("📡 3GPP Analyzer v2")
st.sidebar.caption("기본 분석 + Gemini AI 강화")
st.sidebar.markdown("---")
page = st.sidebar.radio("메뉴", ["🚀 통합 분석기", "⚙️ 설정", "ℹ️ 가이드"])

# ─── Settings ───
if page == "⚙️ 설정":
    st.title("⚙️ 서버 설정")
    st.subheader("Gemini API Key")
    st.info(f"상태: {'✅ 설정됨' if GEMINI_API_KEY else '❌ 미설정'}")
    if not GEMINI_API_KEY:
        st.code('# .streamlit/secrets.toml\nGEMINI_API_KEY = "AIzaSy..."', language="toml")
    st.subheader("Cloud Function URL")
    st.info(f"상태: {'✅ 설정됨' if CLOUD_FUNCTION_URL else '⚠️ 미설정 (서버에서 직접 다운로드)'}")

elif page == "ℹ️ 가이드":
    st.title("ℹ️ 가이드")
    st.write("**기본 분석 (Output 1·2):** 항상 생성. 원본과 동일한 결과물.")
    st.write("**Gemini 분석 (Output 3):** 선택. 서버에 고정된 키 사용, 사용자 키 입력 불필요.")
    st.write("**Cloud Function:** 설정하면 문서를 클라우드에서 처리 (내 PC 다운로드 없음).")

# ─── Main ───
elif page == "🚀 통합 분석기":
    st.title("🚀 3GPP 기고문 통합 분석기")
    st.caption("Output 1·2는 기본 | Output 3 Gemini는 선택")

    # Step 1: Input
    st.header("1️⃣ 데이터 입력")
    if CLOUD_FUNCTION_URL:
        st.success("☁️ Cloud Function 연결됨 — 클라우드에서 처리합니다.")

    input_method = st.radio(
        "입력 방식:",
        ("🔍 회의 번호로 자동 조회", "Excel 파일 업로드", "링크 직접 입력"),
        horizontal=True,
    )
    entries = []

    if input_method == "🔍 회의 번호로 자동 조회":
        col_wg, col_meeting = st.columns([1, 2])

        with col_wg:
            wg = st.selectbox("Working Group:", list(WG_FTP_MAP.keys()))

        with col_meeting:
            # 회의 목록 가져오기
            if st.button("📡 회의 목록 불러오기"):
                with st.spinner("3GPP FTP에서 회의 목록 조회 중..."):
                    st.session_state.meeting_list = list_meetings_from_ftp(wg)
                    st.session_state.agenda_dict = {}
                    st.session_state.all_entries = []

            if st.session_state.meeting_list:
                selected_meeting = st.selectbox(
                    "회의 선택:",
                    st.session_state.meeting_list,
                    format_func=lambda x: x.replace("_", " "),
                )
            else:
                selected_meeting = None
                st.caption("위 버튼을 눌러 회의 목록을 불러오세요.")

        # Agenda 조회
        if selected_meeting:
            if st.button("📋 Agenda 목록 불러오기"):
                with st.spinner(f"{selected_meeting}의 TDoc 리스트 다운로드 중..."):
                    agenda_dict, all_entries = fetch_tdoc_list_xlsx(wg, selected_meeting)
                    st.session_state.agenda_dict = agenda_dict
                    st.session_state.all_entries = all_entries

        if st.session_state.agenda_dict:
            agenda_items = sorted(st.session_state.agenda_dict.keys())
            st.success(f"✅ {len(agenda_items)}개 agenda, 총 {len(st.session_state.all_entries)}개 문서 발견")

            selected_agenda = st.selectbox(
                "Agenda 선택:",
                agenda_items,
                format_func=lambda x: f"{x} ({len(st.session_state.agenda_dict[x])}개 문서)",
            )

            if selected_agenda:
                entries = st.session_state.agenda_dict[selected_agenda]
                st.info(f"📄 **{selected_agenda}** — {len(entries)}개 문서가 분석 대상입니다.")

                # 문서 목록 미리보기
                with st.expander(f"문서 목록 미리보기 ({len(entries)}개)", expanded=False):
                    for e in entries[:30]:
                        st.text(f"  {e['doc']}  |  {e['company']}")
                    if len(entries) > 30:
                        st.caption(f"  ... 외 {len(entries)-30}개")

    elif input_method == "Excel 파일 업로드":
        uploaded = st.file_uploader("엑셀(.xlsx) — 1열: 문서번호(하이퍼링크), 3열: 회사명", type=["xlsx","xls"])
        if uploaded:
            entries = read_excel_from_bytes(uploaded)
            st.info(f"총 {len(entries)}개 문서 인식")
    else:
        raw = st.text_area("3GPP .zip 링크를 한 줄에 하나씩:", height=120)
        if raw:
            for line in raw.strip().split("\n"):
                url = line.strip()
                if url:
                    docid = url.split("/")[-1].replace(".zip","")
                    entries.append({"doc": docid, "company": "Unknown", "link": url})
            st.info(f"총 {len(entries)}개 문서 인식")

    # Step 2: 기본 분석
    st.markdown("---")
    st.header("2️⃣ 기본 분석 (Output 1 + 2)")

    if st.button("🚀 기본 분석 실행 (Run)", type="primary"):
        if not entries:
            st.warning("먼저 데이터를 입력해주세요.")
        else:
            st.session_state.log_text = ""
            st.session_state.process_done = False
            st.session_state.ai_summary_generated = False
            st.session_state.ai_summary_bytes = None

            status = st.empty()
            progress = st.progress(0)

            status.text("기고문 다운로드 및 결론 추출 시작...")
            out1_bio = extract_all_conclusions(entries, status, progress, append_log)

            status.text("TF-IDF 기반 요약 분석 시작...")
            out2_bio = parse_and_summarize(out1_bio, status, append_log)

            status.text("✅ 기본 분석 완료!")
            progress.progress(1.0)

            st.session_state.out1_bytes = out1_bio.getvalue()
            st.session_state.out2_bytes = out2_bio.getvalue()
            st.session_state.process_done = True

    if st.session_state.process_done:
        st.success("🎉 기본 분석 완료!")
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.out1_bytes:
                st.download_button("📥 Output 1 (Conclusions 취합.docx)",
                    data=st.session_state.out1_bytes,
                    file_name="output1_conclusions.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True)
        with col2:
            if st.session_state.out2_bytes:
                st.download_button("📥 Output 2 (TF-IDF 요약.docx)",
                    data=st.session_state.out2_bytes,
                    file_name="output2_summary_tfidf.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True)

        if st.session_state.notebooklm_txt:
            st.download_button("📝 NotebookLM용 텍스트 (.txt)",
                data=st.session_state.notebooklm_txt.encode('utf-8'),
                file_name="NotebookLM_Input_Conclusions.txt", mime="text/plain")

        # Step 3: Gemini
        st.markdown("---")
        st.header("3️⃣ Gemini AI 정밀 분석 (선택)")

        if not GEMINI_API_KEY:
            st.warning("⚙️ 설정에서 Gemini API Key를 먼저 설정하세요.")
        else:
            if st.button("✨ Gemini AI 분석 시작"):
                status = st.empty()
                run_gemini_analysis(st.session_state.extracted_data, status, GEMINI_API_KEY)

        if st.session_state.ai_summary_generated:
            st.success("✅ AI 정밀 요약 완료!")
            st.download_button(
                f"📥 Output 3 (AI 요약 - {st.session_state.ai_model_name}.docx)",
                data=st.session_state.ai_summary_bytes,
                file_name="Output3_AI_Summary.docx",
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                type="primary", use_container_width=True)
            with st.expander("👀 AI 분석 결과 미리보기", expanded=True):
                st.markdown(st.session_state.ai_summary_text)

    with st.expander("📝 처리 로그", expanded=False):
        st.text(st.session_state.log_text)
